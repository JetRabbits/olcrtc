// Package server implements the olcrtc tunnel server logic.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/framing"
	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/names"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/xtaci/smux"
)

const (
	connectCommand   = "connect"
	udpDialCommand   = "udp-dial"
	maxUDPPacketSize = 65535
)

var (
	// ErrKeyRequired re-exports runtime.ErrKeyRequired for compatibility with
	// pre-runtime callers that errors.Is-checked it.
	ErrKeyRequired = runtime.ErrKeyRequired
	// ErrKeySize re-exports runtime.ErrKeySize for the same reason.
	ErrKeySize = runtime.ErrKeySize
	// ErrSocks5AuthFailed is returned when SOCKS5 authentication fails.
	ErrSocks5AuthFailed = errors.New("SOCKS5 auth failed")
	// ErrSocks5ConnectFailed is returned when SOCKS5 connection fails.
	ErrSocks5ConnectFailed = errors.New("SOCKS5 connect failed")
)

// SessionOpenFunc is called after a successful handshake, before the server
// accepts tunnel streams on that session.
type SessionOpenFunc func(sessionID, deviceID string, claims map[string]any)

// SessionCloseFunc is called when a session is torn down. Possible reasons:
// "reconnect" (carrier dropped and was reestablished), "closed" (graceful
// shutdown or ctx cancel).
type SessionCloseFunc func(sessionID, reason string)

// TrafficFunc is called once per tunnel stream, after the copy loops finish.
// bytesIn counts client→target bytes; bytesOut counts target→client bytes.
type TrafficFunc func(sessionID, addr string, bytesIn, bytesOut uint64)

// HealthFunc is called when the server control health snapshot changes.
type HealthFunc func(control.Status)

// Server handles incoming tunnel connections and proxies their traffic.
type Server struct {
	ln                      transport.Transport
	peerLn                  transport.PeerTransport
	cipher                  *crypto.Cipher
	plaintext               bool
	conn                    *muxconn.Conn
	session                 *smux.Session
	controlStrm             *smux.Stream
	controlStop             context.CancelFunc
	sessMu                  sync.RWMutex
	peerSessions            map[string]*peerSession
	peersMu                 sync.Mutex
	peerStats               map[string]peerStat
	reinstallMu             sync.Mutex
	wg                      sync.WaitGroup
	authHook                handshake.AuthFunc
	onOpen                  SessionOpenFunc
	onClose                 SessionCloseFunc
	onTraffic               TrafficFunc
	deviceID                string
	sessionID               string
	dnsServer               string
	resolver                *net.Resolver
	socksProxyAddr          string
	socksProxyPort          int
	socksProxyUser          string
	socksProxyPass          string
	liveness                control.Config
	health                  *runtime.HealthTracker
	done                    chan struct{}
	doneOnce                sync.Once
	signalHandshakeComplete func()
	sessionOpenedAt         atomic.Pointer[time.Time]
}

// peerStat holds the per-session info needed to report the live peer count
// and a disconnect summary.
type peerStat struct {
	deviceID string
	openedAt time.Time
}

type peerSession struct {
	peerID      string
	conn        *muxconn.Conn
	session     *smux.Session
	controlStrm *smux.Stream
	controlStop context.CancelFunc
	sessionID   string
	deviceID    string
}

// ConnectRequest is a message from the client to establish a new connection.
type ConnectRequest struct {
	Cmd  string `json:"cmd"`
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

// Config holds runtime configuration for [Run].
type Config struct {
	Transport        string
	Carrier          string
	RoomURL          string
	ChannelID        string
	KeyHex           string
	Plaintext        bool // skip AEAD encryption (e.g. when WireGuard already encrypts)
	DNSServer        string
	SOCKSProxyAddr   string
	SOCKSProxyPort   int
	SOCKSProxyUser   string
	SOCKSProxyPass   string
	TransportOptions transport.Options
	Engine           string
	URL              string
	Token            string
	Liveness         control.Config
	Traffic          transport.TrafficConfig

	// AuthHook is invoked after CLIENT_HELLO to authorize the client and
	// return a session ID. If nil, every client is admitted with a random UUID.
	AuthHook handshake.AuthFunc

	// OnSessionOpen fires after a successful handshake. Nil means no-op.
	OnSessionOpen SessionOpenFunc
	// OnSessionClose fires when the session is torn down (reconnect, closed). Nil means no-op.
	OnSessionClose SessionCloseFunc
	// OnTraffic fires once per tunnel stream after both copy loops finish. Nil means no-op.
	OnTraffic TrafficFunc
	// OnHealth fires when liveness/reconnect status changes. Nil means no-op.
	OnHealth HealthFunc
}

// Run starts the server with the given configuration.
func Run(ctx context.Context, cfg Config) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var cipher *crypto.Cipher
	if !cfg.Plaintext {
		var err error
		cipher, err = setupCipher(cfg.KeyHex)
		if err != nil {
			return fmt.Errorf("setupCipher failed: %w", err)
		}
	}

	hook := cfg.AuthHook
	if hook == nil {
		hook = defaultAuthHook
	}
	onOpen := cfg.OnSessionOpen
	if onOpen == nil {
		onOpen = func(string, string, map[string]any) {}
	}
	onClose := cfg.OnSessionClose
	if onClose == nil {
		onClose = func(string, string) {}
	}
	onTraffic := cfg.OnTraffic
	if onTraffic == nil {
		onTraffic = func(string, string, uint64, uint64) {}
	}
	s := &Server{
		cipher:         cipher,
		plaintext:      cfg.Plaintext,
		authHook:       hook,
		onOpen:         onOpen,
		onClose:        onClose,
		onTraffic:      onTraffic,
		dnsServer:      cfg.DNSServer,
		socksProxyAddr: cfg.SOCKSProxyAddr,
		socksProxyPort: cfg.SOCKSProxyPort,
		socksProxyUser: cfg.SOCKSProxyUser,
		socksProxyPass: cfg.SOCKSProxyPass,
		liveness:       cfg.Liveness,
		health:         runtime.NewHealthTracker(cfg.OnHealth),
		peerSessions:   make(map[string]*peerSession),
		peerStats:      make(map[string]peerStat),
		done:           make(chan struct{}),
	}
	s.setupResolver()
	logger.Infof("server crypto: plaintext=%v", cfg.Plaintext)

	// Register shutdown BEFORE bringUpLink so a partial setup (e.g.
	// link.New succeeded but ln.Connect timed out) still tears the
	// link down and sends MUC presence-unavailable. Without this, an
	// early bringUpLink error returns straight to the caller and the
	// already-joined MUC presence stays behind as a ghost participant
	// for subsequent tests against the same room. shutdown is
	// idempotent and safe to call before s.serve runs.
	defer func() {
		s.shutdown()
		s.wg.Wait()
	}()

	if err := s.bringUpLink(runCtx, cfg, cancel); err != nil {
		return err
	}

	go func() {
		<-runCtx.Done()
		s.closeSession()
	}()

	s.serve(runCtx)

	return nil
}

func setupCipher(keyHex string) (*crypto.Cipher, error) {
	cipher, err := runtime.SetupCipher(keyHex)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	return cipher, nil
}

func (s *Server) setupResolver() {
	s.resolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			d := net.Dialer{Timeout: 3 * time.Second}
			return d.DialContext(ctx, network, s.dnsServer)
		},
	}
}

func (s *Server) smuxConfig(maxWirePayload int) *smux.Config {
	return runtime.SmuxConfigEx(maxWirePayload, s.plaintext)
}

func linkMaxPayload(tr transport.Transport) int {
	return runtime.MaxPayload(tr)
}

func (s *Server) bringUpLink(
	ctx context.Context,
	cfg Config,
	cancel context.CancelFunc,
) error {
	onPeerData := s.onPeerData
	// The vp8channel peer-routing path uses per-peer KCP sessions over one
	// published VP8 track. Telemost currently delivers client→server reliably,
	// but server→client packets written through that peer path never reach the
	// Android client before the control handshake times out. For the mobile proxy
	// deployment we use one client per room, so prefer the proven single-peer path
	// where both directions are paced by the transport's main writerLoop.
	if cfg.Transport == "vp8channel" {
		onPeerData = nil
	}

	ln, err := transport.New(ctx, cfg.Transport, transport.Config{
		Carrier:    cfg.Carrier,
		RoomURL:    cfg.RoomURL,
		Engine:     cfg.Engine,
		URL:        cfg.URL,
		Token:      cfg.Token,
		ChannelID:  cfg.ChannelID,
		DeviceID:   "",
		Name:       names.Generate(),
		OnData:     s.onData,
		OnPeerData: onPeerData,
		DNSServer:  s.dnsServer,
		ProxyAddr:  s.socksProxyAddr,
		ProxyPort:  s.socksProxyPort,
		Options:    cfg.TransportOptions,
		Traffic:    cfg.Traffic,
	})
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	s.ln = ln
	if peerLn, ok := ln.(transport.PeerTransport); ok && peerLn.SupportsPeerRouting() {
		s.peerLn = peerLn
	}

	ln.SetEndedCallback(func(reason string) {
		logger.Infof("Server link reported conference end: %s", reason)
		cancel()
	})
	ln.SetShouldReconnect(func() bool { return ctx.Err() == nil })
	ln.SetReconnectCallback(func() {
		if ctx.Err() != nil {
			return
		}
		s.handleReconnect()
	})

	// Wire up engine-level reconnect callbacks for goolom. Do not arm
	// reconnect-on-new-participant until after the initial carrier Connect()
	// succeeds: Telemost can replay existing/self descriptions during startup,
	// and treating those as a new Android join causes an immediate repair
	// reconnect loop before any client control path can be proven.
	s.setupEngineCallbacks(ctx, cfg, cancel)

	logger.Infof("Connecting transport=%s carrier=%s ...", cfg.Transport, cfg.Carrier)
	if s.peerLn == nil {
		s.installSession()
	}

	if err := ln.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect link: %w", err)
	}
	logger.Infof("Link connected")
	s.armReconnectOnNewParticipant()
	s.logPeersLine()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ln.WatchConnection(ctx)
	}()
	return nil
}

// setupEngineCallbacks configures engine-level hooks for goolom reconnect
// coordination. The goolom session's reconnectOnNewParticipant defers a
// repair reconnect until the client proves the reverse path is alive
// (SignalHandshakeComplete is called from the first CONTROL_PONG). The
// OnReconnecting callback closes the smux session proactively before goolom
// tears down its WebRTC peers.
func (s *Server) setupEngineCallbacks(ctx context.Context, cfg Config, cancel context.CancelFunc) {
	if s.ln == nil {
		return
	}
	// The engine must be accessible through the transport. Check if the
	// transport exposes the engine session for callback configuration.
	if engineCfg, ok := s.ln.(interface{ SetOnReconnecting(func()) }); ok {
		engineCfg.SetOnReconnecting(func() {
			logger.Infof("server: engine reconnecting - closing smux session proactively")
			s.sessMu.RLock()
			current := s.session
			s.sessMu.RUnlock()
			s.reinstallSession(current)
		})
	}
	if engineCfg, ok := s.ln.(interface{ SignalHandshakeComplete() }); ok {
		s.signalHandshakeComplete = func() {
			engineCfg.SignalHandshakeComplete()
		}
	}
}

func (s *Server) armReconnectOnNewParticipant() {
	if engineCfg, ok := s.ln.(interface{ SetReconnectOnNewParticipant(bool) }); ok {
		logger.Infof("server: arming reconnect-on-new-participant after initial link connect")
		engineCfg.SetReconnectOnNewParticipant(true)
	}
}

func (s *Server) installSession() {
	conn := muxconn.New(s.ln, s.cipher)
	sess, err := smux.Server(conn, s.smuxConfig(linkMaxPayload(s.ln)))
	if err != nil {
		logger.Warnf("smux server init failed: %v", err)
		return
	}
	s.sessMu.Lock()
	s.conn = conn
	s.session = sess
	s.sessMu.Unlock()
}

func (s *Server) handleReconnect() {
	const carrierReconnectSessionGrace = 30 * time.Second
	if openedAt := s.sessionOpenedAt.Load(); openedAt != nil {
		age := time.Since(*openedAt)
		if age < carrierReconnectSessionGrace {
			logger.Infof("server reconnect reason=carrier - skipping smux reinstall (session age = %s < %s)", age.Round(time.Second), carrierReconnectSessionGrace)
			return
		}
	}
	s.recordReconnect()
	logger.Infof("server reconnect reason=carrier - tearing down smux session")
	s.sessMu.RLock()
	current := s.session
	s.sessMu.RUnlock()
	s.reinstallSession(current)
}

func (s *Server) reinstallSession(dead *smux.Session) {
	s.reinstallMu.Lock()
	defer s.reinstallMu.Unlock()

	// Close the old muxconn immediately so that any in-flight Push calls
	// (from data arriving on a new bridge before this reinstall completes)
	// are discarded rather than feeding stale frames into the dying smux
	// session. Without this, a client that reconnects faster than the
	// server can push new-session smux frames into the old muxconn,
	// corrupting the old smux session's stream state (manifests as
	// "frame too large" on the control stream).
	s.sessMu.RLock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.sessMu.RUnlock()

	// Pre-build the replacement so we can swap atomically below.
	newConn := muxconn.New(s.ln, s.cipher)
	newSess, err := smux.Server(newConn, s.smuxConfig(linkMaxPayload(s.ln)))
	if err != nil {
		logger.Warnf("smux server init failed: %v", err)
		_ = newConn.Close()
		return
	}

	s.sessMu.Lock()
	if s.session != dead {
		// Someone else already reinstalled - discard our build.
		s.sessMu.Unlock()
		_ = newSess.Close()
		_ = newConn.Close()
		return
	}
	oldSess := s.session
	oldControl := s.controlStrm
	oldControlStop := s.controlStop
	oldSID := s.sessionID
	s.session = newSess
	s.conn = newConn
	s.controlStrm = nil
	s.controlStop = nil
	s.sessionID = ""
	s.deviceID = ""
	s.sessMu.Unlock()

	if oldControlStop != nil {
		oldControlStop()
	}
	if oldSess != nil {
		_ = oldSess.Close()
	}
	if oldControl != nil {
		_ = oldControl.Close()
	}
	if oldSID != "" {
		s.onClose(oldSID, "reconnect")
		s.trackPeerClose(oldSID, "reconnect")
	}
}

func (s *Server) closeSession() {
	s.sessMu.Lock()
	sess := s.session
	conn := s.conn
	control := s.controlStrm
	controlStop := s.controlStop
	peers := s.peerSessions
	s.peerSessions = make(map[string]*peerSession)
	s.session = nil
	s.conn = nil
	s.controlStrm = nil
	s.controlStop = nil
	oldSID := s.sessionID
	s.sessionID = ""
	s.deviceID = ""
	s.sessMu.Unlock()

	if controlStop != nil {
		controlStop()
	}
	notifyControlClose(control)
	if sess != nil {
		_ = sess.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
	if oldSID != "" {
		s.onClose(oldSID, "closed")
		s.trackPeerClose(oldSID, "closed")
	}
	for _, ps := range peers {
		s.closePeerSession(ps, "closed")
	}
}

func (s *Server) removePeerSession(peerID, reason string) {
	s.sessMu.Lock()
	ps := s.peerSessions[peerID]
	delete(s.peerSessions, peerID)
	s.sessMu.Unlock()
	if ps != nil {
		s.closePeerSession(ps, reason)
	}
}

func (s *Server) closePeerSession(ps *peerSession, reason string) {
	if ps.controlStop != nil {
		ps.controlStop()
	}
	notifyControlClose(ps.controlStrm)
	if ps.session != nil {
		_ = ps.session.Close()
	}
	if ps.conn != nil {
		_ = ps.conn.Close()
	}
	if ps.controlStrm != nil {
		_ = ps.controlStrm.Close()
	}
	if ps.sessionID != "" {
		s.onClose(ps.sessionID, reason)
		s.trackPeerClose(ps.sessionID, reason)
	}
}

// trackPeerOpen records a newly opened session and logs the live peer summary.
func (s *Server) trackPeerOpen(sessionID, deviceID string) {
	s.peersMu.Lock()
	s.peerStats[sessionID] = peerStat{deviceID: deviceID, openedAt: time.Now()}
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("peer connected: device=%s session=%s", deviceID, sessionID)
	logger.Infof("%s", line)
}

// trackPeerClose drops a closed session and logs a disconnect summary plus the
// live peer summary.
func (s *Server) trackPeerClose(sessionID, reason string) {
	s.peersMu.Lock()
	st, ok := s.peerStats[sessionID]
	if !ok {
		s.peersMu.Unlock()
		return // session was never tracked (or already removed) - avoid double count
	}
	delete(s.peerStats, sessionID)
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("peer disconnected: device=%s session=%s reason=%s duration=%s",
		st.deviceID, sessionID, reason, time.Since(st.openedAt).Round(time.Second))
	logger.Infof("%s", line)
}

// peersLineLocked builds the "Current peers count: N, Devices: [...]" summary
// line from the live sessions. The caller must hold peersMu.
func (s *Server) peersLineLocked() string {
	devices := make([]string, 0, len(s.peerStats))
	for _, st := range s.peerStats {
		devices = append(devices, st.deviceID)
	}
	sort.Strings(devices)
	return fmt.Sprintf("Current peers count: %d, Devices: [%s]", len(s.peerStats), strings.Join(devices, ", "))
}

// logPeersLine logs the current peer summary line (count + device list).
func (s *Server) logPeersLine() {
	s.peersMu.Lock()
	line := s.peersLineLocked()
	s.peersMu.Unlock()
	logger.Infof("%s", line)
}

func notifyControlClose(stream *smux.Stream) {
	if stream == nil {
		return
	}
	_ = stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := control.SendClose(stream); err == nil {
		time.Sleep(200 * time.Millisecond)
	}
	_ = stream.SetWriteDeadline(time.Time{})
	_ = stream.CloseWrite()
}

func (s *Server) onData(data []byte) {
	s.sessMu.RLock()
	conn := s.conn
	s.sessMu.RUnlock()
	if conn != nil {
		conn.Push(data)
	}
}

func (s *Server) onPeerData(peerID string, data []byte) {
	ps := s.getPeerSession(peerID)
	if ps == nil {
		return
	}
	ps.conn.Push(data)
}

func (s *Server) getPeerSession(peerID string) *peerSession {
	if peerID == "" || s.peerLn == nil {
		return nil
	}
	s.sessMu.Lock()
	if ps := s.peerSessions[peerID]; ps != nil {
		s.sessMu.Unlock()
		return ps
	}
	conn := muxconn.NewPeer(s.peerLn, s.cipher, peerID)
	sess, err := smux.Server(conn, s.smuxConfig(linkMaxPayload(s.ln)))
	if err != nil {
		s.sessMu.Unlock()
		logger.Warnf("smux server init failed for peer %s: %v", peerID, err)
		_ = conn.Close()
		return nil
	}
	ps := &peerSession{peerID: peerID, conn: conn, session: sess}
	s.peerSessions[peerID] = ps
	s.sessMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.servePeer(ps)
	}()
	return ps
}

// serve drives the smux Accept loop. The first accepted stream on a given
// smux session is the control stream - the handshake runs there. Subsequent
// streams are tunnel streams and proxy traffic.
func (s *Server) serve(ctx context.Context) {
	if s.peerLn != nil {
		<-ctx.Done()
		return
	}
	s.serveSingle(ctx)
}

func (s *Server) serveSingle(ctx context.Context) {
	for {
		if contextDone(ctx) {
			return
		}

		s.sessMu.RLock()
		sess := s.session
		s.sessMu.RUnlock()
		if sess == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
				continue
			}
		}

		if !s.handshakeReady() {
			if !s.acceptHandshake(ctx, sess) {
				continue
			}
		}

		logger.Debugf("serveSingle: waiting for AcceptStream (session=%s)", s.currentSessionID())
		stream, err := sess.AcceptStream()
		if err != nil {
			if s.handleAcceptError(ctx, sess) {
				return
			}
			continue
		}
		logger.Infof("serveSingle: accepted stream sid=%d (session=%s)", stream.ID(), s.currentSessionID())

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleStream(ctx, stream, s.currentSessionID())
		}()
	}
}

// handleAcceptError handles a failed AcceptStream. Returns true if the server should stop.
func (s *Server) handleAcceptError(ctx context.Context, sess *smux.Session) bool {
	if contextDone(ctx) {
		return true
	}
	logger.Debugf("AcceptStream returned error - reinstalling session")
	s.reinstallSession(sess)
	// Do NOT trigger carrier reconnect here. The smux session can close for
	// many benign reasons: the deferred reconnect closed it intentionally,
	// a client disconnected cleanly, or the session timed out waiting for
	// a new client. Triggering a carrier reconnect on every AcceptStream
	// error causes a Telemost ICE rebuild on each client reconnect, which
	// breaks MID binding and forces clients to wait for a new SDP exchange.
	return false
}

func (s *Server) currentSessionID() string {
	s.sessMu.RLock()
	defer s.sessMu.RUnlock()
	return s.sessionID
}

func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func isBenignWelcomeWriteError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "send welcome") && isPeerConsumedMoreThanSent(err)
}

func isPeerConsumedMoreThanSent(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "peer consumed more than sent")
}

// handshakeReady reports whether the current session has completed its
// handshake. The session is reset on reconnect, so this is recomputed.
func (s *Server) handshakeReady() bool {
	s.sessMu.RLock()
	defer s.sessMu.RUnlock()
	return s.sessionID != ""
}

func (s *Server) acceptHandshake(ctx context.Context, sess *smux.Session) bool {
	// Retry loop: after a session reinstall, stale control frames from the
	// old client smux session may arrive on the new smux session with a
	// matching stream ID. These raw JSON bytes (e.g. CONTROL_PING) are
	// interpreted by the framing layer as an impossibly large length prefix,
	// triggering ErrFrameTooLarge. We close the polluted stream and accept
	// the next one (the real handshake).
	const maxStaleRetries = 3
	for retry := 0; retry <= maxStaleRetries; retry++ {
		stream, err := sess.AcceptStream()
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			logger.Debugf("AcceptStream(control) returned %v - reinstalling session", err)
			s.resetLinkPeer()
			s.reinstallSession(sess)
			return false
		}
		_ = stream.SetDeadline(time.Now().Add(handshake.DefaultTimeout))
		hello, sid, err := handshake.Server(stream, s.authHook)
		_ = stream.SetDeadline(time.Time{})
		if err != nil {
			if sid == "" || !isBenignWelcomeWriteError(err) {
				_ = stream.Close()
				if errors.Is(err, framing.ErrFrameTooLarge) && retry < maxStaleRetries {
					logger.Debugf("handshake: discarding stale stream (attempt %d): %v", retry+1, err)
					continue
				}
				logger.Warnf("handshake failed: %v", err)
				s.resetLinkPeer()
				s.reinstallSession(sess)
				return false
			}
			// smux can report "peer consumed more than sent" after the welcome
			// frame was already delivered to the client. Treat this specific
			// post-write error as a soft success and let the immediate control
			// ping/pong prove whether the reverse path is actually alive. If the
			// client did not receive SERVER_WELCOME, no CONTROL_PONG will arrive
			// and the normal liveness/deferred-reconnect path will repair it.
			logger.Warnf("handshake welcome write returned benign smux error; proceeding: %v", err)
		}
		s.sessMu.Lock()
		s.deviceID = hello.DeviceID
		s.sessionID = sid
		s.sessMu.Unlock()
		now := time.Now()
		s.sessionOpenedAt.Store(&now)
		s.recordSession(sid)
		s.onOpen(sid, hello.DeviceID, hello.Claims)
		s.trackPeerOpen(sid, hello.DeviceID)
		logger.Infof("session %s opened (device=%s)", sid, hello.DeviceID)
		s.startControlLoop(ctx, sess, stream)
		return true
	}
	return false
}

func (s *Server) servePeer(ps *peerSession) {
	if !s.acceptPeerHandshake(ps) {
		s.removePeerSession(ps.peerID, "closed")
		return
	}
	for {
		if s.stopping() {
			return
		}
		stream, err := ps.session.AcceptStream()
		if err != nil {
			if s.stopping() {
				return
			}
			logger.Debugf("AcceptStream(peer=%s) returned %v - closing peer session", ps.peerID, err)
			s.removePeerSession(ps.peerID, "closed")
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleStream(context.Background(), stream, ps.sessionID)
		}()
	}
}

func (s *Server) acceptPeerHandshake(ps *peerSession) bool {
	const maxStaleRetries = 3
	for retry := 0; retry <= maxStaleRetries; retry++ {
		stream, err := ps.session.AcceptStream()
		if err != nil {
			if !s.stopping() {
				logger.Debugf("AcceptStream(control peer=%s) returned %v", ps.peerID, err)
			}
			return false
		}
		_ = stream.SetDeadline(time.Now().Add(handshake.DefaultTimeout))
		hello, sid, err := handshake.Server(stream, s.authHook)
		_ = stream.SetDeadline(time.Time{})
		if err != nil {
			_ = stream.Close()
			if errors.Is(err, framing.ErrFrameTooLarge) && retry < maxStaleRetries {
				logger.Debugf("handshake failed peer=%s: discarding stale stream (attempt %d): %v", ps.peerID, retry+1, err)
				continue
			}
			logger.Warnf("handshake failed peer=%s: %v", ps.peerID, err)
			return false
		}
		ps.controlStrm = stream
		ps.deviceID = hello.DeviceID
		ps.sessionID = sid
		s.recordSession(sid)
		s.onOpen(sid, hello.DeviceID, hello.Claims)
		s.trackPeerOpen(sid, hello.DeviceID)
		logger.Infof("session %s opened (device=%s peer=%s)", sid, hello.DeviceID, ps.peerID)
		s.startPeerControlLoop(ps, stream)
		return true
	}
	return false
}

func (s *Server) resetLinkPeer() {
	s.sessMu.RLock()
	ln := s.ln
	s.sessMu.RUnlock()
	if resetter, ok := ln.(interface{ ResetPeer() }); ok {
		resetter.ResetPeer()
	}
}

func (s *Server) startControlLoop(ctx context.Context, sess *smux.Session, stream *smux.Stream) {
	controlCtx, stop := context.WithCancel(ctx)
	s.sessMu.Lock()
	s.controlStrm = stream
	s.controlStop = stop
	s.sessMu.Unlock()

	liveness := s.liveness
	onPong := liveness.OnPong
	onMissedPong := liveness.OnMissedPong
	onUnhealthy := liveness.OnUnhealthy
	liveness.OnPong = func(h control.Health) {
		s.sessMu.RLock()
		sid := s.sessionID
		s.sessMu.RUnlock()
		s.recordPong(h)
		// A server-side handshake only proves that SERVER_WELCOME was written.
		// The first CONTROL_PONG proves the Android client received the welcome
		// and can send data back on the same control stream. Use that stronger
		// signal to suppress reconnect-on-new-participant repair reconnects.
		if s.signalHandshakeComplete != nil {
			s.signalHandshakeComplete()
		}
		logger.Debugf("control alive session=%s rtt=%v seq=%d", sid, h.RTT, h.Seq)
		if onPong != nil {
			onPong(h)
		}
	}
	liveness.OnMissedPong = func(missed int) {
		s.recordMissed(missed)
		logger.Warnf("control missed pong on server: missed_pongs=%d", missed)
		if onMissedPong != nil {
			onMissedPong(missed)
		}
	}
	liveness.OnUnhealthy = func(missed int) {
		s.recordUnhealthy(missed)
		logger.Warnf("control stream unhealthy on server: missed_pongs=%d", missed)
		if onUnhealthy != nil {
			onUnhealthy(missed)
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { _ = stream.Close() }()
		err := control.Run(controlCtx, stream, liveness)
		if controlCtx.Err() != nil || ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warnf("server control stream ended: %v", err)
		}
		if isPeerConsumedMoreThanSent(err) {
			logger.Warnf("server control stream ended with benign smux write accounting error; keeping smux session for data streams")
			return
		}
		s.recordReconnect()
		logger.Infof("server reconnect reason=liveness - reinstalling smux session")
		s.resetLinkPeer()
		s.reinstallSession(sess)
		// Only tell the carrier to rebuild if the session was stable for a
		// while. If the session just opened (< 120s ago), the client may be
		// blocked on the Android VPN consent dialog. In that case the client
		// will reconnect on its own after the dialog is dismissed, and we
		// should keep the Telemost channel alive so it can do so without
		// waiting for a full ICE/WebRTC renegotiation.
		const carrierReconnectGrace = 120 * time.Second
		if openedAt := s.sessionOpenedAt.Load(); openedAt == nil || time.Since(*openedAt) >= carrierReconnectGrace {
			if s.ln != nil {
				logger.Infof("server liveness: triggering carrier reconnect (session age >= %s)", carrierReconnectGrace)
				s.ln.Reconnect("liveness")
			}
		} else {
			logger.Infof("server liveness: skipping carrier reconnect (session age = %s < %s)", time.Since(*openedAt).Round(time.Second), carrierReconnectGrace)
		}
	}()
}

func (s *Server) startPeerControlLoop(ps *peerSession, stream *smux.Stream) {
	controlCtx, stop := context.WithCancel(context.Background())
	ps.controlStop = stop

	liveness := s.liveness
	onPong := liveness.OnPong
	onMissedPong := liveness.OnMissedPong
	onUnhealthy := liveness.OnUnhealthy
	liveness.OnPong = func(h control.Health) {
		s.recordPong(h)
		logger.Debugf("control alive session=%s peer=%s rtt=%v seq=%d", ps.sessionID, ps.peerID, h.RTT, h.Seq)
		if onPong != nil {
			onPong(h)
		}
	}
	liveness.OnMissedPong = func(missed int) {
		s.recordMissed(missed)
		logger.Warnf("control missed pong on server: session=%s peer=%s missed_pongs=%d",
			ps.sessionID, ps.peerID, missed)
		if onMissedPong != nil {
			onMissedPong(missed)
		}
	}
	liveness.OnUnhealthy = func(missed int) {
		s.recordUnhealthy(missed)
		logger.Warnf("control stream unhealthy on server: session=%s peer=%s missed_pongs=%d",
			ps.sessionID, ps.peerID, missed)
		if onUnhealthy != nil {
			onUnhealthy(missed)
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { _ = stream.Close() }()
		err := control.Run(controlCtx, stream, liveness)
		if controlCtx.Err() != nil || s.stopping() {
			return
		}
		if err != nil {
			logger.Warnf("server control stream ended session=%s peer=%s: %v", ps.sessionID, ps.peerID, err)
		}
		s.recordReconnect()
		s.removePeerSession(ps.peerID, "reconnect")
	}()
}

func (s *Server) stopping() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Status returns the latest server-side control health snapshot.
func (s *Server) Status() control.Status {
	return s.health.Status()
}

func (s *Server) recordSession(sessionID string) { s.health.RecordSession(sessionID) }
func (s *Server) recordPong(h control.Health)    { s.health.RecordPong(h) }
func (s *Server) recordMissed(missed int)        { s.health.RecordMissed(missed) }
func (s *Server) recordUnhealthy(missed int)     { s.health.RecordUnhealthy(missed) }
func (s *Server) recordReconnect()               { s.health.RecordReconnect() }

func (s *Server) shutdown() {
	if s.done != nil {
		s.doneOnce.Do(func() { close(s.done) })
	}
	s.closeSession()
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

func (s *Server) handleStream(_ context.Context, stream *smux.Stream, sessionID string) {
	defer func() { _ = stream.Close() }()
	if sessionID == "" {
		sessionID = s.currentSessionID()
	}

	// Read the connect JSON. The client writes the whole JSON in one
	// stream.Write so it usually arrives intact; tolerate fragmentation
	// by reading incrementally up to a sane cap.
	const maxConnReq = 4096
	header := make([]byte, 0, 256)
	tmp := make([]byte, 256)
	_ = stream.SetReadDeadline(time.Now().Add(15 * time.Second))
	for {
		n, err := stream.Read(tmp)
		if n > 0 {
			header = append(header, tmp[:n]...)
			if req, headerLen, ok := parseStreamRequest(header); ok {
				_ = stream.SetReadDeadline(time.Time{})
				logger.Infof("sid=%d handleStream cmd=%s addr=%s port=%d", stream.ID(), req.Cmd, req.Addr, req.Port)
				s.dispatch(stream, req, sessionID, header[headerLen:])
				return
			}
		}
		if err != nil {
			logger.Debugf("sid=%d handleStream read error after %d bytes: %v", stream.ID(), len(header), err)
			return
		}
		if len(header) > maxConnReq {
			return
		}
	}
}

func parseConnectRequest(buf []byte) (ConnectRequest, bool) {
	req, _, ok := parseStreamRequest(buf)
	return req, ok
}

func parseStreamRequest(buf []byte) (ConnectRequest, int, bool) {
	var req ConnectRequest
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&req); err != nil {
		return req, 0, false
	}
	if req.Cmd != connectCommand && req.Cmd != udpDialCommand {
		return req, 0, false
	}
	return req, int(dec.InputOffset()), true
}

// defaultAuthHook admits every client and assigns a random session ID.
// Replace it via [Config.AuthHook] to plug in real authorization.
func defaultAuthHook(_ string, _ map[string]any) (string, error) {
	return uuid.NewString(), nil
}

func (s *Server) dispatch(stream *smux.Stream, req ConnectRequest, sessionID string, initial []byte) {
	if req.Cmd == udpDialCommand {
		s.dispatchUDPDial(stream, req, initial)
		return
	}

	s.dispatchConnect(stream, req, sessionID, initial)
}

func (s *Server) dispatchConnect(stream *smux.Stream, req ConnectRequest, sessionID string, initial []byte) {
	addr := net.JoinHostPort(req.Addr, strconv.Itoa(req.Port))
	logger.Infof("sid=%d connect %s", stream.ID(), addr)

	dialStart := time.Now()
	conn, err := s.dial(req)
	dialElapsed := time.Since(dialStart)

	if err != nil {
		logger.Infof("sid=%d dial %s failed (%v): %v", stream.ID(), addr, dialElapsed, err)
		return
	}
	defer func() { _ = conn.Close() }()

	logger.Infof("sid=%d connected %s in %v", stream.ID(), addr, dialElapsed)

	if _, err := stream.Write([]byte{0x00}); err != nil {
		return
	}
	if len(initial) > 0 {
		if _, err := conn.Write(initial); err != nil {
			return
		}
	}

	var bytesOut uint64
	done := make(chan struct{})
	go func() {
		n, _ := io.Copy(stream, conn)
		if n > 0 {
			bytesOut = uint64(n)
		}
		_ = stream.Close()
		close(done)
	}()
	in, _ := io.Copy(conn, stream)
	_ = conn.Close()
	<-done
	bytesIn := uint64(0)
	if in > 0 {
		bytesIn = uint64(in)
	}
	if s.onTraffic != nil {
		s.onTraffic(sessionID, addr, bytesIn, bytesOut)
	}
}

func (s *Server) dispatchUDPDial(stream *smux.Stream, req ConnectRequest, initial []byte) {
	addr := net.JoinHostPort(req.Addr, strconv.Itoa(req.Port))
	logger.Infof("sid=%d udp-dial %s", stream.ID(), addr)

	dialer := &net.Dialer{
		Timeout:  10 * time.Second,
		Resolver: s.resolver,
	}
	conn, err := dialer.Dial("udp", addr)
	if err != nil {
		logger.Infof("sid=%d udp-dial %s failed: %v", stream.ID(), addr, err)
		return
	}
	defer func() { _ = conn.Close() }()

	go s.frameUDPReplies(stream, conn)

	reader := io.Reader(stream)
	if len(initial) > 0 {
		reader = io.MultiReader(bytes.NewReader(initial), stream)
	}
	var packetsIn uint64
	for {
		packet, err := framing.ReadBytes(reader, maxUDPPacketSize)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				logger.Infof("sid=%d udp-dial read frame failed after recv=%d: %v", stream.ID(), packetsIn, err)
			} else if packetsIn > 0 {
				logger.Infof("sid=%d udp-dial stream ended after recv=%d", stream.ID(), packetsIn)
			}
			return
		}
		if len(packet) == 0 {
			continue
		}
		packetsIn++
		if packetsIn <= 10 || packetsIn%100 == 0 {
			logger.Infof("sid=%d udp-dial recv #%d len=%d -> %s", stream.ID(), packetsIn, len(packet), addr)
		}
		if _, err := conn.Write(packet); err != nil {
			logger.Debugf("sid=%d udp-dial write failed: %v", stream.ID(), err)
			return
		}
	}
}

func (s *Server) frameUDPReplies(stream *smux.Stream, conn net.Conn) {
	buf := make([]byte, maxUDPPacketSize)
	var packetsOut uint64
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if packetsOut > 0 {
				logger.Infof("sid=%d udp-dial remote UDP read ended after replies=%d: %v", stream.ID(), packetsOut, err)
			}
			return
		}
		packetsOut++
		if packetsOut <= 10 || packetsOut%100 == 0 {
			logger.Infof("sid=%d udp-dial reply #%d len=%d", stream.ID(), packetsOut, n)
		}
		if err := framing.WriteBytes(stream, buf[:n], maxUDPPacketSize); err != nil {
			logger.Infof("sid=%d udp-dial reply write failed after replies=%d: %v", stream.ID(), packetsOut, err)
			_ = stream.Close()
			return
		}
	}
}

func (s *Server) dial(req ConnectRequest) (net.Conn, error) {
	addr := net.JoinHostPort(req.Addr, strconv.Itoa(req.Port))
	if s.socksProxyAddr == "" {
		dialer := &net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Resolver:  s.resolver,
		}
		conn, err := dialer.Dial("tcp4", addr)
		if err != nil {
			return nil, fmt.Errorf("dial failed: %w", err)
		}
		return conn, nil
	}

	proxyAddr := net.JoinHostPort(s.socksProxyAddr, strconv.Itoa(s.socksProxyPort))
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.Dial("tcp4", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial proxy: %w", err)
	}

	if err := s.socks5Connect(conn, req.Addr, req.Port); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *Server) socks5Connect(conn net.Conn, targetAddr string, targetPort int) error {
	if err := s.socks5Authenticate(conn); err != nil {
		return err
	}

	addrLen := len(targetAddr)
	if addrLen > 255 {
		addrLen = 255
		targetAddr = targetAddr[:255]
	}

	req := make([]byte, 0, 7+addrLen)
	req = append(req, 5, 1, 0, 3, byte(addrLen))
	req = append(req, []byte(targetAddr)...)
	req = append(req, byte(targetPort>>8), byte(targetPort)) //nolint:gosec,lll // G115: bounded conversion verified by surrounding logic

	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("failed to write socks5 connect req: %w", err)
	}

	resp := make([]byte, 10)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("failed to read socks5 connect resp: %w", err)
	}
	if resp[0] != 5 || resp[1] != 0 {
		return fmt.Errorf("%w: %d", ErrSocks5ConnectFailed, resp[1])
	}

	return nil
}

func (s *Server) socks5Authenticate(conn net.Conn) error {
	if s.socksProxyUser != "" {
		// Offer username/password auth (RFC 1929) only.
		if _, err := conn.Write([]byte{5, 1, 2}); err != nil {
			return fmt.Errorf("failed to write socks5 auth: %w", err)
		}
	} else {
		// No authentication.
		if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
			return fmt.Errorf("failed to write socks5 auth: %w", err)
		}
	}

	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("failed to read socks5 auth resp: %w", err)
	}
	if resp[0] != 5 {
		return ErrSocks5AuthFailed
	}
	switch resp[1] {
	case 0: // no auth accepted
		if s.socksProxyUser != "" {
			return ErrSocks5AuthFailed
		}
	case 2: // username/password
		return s.socks5SendCredentials(conn)
	default:
		return ErrSocks5AuthFailed
	}
	return nil
}

func (s *Server) socks5SendCredentials(conn net.Conn) error {
	user := s.socksProxyUser
	pass := s.socksProxyPass
	if len(user) > 255 {
		user = user[:255]
	}
	if len(pass) > 255 {
		pass = pass[:255]
	}
	authMsg := make([]byte, 0, 3+len(user)+len(pass))
	authMsg = append(authMsg, 1, byte(len(user))) //nolint:gosec // G115: len clamped to ≤255 above
	authMsg = append(authMsg, []byte(user)...)
	authMsg = append(authMsg, byte(len(pass))) //nolint:gosec // G115: len clamped to ≤255 above
	authMsg = append(authMsg, []byte(pass)...)
	if _, err := conn.Write(authMsg); err != nil {
		return fmt.Errorf("failed to write socks5 credentials: %w", err)
	}
	authResp := make([]byte, 2)
	if _, err := io.ReadFull(conn, authResp); err != nil {
		return fmt.Errorf("failed to read socks5 credentials resp: %w", err)
	}
	if authResp[1] != 0 {
		return ErrSocks5AuthFailed
	}
	return nil
}
