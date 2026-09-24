// Package client implements the local SOCKS5 client side of the olcrtc tunnel.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/limits"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

var (
	ErrConnectFailed           = errors.New("tunnel connection failed")
	ErrProxyAuth               = errors.New("SOCKS proxy auth failed")
	ErrKeySize                 = runtime.ErrKeySize
	ErrInvalidSOCKSVersion     = errors.New("invalid socks version")
	ErrUnsupportedSOCKSCommand = errors.New("unsupported socks command")
	ErrUnsupportedAddressType  = errors.New("unsupported address type")
	ErrRemoteNotReady          = errors.New("remote not ready")
	ErrSOCKSAuthFailed         = errors.New("SOCKS5 authentication failed")
	ErrSOCKSCredTooLong        = errors.New("socks5 user/pass exceeds 255 bytes")
	ErrEmptySOCKSDomain        = errors.New("empty socks5 domain")
	ErrSOCKSDomainTooLong      = errors.New("socks5 domain too long")
	// ErrClientStopped is returned when a client has shut down before a
	// requested reconnect can be accepted.
	ErrClientStopped = errors.New("client is stopped")
)

// SOCKS flow kinds tracked by the resource-profile caps.
const (
	flowKindTCP = "tcp"
	flowKindUDP = "udp"
)

const (
	// DefaultSocksSlotWait bounds backpressure for SOCKS flow ceilings. It is
	// deliberately finite: mobile Network Extensions that stall indefinitely are
	// vulnerable to jetsam.
	DefaultSocksSlotWait = 2 * time.Second
	// MaxSocksSlotWait clamps host-provided slot waits to a finite value.
	MaxSocksSlotWait = 10 * time.Second
	// maxSocksSlotWaiters bounds goroutines/fds retained only to wait for a flow
	// slot during bursty clients such as Speedtest.
	maxSocksSlotWaiters = 64
)

const (
	reconnectProvider = "provider"
	reconnectLiveness = "liveness"
	reconnectFallback = "liveness-fallback"
	// reconnectNetwork marks an explicit host-side network handover
	// (e.g. Wi-Fi -> cellular) requested through the mobile Reconnect API.
	reconnectNetwork = "network"
)

const (
	defaultLivenessFallback = 30 * time.Second
	defaultShutdownGrace    = 5 * time.Second
)

// Client handles local SOCKS5 connections and tunnels them to the server.
type Client struct {
	ln          transport.Transport
	keys        *crypto.KeySet
	pair        *tunnelcore.SessionPair
	conn        *muxconn.Conn
	controlConn *muxconn.Conn
	session     *smux.Session
	controlSess *smux.Session
	controlStrm *smux.Stream
	controlStop context.CancelFunc
	sessMu      sync.RWMutex
	reconnectMu sync.Mutex

	// Reconnect request path for host-driven network handover; guarded by
	// requestMu. requestReconnect is non-nil only while the request loop of
	// the current session is running.
	requestMu               sync.Mutex
	requestReconnect        func(reason string) error
	stopRequestReconnect    context.CancelFunc
	reconnectRequestPending chan string
	// requestDispatch replaces handleReconnect for queued requests. Tests
	// set it before starting the loop; production leaves it nil.
	requestDispatch func(context.Context, Config, context.CancelFunc, string)

	health *runtime.HealthTracker

	// controlLastPong is independent corroboration for the transport's fast
	// peer-restart heuristic, not a second session reconnect detector.
	controlLastPong  atomic.Value // time.Time
	deviceID         string
	sessionID        string
	claims           map[string]any
	resourceProfile  limits.Profile
	dnsServer        string
	socksUser        string
	socksPass        string
	sessionReady     chan struct{}
	wg               sync.WaitGroup
	socksMu          sync.Mutex
	socksConns       map[net.Conn]struct{}
	socksClosed      bool
	activeTCP        int64
	activeUDP        int64
	activeTotal      int64
	slotWaitAdmitted int64
	slotWaitRefused  int64
	flowSeq          uint64
	onFlowStats      FlowStatsFunc
	socksSlotWait    time.Duration
	slotWaiters      chan struct{}
	livenessFallback time.Duration
	shutdownGrace    time.Duration
	fallbackPending  atomic.Bool
}

// HealthFunc is called when the client control health snapshot changes.
type HealthFunc func(control.Status)

// Config holds runtime configuration for [Run], [RunWithReady], and [RunWithAddress].
type Config struct {
	Transport        string
	Provider         string
	RoomURL          string
	ChannelID        string
	KeyHex           string
	LocalAddr        string
	DNSServer        string
	Resolver         *net.Resolver
	SOCKSUser        string
	SOCKSPass        string
	TransportOptions transport.Options
	Engine           string
	URL              string
	Token            string
	ProviderToken    string
	Liveness         control.Config
	Traffic          transport.TrafficConfig
	DeviceID         string
	DeviceIDPath     string
	Claims           map[string]any
	OnHealth         HealthFunc
	OnFlowStats      func(FlowStats)

	// OnClientReady is invoked with the live client's reconnect capability
	// immediately before the caller-visible ready callback. Mobile hosts use
	// it to expose a non-terminal Reconnect request without stopping the
	// SOCKS listener. Nil means no-op.
	OnClientReady ConfigClientReadyFunc

	ResourceProfile limits.Profile
	SocksSlotWait   time.Duration
}

type FlowStats struct {
	Seq                               uint64
	TCP, UDP                          int64
	Total                             int64
	SlotWaitAdmitted, SlotWaitRefused int64
}

type FlowStatsFunc func(FlowStats)

// Run starts the client with the given configuration.
func Run(ctx context.Context, cfg Config) error {
	return RunWithAddress(ctx, cfg, nil)
}

// RunWithReady starts the client and invokes onReady after the SOCKS listener opens.
func RunWithReady(ctx context.Context, cfg Config, onReady func()) error {
	if onReady == nil {
		return RunWithAddress(ctx, cfg, nil)
	}
	return RunWithAddress(ctx, cfg, func(string) { onReady() })
}

// RunWithAddress starts the client and reports the actual SOCKS listener address.
func RunWithAddress(ctx context.Context, cfg Config, onReady func(actualAddr string)) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	keys, err := tunnelcore.SetupKeySet(cfg.KeyHex, crypto.Client)
	if err != nil {
		return fmt.Errorf("setup key set: %w", err)
	}
	deviceID, err := resolveDeviceID(cfg.DeviceID, cfg.DeviceIDPath)
	if err != nil {
		return fmt.Errorf("resolve device id: %w", err)
	}
	client := &Client{
		keys: keys, deviceID: deviceID, claims: cfg.Claims, dnsServer: cfg.DNSServer,
		socksUser: cfg.SOCKSUser, socksPass: cfg.SOCKSPass,
		resourceProfile:         limits.Normalize(cfg.ResourceProfile),
		socksSlotWait:           NormalizeSocksSlotWait(cfg.SocksSlotWait),
		slotWaiters:             make(chan struct{}, maxSocksSlotWaiters),
		health:                  runtime.NewHealthTracker(cfg.OnHealth),
		sessionReady:            make(chan struct{}),
		reconnectRequestPending: make(chan string, 1),
		onFlowStats:             cfg.OnFlowStats,
	}
	defer func() {
		cancel()
		client.shutdown()
	}()
	if bringUpErr := client.bringUpLink(runCtx, cfg, cancel); bringUpErr != nil {
		return bringUpErr
	}
	listener, err := (&net.ListenConfig{}).Listen(runCtx, "tcp4", cfg.LocalAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", cfg.LocalAddr, err)
	}
	defer func() { _ = listener.Close() }()
	actualAddr := listener.Addr().String()
	logger.Infof("SOCKS5 server listening on %s", actualAddr)
	// Publish the live client before ready becomes visible to mobile
	// callers. A network handover immediately after WaitReady must find the
	// active request path, not a half-initialized runtime.
	if cfg.OnClientReady != nil {
		cfg.OnClientReady(client)
	}
	if onReady != nil {
		onReady(actualAddr)
	}
	client.goTracked(func() { client.acceptLoop(runCtx, listener) })
	<-runCtx.Done()
	return nil
}

// NormalizeSocksSlotWait returns the effective bounded SOCKS slot wait.
func NormalizeSocksSlotWait(wait time.Duration) time.Duration {
	if wait <= 0 {
		return DefaultSocksSlotWait
	}
	if wait > MaxSocksSlotWait {
		return MaxSocksSlotWait
	}
	return wait
}

// registerSocksConn tracks conn so shutdown can close it, and enforces the
// accepted-client cap. Flow ceilings are handled later with bounded waiting.
// It reports false when the cap is reached or the client is tearing down; the
// caller then closes conn itself.
func (c *Client) registerSocksConn(conn net.Conn) bool {
	c.socksMu.Lock()
	defer c.socksMu.Unlock()
	if c.socksClosed {
		return false
	}
	if len(c.socksConns) >= c.maxSocksConns() {
		logger.Warnf("SOCKS5: %d concurrent connections reached, refusing new ones", c.maxSocksConns())
		return false
	}
	if c.socksConns == nil {
		c.socksConns = make(map[net.Conn]struct{})
	}
	c.socksConns[conn] = struct{}{}
	return true
}

func (c *Client) maxSocksConns() int {
	if c.resourceProfile.SOCKS.MaxTotal > 0 {
		return c.resourceProfile.SOCKS.MaxTotal + maxSocksSlotWaiters
	}
	return maxSocksConns
}

func (c *Client) tryBeginFlow(kind string) bool {
	c.socksMu.Lock()
	profile := c.resourceProfile.SOCKS
	if kind == flowKindTCP && profile.MaxTCP > 0 && c.activeTCP >= int64(profile.MaxTCP) {
		c.socksMu.Unlock()
		return false
	}
	if kind == flowKindUDP && profile.MaxUDP > 0 && c.activeUDP >= int64(profile.MaxUDP) {
		c.socksMu.Unlock()
		return false
	}
	if profile.MaxTotal > 0 && c.activeTotal >= int64(profile.MaxTotal) {
		c.socksMu.Unlock()
		return false
	}
	if kind == flowKindTCP {
		c.activeTCP++
	} else {
		c.activeUDP++
	}
	c.activeTotal++
	stats := c.nextFlowStatsLocked()
	c.socksMu.Unlock()
	c.publishFlowStats(stats)
	return true
}

func (c *Client) waitBeginFlow(ctx context.Context, conn net.Conn, kind string) (net.Conn, bool) {
	if c.tryBeginFlow(kind) {
		return conn, true
	}
	wait := NormalizeSocksSlotWait(c.socksSlotWait)
	if !c.tryAcquireSlotWaiter() {
		c.recordSlotWaitRefused()
		logger.Warnf("SOCKS5: slot waiter limit reached, refusing %s flow", kind)
		return conn, false
	}
	defer c.releaseSlotWaiter()

	peerConn, peerGone := watchPeerGone(conn, wait)
	defer peerConn.stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return peerConn.conn(), false
		case <-peerGone:
			return peerConn.conn(), false
		case <-timer.C:
			c.recordSlotWaitRefused()
			return peerConn.conn(), false
		case <-ticker.C:
			if c.tryBeginFlow(kind) {
				c.recordSlotWaitAdmitted()
				return peerConn.conn(), true
			}
		}
	}
}

func (c *Client) tryAcquireSlotWaiter() bool {
	c.socksMu.Lock()
	if c.slotWaiters == nil {
		c.slotWaiters = make(chan struct{}, maxSocksSlotWaiters)
	}
	waiters := c.slotWaiters
	c.socksMu.Unlock()
	select {
	case waiters <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *Client) releaseSlotWaiter() {
	c.socksMu.Lock()
	waiters := c.slotWaiters
	c.socksMu.Unlock()
	if waiters == nil {
		return
	}
	select {
	case <-waiters:
	default:
	}
}

func (c *Client) recordSlotWaitAdmitted() {
	c.socksMu.Lock()
	c.slotWaitAdmitted++
	stats := c.nextFlowStatsLocked()
	c.socksMu.Unlock()
	c.publishFlowStats(stats)
}

func (c *Client) recordSlotWaitRefused() {
	c.socksMu.Lock()
	c.slotWaitRefused++
	stats := c.nextFlowStatsLocked()
	c.socksMu.Unlock()
	c.publishFlowStats(stats)
}

func (c *Client) endFlow(kind string) {
	c.socksMu.Lock()
	if kind == flowKindTCP && c.activeTCP > 0 {
		c.activeTCP--
	}
	if kind == flowKindUDP && c.activeUDP > 0 {
		c.activeUDP--
	}
	if c.activeTotal > 0 {
		c.activeTotal--
	}
	stats := c.nextFlowStatsLocked()
	c.socksMu.Unlock()
	c.publishFlowStats(stats)
}

func (c *Client) nextFlowStatsLocked() FlowStats {
	c.flowSeq++
	return FlowStats{Seq: c.flowSeq, TCP: c.activeTCP, UDP: c.activeUDP, Total: c.activeTotal,
		SlotWaitAdmitted: c.slotWaitAdmitted, SlotWaitRefused: c.slotWaitRefused}
}

func (c *Client) publishFlowStats(stats FlowStats) {
	if c.onFlowStats != nil {
		c.onFlowStats(stats)
	}
}

func (c *Client) unregisterSocksConn(conn net.Conn) {
	c.socksMu.Lock()
	delete(c.socksConns, conn)
	c.socksMu.Unlock()
}

// closeSocksConns closes every live SOCKS connection. Without it shutdown
// would have to wait out the negotiation deadline of a client that connected
// and then went quiet.
func (c *Client) closeSocksConns() {
	c.socksMu.Lock()
	conns := c.socksConns
	c.socksConns = nil
	c.socksClosed = true
	c.socksMu.Unlock()
	for conn := range conns {
		_ = conn.Close()
	}
}

func (c *Client) goTracked(fn func()) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		fn()
	}()
}

func resolveDeviceID(deviceID, path string) (string, error) {
	if deviceID != "" {
		return deviceID, nil
	}
	if path == "" {
		return uuid.NewString(), nil
	}
	data, err := os.ReadFile(path) // #nosec G304 - path is explicit user configuration
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read device id %s: %w", path, err)
	}
	id := uuid.NewString()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("mkdir device id dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write device id %s: %w", path, err)
	}
	return id, nil
}
