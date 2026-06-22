// Package goolom implements an engine.Session backed by the Goolom SFU
// signaling protocol. Goolom is the proprietary SFU developed for Yandex
// Telemost; the on-wire protocol - capabilities offer, separated subscriber
// and publisher PeerConnections, ack/pong keepalive, slots-based subscribe
// model - is what this engine speaks.
//
// HTTP auth (room-info lookup, telemetry referer, etc.) lives in the auth
// package; this engine consumes a media-server WebSocket URL plus the
// peer/room/credentials tuple supplied as engine.Config.
package goolom

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

const (
	realDataChannelMessageLimit = 12288
	defaultSendDelayLow         = 2 * time.Millisecond
	defaultSendDelayMax         = 12 * time.Millisecond
	defaultTelemetryInterval    = 20 * time.Second
	defaultSendQueueSize        = 5000
	defaultBufferHighWaterMark  = 512 * 1024
	defaultSendQueueCapHard     = 4000

	wsReadTimeout      = 60 * time.Second
	wsHandshakeTimeout = 15 * time.Second

	keyUID          = "uid"
	keyDescription  = "description"
	keyPcSeq        = "pcSeq"
	keyName         = "name"
	stateTerminated = "terminated"

	credentialKeyRoomID           = "roomID"
	credentialKeyCredentials      = "credentials"
	credentialKeyRoomURL          = "roomURL"
	credentialKeyTelemetryReferer = "telemetryReferer"
)

var (
	// ErrDataChannelTimeout is returned when the DataChannel fails to open in time.
	ErrDataChannelTimeout = errors.New("datachannel timeout")
	// ErrDataChannelNotReady is returned when send is called before the DataChannel is open.
	ErrDataChannelNotReady = errors.New("datachannel not ready")
	// ErrSendQueueClosed is returned when send is called after Close.
	ErrSendQueueClosed = errors.New("send queue closed")
	// ErrSendQueueTimeout is returned when the send queue cannot accept new data in time.
	ErrSendQueueTimeout = errors.New("send queue timeout")
	// ErrSessionClosed is returned when the session is closed mid-operation.
	ErrSessionClosed = errors.New("session closed")
	// ErrPeerClosed is returned when the peer is closed mid-operation.
	ErrPeerClosed = errors.New("peer closed")
	// ErrSubscriberMediaTimeout is returned when the subscriber media is not ready in time.
	ErrSubscriberMediaTimeout = errors.New("subscriber media timeout")
	// ErrPublisherNotInitialized is returned when the publisher PC is not set up.
	ErrPublisherNotInitialized = errors.New("publisher peer connection not initialized")
	// ErrURLRequired is returned when no media-server WebSocket URL was supplied.
	ErrURLRequired = errors.New("goolom media server URL required")
	// ErrRoomIDRequired is returned when no room ID was supplied.
	ErrRoomIDRequired = errors.New("goolom room ID required")
	// ErrPeerIDRequired is returned when no peer ID was supplied.
	ErrPeerIDRequired = errors.New("goolom peer ID required")
	// ErrNoRefresh is returned when reconnect is attempted without a refresh callback.
	ErrNoRefresh = errors.New("goolom reconnect: no refresh callback supplied")
)

// TrafficShape controls outgoing data-channel pacing.
type TrafficShape struct {
	MaxMessageSize int
	MinDelay       time.Duration
	MaxDelay       time.Duration
}

// Session is the Goolom engine handle.
type Session struct {
	name             string
	mediaServerURL   string
	peerID           string
	roomID           string
	credentials      string
	roomURL          string // referer for telemetry - opaque to the engine
	telemetryReferer string
	refresh          func(ctx context.Context) (engine.Credentials, error)

	ws        *websocket.Conn
	wsMu      sync.Mutex
	pcSub     *webrtc.PeerConnection
	pcPub     *webrtc.PeerConnection
	dc        *webrtc.DataChannel
	iceUDPMux ice.UDPMux

	onData          func([]byte)
	onReconnect     func(*webrtc.DataChannel)
	onReconnecting  func() // called at the start of reconnect(), before WS/PC teardown
	shouldReconnect func() bool
	onEnded         func(string)

	reconnectCh    chan struct{}
	closeCh        chan struct{}
	keepAliveCh    chan struct{}
	telemetryCh    chan struct{}
	sessionCloseCh chan struct{}
	lastReconnect  time.Time
	reconnectCount int
	sessionMu      sync.Mutex

	sendQueue       chan []byte
	sendQueueClosed atomic.Bool
	closed          atomic.Bool
	reconnecting    atomic.Bool
	telemetryActive atomic.Bool
	setSlotsKey     atomic.Uint64

	ackMu      sync.Mutex
	ackWaiters map[string]chan struct{}

	signalSummaryMu     sync.Mutex
	signalSummaryCounts map[string]int

	trafficShape TrafficShape

	// reconnectOnNewParticipant causes the session to reconnect when a new
	// participant with sendVideo=true joins. This is used by the OLCRTC server
	// to ensure fresh SDP exchanges when a new client joins an existing room.
	// Without this, Telemost's initial MID binding fails permanently when the
	// publisher was already in the room before the subscriber connected.
	// Uses CompareAndSwap to ensure only ONE reconnect per session lifetime.
	reconnectOnNewParticipantEnabled atomic.Bool
	reconnectOnNewParticipant        atomic.Bool

	// deferredReconnectCh is closed by the server after the first control pong,
	// proving the client received SERVER_WELCOME and the reverse VP8/KCP path is
	// alive. reconnectOnNewParticipant uses this to avoid repair reconnects while
	// the client is actually live.
	deferredReconnectCh chan struct{}
	// deferredReconnectPending is true while reconnectOnNewParticipant is waiting
	// for either control-path proof or positive Telemost evidence that MID binding
	// is broken. It lets slotsConfig fast-path trigger the repair reconnect before
	// the fallback timer expires.
	deferredReconnectPending atomic.Bool

	// skipCredentialRefresh tells reconnect() to skip the s.refresh(ctx) call
	// (HTTP round-trip to Telemost API) and reuse existing room credentials.
	// Set by checkNewParticipant() since the room credentials don't change
	// between reconnects for the same room. This reduces reconnect time from
	// ~46s to ~5s, keeping it within the client's 20s handshake timeout.
	skipCredentialRefresh atomic.Bool

	// skipMediaReady tells Connect() to skip waitForMediaReady(). During
	// reconnectOnNewParticipant, the server just needs the subscriber PC
	// ICE-connected (happens in ~1s) — it doesn't need to wait for the
	// full 20s media-ready timeout. Set alongside skipCredentialRefresh.
	skipMediaReady atomic.Bool

	// lastHandshakeAt is set to the current time (Unix ns) when
	// SignalHandshakeComplete is called. In the OLCRTC server this is now the
	// first control-pong time, not merely the SERVER_WELCOME write time.
	lastHandshakeAt atomic.Int64

	videoTrackMu    sync.RWMutex
	videoTracks     []webrtc.TrackLocal
	onVideoTrack    func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
	subscriberReady atomic.Bool
	publisherReady  atomic.Bool
	subscriberConn  chan struct{}
	publisherConn   chan struct{}
	wg              sync.WaitGroup

	httpClient *http.Client
}

// New creates a new Goolom engine session.
//
// cfg.URL is the media server WebSocket URL. cfg.Token carries the peer ID.
// cfg.Extra carries the rest of the room tuple: roomID, credentials, and an
// optional roomURL / telemetryReferer string the engine uses verbatim as the
// Referer header for telemetry posts.
func New(_ context.Context, cfg engine.Config) (engine.Session, error) {
	if cfg.URL == "" {
		return nil, ErrURLRequired
	}
	peerID := cfg.Token
	if peerID == "" {
		return nil, ErrPeerIDRequired
	}
	roomID := ""
	credentials := ""
	roomURL := ""
	telemetryReferer := ""
	if cfg.Extra != nil {
		roomID = cfg.Extra[credentialKeyRoomID]
		credentials = cfg.Extra[credentialKeyCredentials]
		roomURL = cfg.Extra[credentialKeyRoomURL]
		telemetryReferer = cfg.Extra[credentialKeyTelemetryReferer]
	}
	if roomID == "" {
		return nil, ErrRoomIDRequired
	}
	if telemetryReferer == "" {
		telemetryReferer = roomURL
	}

	return &Session{
		name:                cfg.Name,
		mediaServerURL:      cfg.URL,
		peerID:              peerID,
		roomID:              roomID,
		credentials:         credentials,
		roomURL:             roomURL,
		telemetryReferer:    telemetryReferer,
		refresh:             cfg.Refresh,
		onData:              cfg.OnData,
		reconnectCh:         make(chan struct{}, 1),
		closeCh:             make(chan struct{}),
		keepAliveCh:         make(chan struct{}),
		sessionCloseCh:      make(chan struct{}),
		telemetryCh:         make(chan struct{}, 1),
		sendQueue:           make(chan []byte, defaultSendQueueSize),
		ackWaiters:          make(map[string]chan struct{}),
		signalSummaryCounts: make(map[string]int),
		subscriberConn:      make(chan struct{}),
		publisherConn:       make(chan struct{}),
		trafficShape: TrafficShape{
			MaxMessageSize: realDataChannelMessageLimit,
			MinDelay:       defaultSendDelayLow,
			MaxDelay:       defaultSendDelayMax,
		},
		deferredReconnectCh: make(chan struct{}),
	}, nil
}

// Capabilities reports what this engine can do.
func (s *Session) Capabilities() engine.Capabilities {
	return engine.Capabilities{ByteStream: true, VideoTrack: true}
}

// SetTrafficShape adjusts the outgoing data-channel pacing.
func (s *Session) SetTrafficShape(shape TrafficShape) {
	if shape.MaxMessageSize <= 0 {
		shape.MaxMessageSize = realDataChannelMessageLimit
	}
	if shape.MaxDelay < shape.MinDelay {
		shape.MaxDelay = shape.MinDelay
	}
	s.trafficShape = shape
}

// Send queues data for transmission.
func (s *Session) Send(data []byte) error {
	if s.dc == nil || s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return ErrDataChannelNotReady
	}
	if s.sendQueueClosed.Load() {
		return ErrSendQueueClosed
	}
	select {
	case s.sendQueue <- data:
		return nil
	case <-time.After(50 * time.Millisecond):
		return ErrSendQueueTimeout
	}
}

// GetSendQueue returns the transmission queue.
func (s *Session) GetSendQueue() chan []byte { return s.sendQueue }

// GetBufferedAmount returns the WebRTC buffered amount.
func (s *Session) GetBufferedAmount() uint64 {
	if s.dc != nil {
		return s.dc.BufferedAmount()
	}
	return 0
}

// SetEndedCallback sets the callback for connection termination.
func (s *Session) SetEndedCallback(cb func(string)) { s.onEnded = cb }

// SetReconnectCallback sets the callback for reconnection events.
func (s *Session) SetReconnectCallback(cb func(*webrtc.DataChannel)) { s.onReconnect = cb }

// SetShouldReconnect sets the policy for reconnection.
func (s *Session) SetShouldReconnect(fn func() bool) { s.shouldReconnect = fn }

// SetReconnectOnNewParticipant enables automatic reconnection when a new
// participant with video joins. Used by the OLCRTC server to ensure Telemost
// provides fresh SDP exchanges with proper MID binding.
func (s *Session) SetReconnectOnNewParticipant(v bool) {
	s.reconnectOnNewParticipantEnabled.Store(v)
	s.reconnectOnNewParticipant.Store(v)
}

// SetOnReconnecting registers a callback that fires at the start of reconnect(),
// before WebSocket and PeerConnection teardown. The OLCRTC server uses this
// to close its smux session proactively, preventing the client from connecting
// to a dying session during the reconnect window.
func (s *Session) SetOnReconnecting(cb func()) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	s.onReconnecting = cb
}

// SignalHandshakeComplete closes the deferredReconnectCh, unblocking the
// reconnectOnNewParticipant goroutine. The OLCRTC server calls this after the
// first CONTROL_PONG, which proves SERVER_WELCOME reached the client and the
// control stream is usable in both directions.
func (s *Session) SignalHandshakeComplete() {
	s.lastHandshakeAt.Store(time.Now().UnixNano())
	s.sessionMu.Lock()
	ch := s.deferredReconnectCh
	s.sessionMu.Unlock()
	closeSignal(ch)
}

// CanSend checks if data can be sent.
func (s *Session) CanSend() bool {
	if s.onData == nil {
		if s.hasLocalVideoTracks() {
			return !s.closed.Load() && s.subscriberReady.Load() && s.publisherReady.Load()
		}
		return !s.closed.Load() && s.subscriberReady.Load()
	}
	if s.dc == nil || s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return false
	}
	return len(s.sendQueue) < defaultSendQueueCapHard
}

// AddVideoTrack adds a video track to the publisher peer connection.
func (s *Session) AddVideoTrack(track webrtc.TrackLocal) error {
	s.videoTrackMu.Lock()
	s.videoTracks = append(s.videoTracks, track)
	s.videoTrackMu.Unlock()

	if s.pcPub == nil {
		return nil
	}
	if _, err := s.pcPub.AddTrack(track); err != nil {
		return fmt.Errorf("failed to add track: %w", err)
	}
	return nil
}

// SetVideoTrackHandler registers a callback for remote video tracks.
func (s *Session) SetVideoTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	s.videoTrackMu.Lock()
	defer s.videoTrackMu.Unlock()
	s.onVideoTrack = cb
}

func (s *Session) hasLocalVideoTracks() bool {
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()
	return len(s.videoTracks) > 0
}

func (s *Session) videoTrackHandler() func(*webrtc.TrackRemote, *webrtc.RTPReceiver) {
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()
	return s.onVideoTrack
}

func (s *Session) attachPendingVideoTracks() error {
	s.videoTrackMu.RLock()
	defer s.videoTrackMu.RUnlock()

	for _, track := range s.videoTracks {
		if _, err := s.pcPub.AddTrack(track); err != nil {
			return fmt.Errorf("add video track: %w", err)
		}
	}
	return nil
}

func closeSignal(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func init() { //nolint:gochecknoinits // engine registration is the canonical Go pattern for plugins
	engine.Register("goolom", New)
}
