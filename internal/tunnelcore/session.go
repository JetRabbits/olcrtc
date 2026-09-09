package tunnelcore

import (
	"errors"
	"fmt"
	"io"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/limits"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ErrUnknownSessionRole is returned for an unsupported smux constructor role.
var ErrUnknownSessionRole = errors.New("unknown smux role")

// SessionRole selects the matching smux constructor.
type SessionRole uint8

const (
	// ClientRole creates smux client sessions.
	ClientRole SessionRole = iota + 1
	// ServerRole creates smux server sessions.
	ServerRole
)

// SessionPair owns the data session and its optional isolated control session.
type SessionPair struct {
	DataConn       *muxconn.Conn
	DataSession    *smux.Session
	ControlConn    *muxconn.Conn
	ControlSession *smux.Session
}

// NewSessionPair builds data and optional isolated-control muxconn/smux sessions.
// If only the control session fails, the usable data pair is returned with the error.
func NewSessionPair(tr transport.Transport, keys *crypto.KeySet, role SessionRole) (*SessionPair, error) {
	return NewSessionPairWithProfile(tr, keys, role, limits.Default())
}

func NewSessionPairWithProfile(
	tr transport.Transport,
	keys *crypto.KeySet,
	role SessionRole,
	profile limits.Profile,
) (*SessionPair, error) {
	profile = limits.Normalize(profile)
	dataConn := muxconn.NewWithQueue(tr, keys, profile.MuxConn.DataInboundQueue)
	controlConn := muxconn.NewControlWithQueue(tr, keys, profile.MuxConn.ControlInboundQueue)
	return NewSessionPairWithConns(tr, dataConn, controlConn, role, profile)
}

// NewSessionPairWithConns builds a pair from muxconns already installed by the caller.
func NewSessionPairWithConns(
	tr transport.Transport,
	dataConn, controlConn *muxconn.Conn,
	role SessionRole,
	profile limits.Profile,
) (*SessionPair, error) {
	profile = limits.Normalize(profile)
	dataSession, err := NewSession(dataConn, role, runtime.SmuxConfigForProfile(tr, profile))
	if err != nil {
		_ = dataConn.Close()
		if controlConn != nil {
			_ = controlConn.Close()
		}
		return nil, fmt.Errorf("data smux session: %w", err)
	}

	pair := &SessionPair{
		DataConn:       dataConn,
		DataSession:    dataSession,
		ControlSession: dataSession,
	}
	if controlConn == nil {
		return pair, nil
	}
	pair.ControlConn = controlConn
	controlCfg := runtime.ControlSmuxConfigWithProfile(runtime.MaxPayload(tr), profile)
	controlSession, err := NewSession(controlConn, role, controlCfg)
	if err != nil {
		_ = controlConn.Close()
		pair.ControlConn = nil
		return pair, fmt.Errorf("control smux session: %w", err)
	}
	pair.ControlSession = controlSession
	return pair, nil
}

// NewControlSession builds only an isolated control muxconn/smux session.
func NewControlSession(
	tr transport.Transport,
	keys *crypto.KeySet,
	role SessionRole,
) (*muxconn.Conn, *smux.Session, error) {
	return NewControlSessionWithProfile(tr, keys, role, limits.Default())
}

func NewControlSessionWithProfile(
	tr transport.Transport,
	keys *crypto.KeySet,
	role SessionRole,
	profile limits.Profile,
) (*muxconn.Conn, *smux.Session, error) {
	profile = limits.Normalize(profile)
	conn := muxconn.NewControlWithQueue(tr, keys, profile.MuxConn.ControlInboundQueue)
	if conn == nil {
		return nil, nil, nil
	}
	session, err := NewSession(conn, role, runtime.ControlSmuxConfigWithProfile(runtime.MaxPayload(tr), profile))
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("control smux session: %w", err)
	}
	return conn, session, nil
}

// NewSession creates one smux session with the constructor selected by role.
func NewSession(conn io.ReadWriteCloser, role SessionRole, cfg *smux.Config) (*smux.Session, error) {
	switch role {
	case ClientRole:
		session, err := smux.Client(conn, cfg)
		if err != nil {
			return nil, fmt.Errorf("smux client: %w", err)
		}
		return session, nil
	case ServerRole:
		session, err := smux.Server(conn, cfg)
		if err != nil {
			return nil, fmt.Errorf("smux server: %w", err)
		}
		return session, nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownSessionRole, role)
	}
}

// HasIsolatedControl reports whether control uses a distinct muxconn and session.
func (p *SessionPair) HasIsolatedControl() bool {
	return p != nil && p.ControlConn != nil
}

// CloseConns closes data then control muxconns so late transport frames are discarded.
func (p *SessionPair) CloseConns() error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.DataConn != nil {
		errs = append(errs, p.DataConn.Close())
	}
	if p.ControlConn != nil {
		errs = append(errs, p.ControlConn.Close())
	}
	return errors.Join(errs...)
}

// Close deterministically closes sessions before their muxconns and returns all errors.
func (p *SessionPair) Close() error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.DataSession != nil {
		errs = append(errs, p.DataSession.Close())
	}
	if p.ControlSession != nil && p.ControlSession != p.DataSession {
		errs = append(errs, p.ControlSession.Close())
	}
	if err := p.CloseConns(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
