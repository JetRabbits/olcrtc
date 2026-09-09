package client

import (
	"context"
)

// ReconnectRequester is the narrowed capability handed to Config.OnClientReady
// consumers; *Client implements it.
type ReconnectRequester interface {
	RequestReconnect(reason string) error
}

// ConfigClientReadyFunc publishes a live ReconnectRequester to host code.
type ConfigClientReadyFunc func(ReconnectRequester)

// RequestReconnect asks the running client to rebuild its remote carrier and
// smux session without stopping the local SOCKS5 listener. It is the fast
// host-driven transport handover path (for example Wi-Fi -> cellular on
// Android). Repeated requests while one is already pending are coalesced:
// the first request already observes the current network state.
func (c *Client) RequestReconnect(reason string) error {
	if reason == "" {
		reason = reconnectNetwork
	}

	c.requestMu.Lock()
	request := c.requestReconnect
	c.requestMu.Unlock()
	if request == nil {
		return ErrClientStopped
	}
	return request(reason)
}

// startReconnectRequestLoop installs the request closure for the lifetime of
// one bringUpLink generation, mirroring the control-loop lifecycle.
func (c *Client) startReconnectRequestLoop(
	ctx context.Context,
	cfg Config,
	cancel context.CancelFunc,
) {
	watchCtx, stop := context.WithCancel(ctx)

	c.requestMu.Lock()
	c.stopRequestReconnect = stop
	c.requestReconnect = func(reason string) error {
		if watchCtx.Err() != nil {
			return ErrClientStopped
		}
		select {
		case c.reconnectRequestPending <- reason:
		default:
			// One reconnect at a time is enough. Network handovers often
			// arrive as a burst of callbacks; the queued request already
			// sees the current network state.
		}
		return nil
	}
	c.requestMu.Unlock()

	c.goTracked(func() {
		defer stop()
		for {
			select {
			case <-watchCtx.Done():
				c.clearReconnectRequest()
				return
			case reason := <-c.reconnectRequestPending:
				c.dispatchReconnectRequest(ctx, cfg, cancel, reason)
			}
		}
	})
}

// dispatchReconnectRequest routes a queued request to handleReconnect. The
// requestDispatch field exists so tests can observe request routing without
// driving a real transport; production code never sets it.
func (c *Client) dispatchReconnectRequest(
	ctx context.Context,
	cfg Config,
	cancel context.CancelFunc,
	reason string,
) {
	if c.requestDispatch != nil {
		c.requestDispatch(ctx, cfg, cancel, reason)
		return
	}
	c.handleReconnect(ctx, cfg, cancel, reason)
}

func (c *Client) clearReconnectRequest() {
	c.requestMu.Lock()
	c.requestReconnect = nil
	c.requestMu.Unlock()
}

// stopReconnectRequests detaches the public request path first so callers
// racing shutdown observe ErrClientStopped instead of a dead channel.
func (c *Client) stopReconnectRequests() {
	c.requestMu.Lock()
	stop := c.stopRequestReconnect
	c.stopRequestReconnect = nil
	c.requestReconnect = nil
	c.requestMu.Unlock()
	if stop != nil {
		stop()
	}
}
