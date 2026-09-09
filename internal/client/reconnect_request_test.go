package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/runtime"
)

func newReconnectTestClient() *Client {
	return &Client{
		health:                  runtime.NewHealthTracker(nil),
		reconnectRequestPending: make(chan string, 1),
	}
}

func waitForReconnectRequestClosed(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.requestMu.Lock()
		request := c.requestReconnect
		c.requestMu.Unlock()
		if request == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("reconnect request path still installed after context cancellation")
}

func TestRequestReconnectRejectedBeforeStart(t *testing.T) {
	c := newReconnectTestClient()
	if err := c.RequestReconnect("network"); !errors.Is(err, ErrClientStopped) {
		t.Fatalf("RequestReconnect() error = %v, want %v", err, ErrClientStopped)
	}
}

func TestRequestReconnectCoalescesAndDefaultsReason(t *testing.T) {
	c := newReconnectTestClient()
	got := make(chan string, 8)
	c.requestDispatch = func(_ context.Context, _ Config, _ context.CancelFunc, reason string) {
		got <- reason
		// Hold the consumer so burst requests hit the coalescing branch.
		time.Sleep(50 * time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.startReconnectRequestLoop(ctx, Config{}, cancel)

	if err := c.RequestReconnect(""); err != nil {
		t.Fatalf("RequestReconnect() error = %v", err)
	}
	for range 5 {
		if err := c.RequestReconnect("network-change"); err != nil {
			t.Fatalf("RequestReconnect() error = %v", err)
		}
	}

	if first := <-got; first != reconnectNetwork {
		t.Fatalf("first dispatched reason = %q, want %q", first, reconnectNetwork)
	}
	// The burst collapses to at most one follow-up dispatch: the queue slot
	// is size one and every rejected send is a no-op.
	second := 0
	timeout := time.After(200 * time.Millisecond)
	for done := false; !done; {
		select {
		case reason := <-got:
			if reason != "network-change" {
				t.Fatalf("follow-up dispatched reason = %q", reason)
			}
			second++
			if second > 1 {
				t.Fatalf("burst must coalesce to at most one follow-up, got %d", second)
			}
		case <-timeout:
			done = true
		}
	}

	cancel()
	waitForReconnectRequestClosed(t, c)
	if err := c.RequestReconnect("network"); !errors.Is(err, ErrClientStopped) {
		t.Fatalf("after cancel: RequestReconnect() error = %v, want %v", err, ErrClientStopped)
	}
}

func TestStopReconnectRequestsDetachesPublicPath(t *testing.T) {
	c := newReconnectTestClient()
	c.requestDispatch = func(context.Context, Config, context.CancelFunc, string) {}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.startReconnectRequestLoop(ctx, Config{}, cancel)

	c.stopReconnectRequests()

	if err := c.RequestReconnect("network"); !errors.Is(err, ErrClientStopped) {
		t.Fatalf("RequestReconnect() error = %v, want %v", err, ErrClientStopped)
	}
}
