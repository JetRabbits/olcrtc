package mobile

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

type recordingRequester struct {
	mu      sync.Mutex
	reasons []string
	err     error
}

func (r *recordingRequester) RequestReconnect(reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reasons = append(r.reasons, reason)
	return r.err
}

func (r *recordingRequester) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.reasons...)
}

func TestRuntimeReconnectForwarding(t *testing.T) {
	requester := &recordingRequester{}
	runner := func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		if cfg.OnClientReady != nil {
			cfg.OnClientReady(requester)
		}
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	}

	runtime := configuredRuntime(t, runner)
	if err := runtime.Reconnect("network"); !errors.Is(err, ErrReconnectUnavailable) {
		t.Fatalf("idle Reconnect() error = %v, want %v", err, ErrReconnectUnavailable)
	}

	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(500); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}

	if err := runtime.Reconnect("network"); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	if err := runtime.Reconnect(""); err != nil {
		t.Fatalf("Reconnect(\"\") error = %v", err)
	}
	got := requester.snapshot()
	if len(got) != 2 || got[0] != "network" || got[1] != "network" {
		t.Fatalf("forwarded reasons = %v, want [network network]", got)
	}

	if err := runtime.Stop(500); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := runtime.Reconnect("network"); !errors.Is(err, ErrReconnectUnavailable) {
		t.Fatalf("stopped Reconnect() error = %v, want %v", err, ErrReconnectUnavailable)
	}
}

// A client that publishes its reconnecter only while its context is already
// canceled belongs to a superseded generation; attaching it must stay a no-op
// so Reconnect can never target a dead session after Stop.
func TestRuntimeReconnectLateAttachIgnored(t *testing.T) {
	requester := &recordingRequester{}
	runner := func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		if cfg.OnClientReady != nil {
			cfg.OnClientReady(requester)
		}
		return ctx.Err()
	}

	runtime := configuredRuntime(t, runner)
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(500); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	if err := runtime.Stop(500); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := runtime.Reconnect("network"); !errors.Is(err, ErrReconnectUnavailable) {
		t.Fatalf("late-attach Reconnect() error = %v, want %v", err, ErrReconnectUnavailable)
	}
	if got := requester.snapshot(); len(got) != 0 {
		t.Fatalf("late-attached requester received reasons: %v", got)
	}
}

func TestRuntimeReconnectPropagatesRequesterError(t *testing.T) {
	requester := &recordingRequester{err: errTestRun}
	runner := func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		if cfg.OnClientReady != nil {
			cfg.OnClientReady(requester)
		}
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	}

	runtime := configuredRuntime(t, runner)
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(500); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}
	if err := runtime.Reconnect("network"); !errors.Is(err, errTestRun) {
		t.Fatalf("Reconnect() error = %v, want %v", err, errTestRun)
	}
	if err := runtime.Stop(500); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}
