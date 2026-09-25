package mobile

import (
	"context"
	"sync"
	"testing"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

type recordingFlowController struct {
	mu       sync.Mutex
	ceilings [][3]int
}

func (c *recordingFlowController) SetSocksFlowCeilings(maxTCP, maxUDP, maxTotal int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ceilings = append(c.ceilings, [3]int{maxTCP, maxUDP, maxTotal})
}

func (c *recordingFlowController) snapshot() [][3]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][3]int(nil), c.ceilings...)
}

func TestRuntimeSetSocksFlowCeilingsFollowsSessionLifecycle(t *testing.T) {
	controller := &recordingFlowController{}
	runner := func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		if cfg.OnClientReady != nil {
			cfg.OnClientReady(&recordingRequester{})
		}
		if cfg.OnSocksFlowControl != nil {
			cfg.OnSocksFlowControl(controller)
		}
		onReady("127.0.0.1:1080")
		<-ctx.Done()
		return ctx.Err()
	}

	runtime := configuredRuntime(t, runner)
	// Idle: the request must be reported as a no-op, never silently dropped.
	if runtime.SetSocksFlowCeilings(48, 24, 72) {
		t.Fatal("idle SetSocksFlowCeilings() = true, want false")
	}
	if len(controller.snapshot()) != 0 {
		t.Fatalf("idle ceilings applied: %v", controller.snapshot())
	}

	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.WaitReady(500); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}

	if !runtime.SetSocksFlowCeilings(48, 24, 72) {
		t.Fatal("running SetSocksFlowCeilings() = false, want true")
	}
	if !runtime.SetSocksFlowCeilings(8, 0, 0) {
		t.Fatal("running SetSocksFlowCeilings() shed = false, want true")
	}
	got := controller.snapshot()
	if len(got) != 2 || got[0] != [3]int{48, 24, 72} || got[1] != [3]int{8, 0, 0} {
		t.Fatalf("forwarded ceilings = %v, want [[48 24 72] [8 0 0]]", got)
	}

	if err := runtime.Stop(500); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	// A stopped generation must not accept ceiling writes: the capability is
	// cleared with the generation so a stale client cannot be retuned.
	if runtime.SetSocksFlowCeilings(64, 32, 96) {
		t.Fatal("stopped SetSocksFlowCeilings() = true, want false")
	}
	if len(controller.snapshot()) != 2 {
		t.Fatalf("ceilings applied after stop: %v", controller.snapshot())
	}
}
