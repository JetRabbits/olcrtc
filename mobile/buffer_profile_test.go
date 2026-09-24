package mobile

import (
	"context"
	"errors"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/limits"
	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

func TestRuntimeSetBufferProfileSelectsSizesOnly(t *testing.T) {
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		return nil
	})

	if err := runtime.SetBufferProfile(BufferProfileMobileLowMemory); err != nil {
		t.Fatalf("SetBufferProfile(mobile) error = %v", err)
	}
	runtime.mu.Lock()
	got := runtime.defaults.resourceProfile
	runtime.mu.Unlock()

	want := limits.MobileLowMemory()
	if got.Smux.DataReceiveBuffer != want.Smux.DataReceiveBuffer {
		t.Fatalf("Smux.DataReceiveBuffer = %d, want %d (mobile profile)",
			got.Smux.DataReceiveBuffer, want.Smux.DataReceiveBuffer)
	}
	if got.Smux.DataReceiveBuffer >= 4*1024*1024 {
		t.Fatalf("Smux.DataReceiveBuffer = %d, want a mobile-sized buffer, not desktop-sized", got.Smux.DataReceiveBuffer)
	}
	if got.KCP.DataSendWindow != want.KCP.DataSendWindow {
		t.Fatalf("KCP.DataSendWindow = %d, want %d", got.KCP.DataSendWindow, want.KCP.DataSendWindow)
	}

	if err := runtime.SetBufferProfile(BufferProfileDefault); err != nil {
		t.Fatalf("SetBufferProfile(default) error = %v", err)
	}
	runtime.mu.Lock()
	got = runtime.defaults.resourceProfile
	runtime.mu.Unlock()
	if got.Smux.DataReceiveBuffer != 0 {
		t.Fatalf("after default, Smux.DataReceiveBuffer = %d, want 0 (Normalize fills Default)", got.Smux.DataReceiveBuffer)
	}
}

func TestRuntimeSetBufferProfileRejectsUnknownName(t *testing.T) {
	runtime := configuredRuntime(t, func(ctx context.Context, cfg client.Config, onReady func(string)) error {
		return nil
	})
	runtime.SetResourceProfile(client.ResourceProfile{Smux: limits.Smux{DataReceiveBuffer: 1234}})

	err := runtime.SetBufferProfile("mobile")
	if !errors.Is(err, ErrInvalidBufferProfile) {
		t.Fatalf("SetBufferProfile(unknown) error = %v, want %v", err, ErrInvalidBufferProfile)
	}

	runtime.mu.Lock()
	got := runtime.defaults.resourceProfile
	runtime.mu.Unlock()
	if got.Smux.DataReceiveBuffer != 1234 {
		t.Fatalf("rejected profile changed buffers to %d, want the previous 1234", got.Smux.DataReceiveBuffer)
	}
}

// TestBufferProfileKeepsHostSockSCeilings guards the exact composition iOS needs:
// mobile-sized transport buffers together with host-chosen flow ceilings. The
// profile's own MaxUDP=8 must not be able to starve tunnelled DNS.
func TestBufferProfileKeepsHostSockSCeilings(t *testing.T) {
	cfg := runtimeConfig{
		resourceProfile: limits.MobileLowMemory(),
		socksFlowLimits: limits.SOCKS{MaxTCP: 48, MaxUDP: 24, MaxTotal: 72},
	}
	out := cfg.clientConfig()

	if out.ResourceProfile.SOCKS.MaxTCP != 48 || out.ResourceProfile.SOCKS.MaxUDP != 24 || out.ResourceProfile.SOCKS.MaxTotal != 72 {
		t.Fatalf("SOCKS ceilings = %+v, want the host override {48 24 72}", out.ResourceProfile.SOCKS)
	}
	if out.ResourceProfile.Smux.DataReceiveBuffer != limits.MobileLowMemory().Smux.DataReceiveBuffer {
		t.Fatalf("Smux.DataReceiveBuffer = %d, want the mobile profile's %d",
			out.ResourceProfile.Smux.DataReceiveBuffer, limits.MobileLowMemory().Smux.DataReceiveBuffer)
	}
}
