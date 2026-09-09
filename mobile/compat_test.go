package mobile

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

type compatProtector struct{}

func (compatProtector) Protect(int) bool { return true }

func resetCompatibilitySingleton(t *testing.T) {
	t.Helper()
	singletonMu.Lock()
	singletonRuntime = newRuntime(func(context.Context, client.Config, func(string)) error { return nil })
	singletonStats = client.FlowStats{}
	singletonMu.Unlock()
	t.Cleanup(func() {
		singletonMu.Lock()
		singletonRuntime = New()
		singletonStats = client.FlowStats{}
		singletonMu.Unlock()
	})
}

func TestCompatibilityDefaultsAreSane(t *testing.T) {
	resetCompatibilitySingleton(t)

	singletonRuntime.mu.Lock()
	defer singletonRuntime.mu.Unlock()
	if singletonRuntime.defaults.transport != transportVP8 {
		t.Fatalf("default transport = %q, want %q", singletonRuntime.defaults.transport, transportVP8)
	}
	if singletonRuntime.defaults.socksHost == "" || singletonRuntime.defaults.socksPort == 0 {
		t.Fatalf("SOCKS defaults not configured: host=%q port=%d", singletonRuntime.defaults.socksHost, singletonRuntime.defaults.socksPort)
	}
	if singletonRuntime.defaults.vp8.FPS != defaultVP8FPS || singletonRuntime.defaults.vp8.BatchSize != defaultVP8BatchSize {
		t.Fatalf("VP8 defaults = %+v", singletonRuntime.defaults.vp8)
	}
	if ActiveTCPFlows() != 0 || ActiveUDPFlows() != 0 || ActiveTotalFlows() != 0 {
		t.Fatalf("flow stats should start at zero")
	}
}

func TestCompatibilitySettersStoreValuesWithoutPanic(t *testing.T) {
	resetCompatibilitySingleton(t)

	SetProviders()
	SetProtector(compatProtector{})
	SetDebug(true)
	SetDebug(false)
	SetDNS("1.1.1.1")
	SetSocksListenHost("[127.0.0.1]")
	SetVP8Options(200, 1000)
	SetLowMemoryProfile(false)

	singletonRuntime.mu.Lock()
	defer singletonRuntime.mu.Unlock()
	if singletonRuntime.defaults.dnsServer != "1.1.1.1:53" {
		t.Fatalf("dns = %q", singletonRuntime.defaults.dnsServer)
	}
	if singletonRuntime.defaults.socksHost != "127.0.0.1" {
		t.Fatalf("socks host = %q", singletonRuntime.defaults.socksHost)
	}
	if singletonRuntime.defaults.vp8.FPS != 120 || singletonRuntime.defaults.vp8.BatchSize != 64 {
		t.Fatalf("VP8 clamp = %+v, want FPS 120 batch 64", singletonRuntime.defaults.vp8)
	}
}

func TestStartWithTransportValidatesArguments(t *testing.T) {
	resetCompatibilitySingleton(t)

	err := StartWithTransport("none", "vp8", "room", "client", "not-hex", defaultSOCKSPort, "", "")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("StartWithTransport invalid key error = %v, want ErrInvalidConfig", err)
	}
}

func TestStopIsIdempotentAndReturnsRuntimeError(t *testing.T) {
	resetCompatibilitySingleton(t)
	if err := Stop(); err != nil {
		t.Fatalf("first Stop on idle returned error: %v", err)
	}
	if err := Stop(); err != nil {
		t.Fatalf("second Stop on idle returned error: %v", err)
	}

	want := errors.New("stop failure")
	singletonMu.Lock()
	singletonRuntime = newRuntime(func(ctx context.Context, _ client.Config, _ func(string)) error {
		<-ctx.Done()
		return want
	})
	singletonMu.Unlock()
	validKey := strings.Repeat("00", 32)
	if err := StartWithTransport("none", "vp8", "room", "client", validKey, defaultSOCKSPort, "", ""); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := Stop(); !errors.Is(err, want) {
		t.Fatalf("Stop error = %v, want %v", err, want)
	}
}

func TestConfigureSingletonLeavesChannelIDEmptyForBindingTokenRegression(t *testing.T) {
	resetCompatibilitySingleton(t)

	validKey := strings.Repeat("00", 32)
	if err := configureSingleton("none", "vp8", "https://rooms.example/r/abc", "deadbeef", validKey, defaultSOCKSPort, "", ""); err != nil {
		t.Fatalf("configureSingleton: %v", err)
	}
	singletonRuntime.mu.Lock()
	defer singletonRuntime.mu.Unlock()
	if singletonRuntime.defaults.channelID != "" {
		t.Fatalf("ChannelID = %q, want empty so binding token derives from RoomURL", singletonRuntime.defaults.channelID)
	}
	if singletonRuntime.defaults.deviceID != "deadbeef" {
		t.Fatalf("DeviceID = %q, want client ID", singletonRuntime.defaults.deviceID)
	}
}
