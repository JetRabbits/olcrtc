package vp8channel

import (
	"testing"
	"time"
)

// advance feeds a sequence of samples spaced by adaptiveControlInterval and
// returns how many times the detector fired. The second cumulative counter is
// the outbound *segment* count (retransmits included).
func drive(d *stallDetector, start time.Time, ticks int, inStep, outStep uint64, queue, confirmed bool) (fires int, end time.Time) {
	var in, out uint64
	now := start
	for i := 0; i < ticks; i++ {
		in += inStep
		out += outStep
		if d.observe(in, out, queue, confirmed, now) {
			fires++
		}
		now = now.Add(adaptiveControlInterval)
	}
	return fires, now
}

func TestStallDetectorFiresWhenInboundFrozenWhileSending(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	// Healthy: inbound and outbound both growing -> never fires.
	fires, now := drive(d, now, 20, 1000, 1000, false, true)
	if fires != 0 {
		t.Fatalf("healthy traffic fired recovery %d times", fires)
	}

	// Stall: inbound frozen (inStep=0) but we keep sending (outStep>0) -> after
	// stallTimeout (8s = 16 ticks) it must fire exactly once.
	fires, _ = drive(d, now, 30, 0, 1000, false, true)
	if fires != 1 {
		t.Fatalf("stall fired %d times, want exactly 1 within one recovery gap", fires)
	}
}

func TestStallDetectorIgnoresIdleTunnel(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	// Inbound frozen AND not sending (no outbound growth, empty queue): idle,
	// not a stall. Must never fire even over a long span.
	fires, _ := drive(d, now, 200, 0, 0, false, true)
	if fires != 0 {
		t.Fatalf("idle tunnel fired recovery %d times", fires)
	}
}

func TestStallDetectorWaitsForPeerConfirmed(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	// Before the peer is confirmed, a frozen inbound while sending is just the
	// handshake warmup, not a stall.
	fires, _ := drive(d, now, 100, 0, 1000, true, false)
	if fires != 0 {
		t.Fatalf("fired before peer confirmed: %d", fires)
	}
}

func TestStallDetectorQueueNonEmptyCountsAsSending(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	// Inbound frozen, outbound bytes flat, but the writer queue is backed up
	// (queueNonEmpty) -> we are trying to send, so this is a stall.
	fires, _ := drive(d, now, 30, 0, 0, true, true)
	if fires != 1 {
		t.Fatalf("queue-backed stall fired %d times, want 1", fires)
	}
}

func TestStallDetectorRateLimitsRecoveries(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	// Sustained stall over a long span. With stallTimeout 8s and gap 25s, the
	// detector may fire at most once per (gap) window, so over ~100s it should
	// fire only a handful of times, never every tick.
	fires, _ := drive(d, now, 200, 0, 1000, false, true) // 200 ticks = 100s
	if fires < 1 {
		t.Fatalf("sustained stall never fired")
	}
	if fires > 5 {
		t.Fatalf("sustained stall fired too often (%d), rate limit not working", fires)
	}
}

func TestStallDetectorRecoversAfterInboundResumes(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	// Stall, fire once.
	fires, now := drive(d, now, 20, 0, 1000, false, true)
	if fires != 1 {
		t.Fatalf("expected 1 fire during stall, got %d", fires)
	}
	// Inbound resumes (recovery worked): detector must disarm and not fire.
	fires, _ = drive(d, now, 40, 1000, 1000, false, true)
	if fires != 0 {
		t.Fatalf("fired after inbound resumed: %d", fires)
	}
}

// TestStallDetectorFiresWithSparseOutboundGrowth reproduces the production bug
// where a genuine SFU blackhole went undetected (stall_recov:0). At the pacer
// floor the outbound counter only advances in bursts (many control ticks show
// zero growth), so a per-tick "did outbound grow?" reset made the stall timer
// restart every ~500ms and never reach stallTimeout. With the sending-window
// grace, intermittent outbound progress must still be recognised as "sending"
// and the stall must fire.
func TestStallDetectorFiresWithSparseOutboundGrowth(t *testing.T) {
	t.Parallel()
	d := newStallDetector(8*time.Second, 25*time.Second)
	now := time.Now()

	var in, out uint64
	fires := 0
	// 60 ticks = 30s. Inbound frozen. Outbound advances only every 4th tick
	// (every 2s < sendIdleGrace), queue empty — exactly the floored-pacer /
	// retransmit-burst pattern observed on the wire.
	for i := 0; i < 60; i++ {
		if i%4 == 0 {
			out += 1000
		}
		if d.observe(in, out, false, true, now) {
			fires++
		}
		now = now.Add(adaptiveControlInterval)
	}
	if fires < 1 {
		t.Fatalf("sparse-outbound stall never fired (regression: stall_recov stuck at 0)")
	}
}

func TestEnvStallRecoveryEnabled(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"", true}, {"1", true}, {"true", true}, {"on", true},
		{"0", false}, {"false", false}, {"off", false}, {"disabled", false},
	} {
		t.Setenv("OLCRTC_VP8_STALL_RECOVERY", tc.val)
		if got := envStallRecoveryEnabled(); got != tc.want {
			t.Fatalf("env=%q enabled=%v want %v", tc.val, got, tc.want)
		}
	}
}

func TestEnvStallTimeout(t *testing.T) {
	t.Setenv("OLCRTC_VP8_STALL_TIMEOUT_MS", "5000")
	if got := envStallTimeout(); got != 5*time.Second {
		t.Fatalf("envStallTimeout = %v, want 5s", got)
	}
	t.Setenv("OLCRTC_VP8_STALL_TIMEOUT_MS", "bogus")
	if got := envStallTimeout(); got != 0 {
		t.Fatalf("invalid envStallTimeout = %v, want 0", got)
	}
}
