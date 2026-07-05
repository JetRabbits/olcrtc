package vp8channel

import (
	"testing"
	"time"
)

func TestNewPaceControllerStartsConservativeAndClamped(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Start rate is capped at the (high) upper bound but seeded from the
	// conservative adaptive start, never above max, never below min.
	c := newPaceController(1_200_000, 30, now)
	if c.currentRate() != adaptiveStartBytesPerSec {
		t.Fatalf("start rate = %d, want %d", c.currentRate(), adaptiveStartBytesPerSec)
	}
	if got, want := c.perTickBytes(), adaptiveStartBytesPerSec/30; got != want {
		t.Fatalf("perTickBytes = %d, want %d", got, want)
	}

	// A small upper bound clamps the start rate down to it.
	c = newPaceController(100_000, 30, now)
	if c.currentRate() != 100_000 {
		t.Fatalf("start rate with small cap = %d, want 100000", c.currentRate())
	}

	// A tiny upper bound below the floor clamps min == max == start.
	c = newPaceController(10_000, 30, now)
	if c.currentRate() != 10_000 {
		t.Fatalf("start rate with tiny cap = %d, want 10000", c.currentRate())
	}
}

func TestPaceControllerWarmupHoldsRate(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)
	before := c.currentRate()

	// srtt <= 0 means no RTT sample yet; the rate must not move.
	if got := c.observe(0, 0, 0, now); got != c.perTickBytes() {
		t.Fatalf("observe(0) perTick = %d, want %d", got, c.perTickBytes())
	}
	if c.currentRate() != before {
		t.Fatalf("warmup changed rate: %d -> %d", before, c.currentRate())
	}
}

func TestPaceControllerProbesUpUnderLowDelay(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)
	start := c.currentRate()

	// srtt == baseline (ratio 1.0 <= probe ratio) probes the rate up additively.
	c.observe(100, 0, 0, now)
	if c.currentRate() != start+adaptiveIncreaseStep {
		t.Fatalf("after 1 probe rate = %d, want %d", c.currentRate(), start+adaptiveIncreaseStep)
	}

	// Repeated low-delay observations keep ramping, capped at max.
	for i := 0; i < 1000; i++ {
		now = now.Add(adaptiveControlInterval)
		c.observe(100, 0, 0, now)
	}
	if c.currentRate() != c.currentRate() || c.rate > c.maxRate {
		t.Fatalf("rate exceeded max: %v > %v", c.rate, c.maxRate)
	}
	if c.currentRate() != 1_200_000 {
		t.Fatalf("rate did not reach cap: %d", c.currentRate())
	}
}

func TestPaceControllerBacksOffUnderBufferbloat(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)

	// Establish a baseline propagation RTT of 100ms and ramp up a bit so there
	// is room to back off. Keep every observation inside a single baseline
	// window so the 100ms baseline stays pinned (the queue still drains to
	// 100ms occasionally — genuine bufferbloat, not a raised propagation RTT).
	c.observe(100, 0, 0, now)
	for i := 0; i < 20; i++ {
		c.observe(100, 0, 0, now)
	}
	high := c.currentRate()

	// srtt balloons to 3x baseline (>= backoff ratio) -> multiplicative decrease.
	c.observe(300, 0, 0, now)
	if c.currentRate() >= high {
		t.Fatalf("bufferbloat did not reduce rate: high=%d now=%d", high, c.currentRate())
	}
	// Sustained bloat (with the baseline still reachable at 100ms) drives the
	// rate toward the floor but never below it.
	for i := 0; i < 1000; i++ {
		c.observe(300, 0, 0, now)
	}
	if c.currentRate() < adaptiveMinBytesPerSec {
		t.Fatalf("rate fell below floor: %d < %d", c.currentRate(), adaptiveMinBytesPerSec)
	}
	if c.currentRate() != adaptiveMinBytesPerSec {
		t.Fatalf("rate did not settle at floor: %d", c.currentRate())
	}
}

func TestPaceControllerDeadBandHoldsRate(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)
	c.observe(100, 0, 0, now) // baseline 100, ratio 1.0 -> probes up once
	held := c.currentRate()

	// ratio between probe (1.2) and backoff (1.5): e.g. 1.35 -> hold.
	now = now.Add(adaptiveControlInterval)
	c.observe(135, 0, 0, now)
	if c.currentRate() != held {
		t.Fatalf("dead-band moved rate: %d -> %d", held, c.currentRate())
	}
}

func TestPaceControllerBaselineAdoptsLowerImmediately(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)

	c.observe(300, 0, 0, now) // first sample sets baseline 300
	if c.baseSrtt != 300 {
		t.Fatalf("baseline = %v, want 300", c.baseSrtt)
	}
	// A lower sample is adopted immediately (propagation delay can only be
	// discovered by seeing a lower RTT).
	now = now.Add(adaptiveControlInterval)
	c.observe(120, 0, 0, now)
	if c.baseSrtt != 120 {
		t.Fatalf("baseline did not adopt lower sample: %v", c.baseSrtt)
	}
}

func TestPaceControllerBaselineRollsForwardOnPersistentIncrease(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)

	c.observe(100, 0, 0, now) // baseline 100

	// A genuinely higher path RTT (200ms) persists. Over up to two baseline
	// windows the windowed-minimum filter must roll the baseline forward so
	// the controller stops treating the new propagation delay as bufferbloat.
	end := now.Add(3 * adaptiveBaselineWindow)
	for now.Before(end) {
		now = now.Add(adaptiveControlInterval)
		c.observe(200, 0, 0, now)
	}
	if c.baseSrtt < 200 {
		t.Fatalf("baseline did not roll forward to persistent RTT: %v", c.baseSrtt)
	}
}

func TestPaceControllerNeverExceedsBounds(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(300_000, 30, now)

	// Alternate extreme low and high delay; rate must stay within [min,max].
	for i := 0; i < 5000; i++ {
		now = now.Add(adaptiveControlInterval)
		if i%2 == 0 {
			c.observe(50, 0, 0, now)
		} else {
			c.observe(2000, 0, 0, now)
		}
		if c.rate > c.maxRate {
			t.Fatalf("rate exceeded max at i=%d: %v > %v", i, c.rate, c.maxRate)
		}
		if c.rate < c.minRate {
			t.Fatalf("rate below min at i=%d: %v < %v", i, c.rate, c.minRate)
		}
	}
}

func TestPaceControllerPerTickFlooredAtEpochHeader(t *testing.T) {
	t.Parallel()
	now := time.Now()
	// Tiny cap so rate/fps would round below the epoch header length.
	c := newPaceController(10_000, 120, now)
	if got := c.perTickBytes(); got < epochHdrLen {
		t.Fatalf("perTickBytes = %d, want >= %d", got, epochHdrLen)
	}
}

func TestPaceControllerBacksOffUnderHighLossEvenAtLowDelay(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)

	// Establish a clean low-loss baseline and ramp up.
	var segs, retr uint64
	for i := 0; i < 30; i++ {
		segs += 200 // no retransmits -> loss 0
		c.observe(100, segs, retr, now)
	}
	high := c.currentRate()

	// srtt stays flat at baseline (delay is blind once the policer saturates),
	// but the retransmit ratio spikes to ~50%. The loss governor must back off.
	c.observe(100, segs+200, retr+100, now)
	if c.currentRate() >= high {
		t.Fatalf("high loss did not reduce rate: high=%d now=%d", high, c.currentRate())
	}
}

func TestPaceControllerDoesNotProbeUpUnderModerateLoss(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)

	var segs, retr uint64
	segs += 200
	c.observe(100, segs, retr, now) // clean baseline, one probe up
	held := c.currentRate()

	// Loss ratio ~10% (above adaptiveLossLow=5%, below adaptiveLossHigh=20%):
	// delay is at baseline but the controller must hold, not probe up.
	segs += 200
	retr += 20
	c.observe(100, segs, retr, now)
	if c.currentRate() != held {
		t.Fatalf("probed up under moderate loss: %d -> %d", held, c.currentRate())
	}
}

func TestPaceControllerHighLossSamplesDoNotPoisonBaseline(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newPaceController(1_200_000, 30, now)

	var segs, retr uint64
	segs += 200
	c.observe(100, segs, retr, now) // clean baseline 100

	// A stalled path: srtt frozen high at 700 with heavy loss, sustained for
	// several baseline windows. The baseline must NOT roll forward to 700
	// (those are congested samples), so the controller keeps treating 700 as
	// bufferbloat and never ramps into the stall.
	end := now.Add(4 * adaptiveBaselineWindow)
	for now.Before(end) {
		now = now.Add(adaptiveControlInterval)
		segs += 200
		retr += 120 // ~60% loss
		c.observe(700, segs, retr, now)
	}
	if c.baseSrtt >= 700 {
		t.Fatalf("baseline poisoned by congested samples: %v", c.baseSrtt)
	}
	if c.currentRate() != adaptiveMinBytesPerSec {
		t.Fatalf("rate did not settle at floor under sustained loss/bloat: %d", c.currentRate())
	}
}

func TestEnvAdaptivePacerEnabled(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"", true},
		{"1", true},
		{"true", true},
		{"on", true},
		{"0", false},
		{"false", false},
		{"off", false},
		{"no", false},
		{"disabled", false},
		{" OFF ", false},
	} {
		t.Setenv("OLCRTC_VP8_ADAPTIVE_PACER", tc.val)
		if got := envAdaptivePacerEnabled(); got != tc.want {
			t.Fatalf("env=%q enabled=%v, want %v", tc.val, got, tc.want)
		}
	}
}
