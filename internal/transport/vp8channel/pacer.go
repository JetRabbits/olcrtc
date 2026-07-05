package vp8channel

import (
	"os"
	"strings"
	"time"
)

// Adaptive, delay-based pacing for the VP8 wire rate.
//
// Why delay-based (Vegas/BBR-style), not loss-based or fixed:
//
// The Telemost SFU + real mobile radio path is congestion/bufferbloat-limited,
// not random-loss-limited. A fixed pacer (perTickBytes = MaxBytesPerSec/fps)
// keeps the wire full at a rate the path may not sustain: KCP (nc=1) plus the
// pacer overdrive the bottleneck, its buffers fill, srtt balloons (observed
// 314ms -> ~1s), the send window stalls waiting for ACKs, and the transfer
// collapses into an RTO retransmit stall. Adding packets (loss-based recovery
// or FEC) only makes a policed/queue-limited path worse (see Exp #68).
//
// The controller instead watches KCP's smoothed RTT. Queueing delay above the
// path's baseline (propagation) RTT is the earliest, cleanest congestion
// signal. When srtt rises above baseline*backoffRatio it multiplicatively
// decreases the rate to drain the queue; when srtt sits near baseline it
// additively probes the rate back up. This is AIMD on delay.
//
// The same vp8channel transport runs on both the OLCRTC server (downlink) and
// the Android client (uplink), so this controller is symmetric by
// construction: each end independently paces its own send direction to the
// delay it observes. The client uplink ACK path (which gates the server's
// download window) is therefore controlled without any client-specific tuning.
//
// MaxBytesPerSec (opts / OLCRTC_VP8_MAX_BYTES_PER_SEC / default) remains the
// hard upper bound; the controller only ever paces at or below it.
const (
	// adaptiveStartBytesPerSec is the initial send rate. Deliberately
	// conservative: starting near the (high) upper bound would overshoot and
	// bloat the path before the first RTT sample lands, exactly the collapse
	// we are avoiding. Probe up from here instead.
	adaptiveStartBytesPerSec = 250_000
	// adaptiveMinBytesPerSec floors the rate so control PONGs / keepalives and
	// slow recovery always have wire, even under sustained congestion.
	adaptiveMinBytesPerSec = 48_000
	// adaptiveBackoffRatio: srtt >= ratio*baseline means the bottleneck queue
	// is building (bufferbloat) -> back off.
	adaptiveBackoffRatio = 1.5
	// adaptiveProbeRatio: srtt <= ratio*baseline means there is headroom ->
	// probe the rate up. The gap between probe and backoff ratios is a dead
	// band that keeps the controller from oscillating around the operating
	// point.
	adaptiveProbeRatio = 1.2
	// adaptiveDecreaseFactor is the multiplicative decrease applied per control
	// tick while bloated (MD of AIMD).
	adaptiveDecreaseFactor = 0.80
	// adaptiveIncreaseStep is the additive increase per control tick while
	// there is headroom (AI of AIMD), in bytes/sec.
	adaptiveIncreaseStep = 24_000
	// adaptiveControlInterval is how often the controller re-evaluates srtt.
	adaptiveControlInterval = 500 * time.Millisecond
	// adaptiveBaselineWindow bounds how long a min-RTT sample is trusted as the
	// baseline. Every window the running minimum is rolled forward so a
	// genuinely higher path RTT is eventually accepted instead of pinning to a
	// stale warmup low (BBR min-RTT windowed-minimum filter).
	adaptiveBaselineWindow = 10 * time.Second
	// adaptiveLossHigh: when the per-interval retransmit ratio reaches this,
	// the bottleneck is saturated and dropping — back off regardless of srtt.
	// Once a policer/queue saturates, srtt stops rising (it can even freeze on
	// a stall) so delay alone becomes blind; loss is then the live congestion
	// signal. Without this the controller mistook a stalled-high-but-flat srtt
	// for "stable low delay" and ramped straight into the collapse.
	adaptiveLossHigh = 0.20
	// adaptiveLossLow: only probe the rate up, and only learn the RTT baseline,
	// when loss is at or below this. A congested sample (high loss, inflated
	// srtt) must never be adopted as the propagation-delay baseline.
	adaptiveLossLow = 0.05
)

// paceController implements the delay-based AIMD rate controller described
// above, augmented with a loss-rate governor for the saturated regime. It is
// not safe for concurrent use; a single goroutine (paceControlLoop) owns it and
// publishes the result via an atomic.
type paceController struct {
	fps     int
	minRate float64
	maxRate float64
	rate    float64

	// baseSrtt is the windowed-minimum smoothed RTT in milliseconds, an
	// estimate of the path's queue-free (propagation) latency. Only low-loss
	// samples feed it.
	baseSrtt  float64
	windowMin float64
	baseStamp time.Time

	// prevSegsOut/prevRetrans hold the previous cumulative KCP counters so the
	// controller can derive a per-interval retransmit ratio.
	prevSegsOut uint64
	prevRetrans uint64
	havePrev    bool
}

func newPaceController(maxBytesPerSec, fps int, now time.Time) *paceController {
	if fps <= 0 {
		fps = defaultFPS
	}
	maxRate := float64(maxBytesPerSec)
	if maxRate <= 0 {
		maxRate = float64(defaultMaxBytesPerSec)
	}
	minRate := float64(adaptiveMinBytesPerSec)
	if minRate > maxRate {
		minRate = maxRate
	}
	start := float64(adaptiveStartBytesPerSec)
	if start > maxRate {
		start = maxRate
	}
	if start < minRate {
		start = minRate
	}
	return &paceController{
		fps:       fps,
		minRate:   minRate,
		maxRate:   maxRate,
		rate:      start,
		baseStamp: now,
	}
}

// observe folds one control sample into the controller and returns the
// resulting per-frame-tick byte budget:
//   - srttMS: KCP smoothed RTT (ms); <= 0 means no sample yet (warmup / just
//     restarted) and the rate is held.
//   - segsOut/retrans: cumulative KCP output/retransmit segment counters, used
//     to derive the per-interval loss ratio.
func (c *paceController) observe(srttMS int32, segsOut, retrans uint64, now time.Time) int {
	// Per-interval retransmit ratio from the cumulative counter deltas.
	lossRatio := 0.0
	if c.havePrev {
		dOut := int64(segsOut) - int64(c.prevSegsOut)
		dRe := int64(retrans) - int64(c.prevRetrans)
		if dOut > 0 && dRe > 0 {
			lossRatio = float64(dRe) / float64(dOut)
			if lossRatio > 1 {
				lossRatio = 1
			}
		}
	}
	c.prevSegsOut = segsOut
	c.prevRetrans = retrans
	c.havePrev = true

	if srttMS <= 0 {
		return c.perTickBytes()
	}
	s := float64(srttMS)
	lowLoss := lossRatio <= adaptiveLossLow

	// Sliding windowed-minimum RTT (baseline = propagation delay), learned ONLY
	// from low-loss samples. Adopt any lower clean sample immediately; every
	// window, roll the running minimum forward so a persistently higher (but
	// uncongested) path RTT is eventually accepted. Gating on low loss is what
	// stops a stalled/congested high srtt from poisoning the baseline and
	// tricking the controller into ramping into the collapse.
	if lowLoss {
		if c.baseSrtt == 0 || s < c.baseSrtt {
			c.baseSrtt = s
		}
		if c.windowMin == 0 || s < c.windowMin {
			c.windowMin = s
		}
		if now.Sub(c.baseStamp) >= adaptiveBaselineWindow {
			c.baseSrtt = c.windowMin
			c.windowMin = s
			c.baseStamp = now
		}
	}
	if c.baseSrtt == 0 {
		// No clean baseline established yet: hold rather than react to a
		// possibly-congested first sample.
		return c.perTickBytes()
	}

	ratio := s / c.baseSrtt
	switch {
	case lossRatio >= adaptiveLossHigh || ratio >= adaptiveBackoffRatio:
		// Saturated (dropping) or queue building: multiplicative decrease.
		c.rate *= adaptiveDecreaseFactor
	case lowLoss && ratio <= adaptiveProbeRatio:
		// Delay near baseline and loss low: additive increase (probe up).
		c.rate += adaptiveIncreaseStep
	}
	if c.rate > c.maxRate {
		c.rate = c.maxRate
	}
	if c.rate < c.minRate {
		c.rate = c.minRate
	}
	return c.perTickBytes()
}

// currentRate returns the controller's current effective byte-rate, for
// diagnostics.
func (c *paceController) currentRate() int {
	return int(c.rate)
}

func (c *paceController) perTickBytes() int {
	v := int(c.rate) / c.fps
	if v < epochHdrLen {
		v = epochHdrLen
	}
	return v
}

// envAdaptivePacerEnabled reports whether the adaptive (delay-based) pacer is
// active. Default ON; set OLCRTC_VP8_ADAPTIVE_PACER=0/false/off to fall back to
// the historical fixed pacer (an operational rollback switch that needs no
// rebuild).
func envAdaptivePacerEnabled() bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv("OLCRTC_VP8_ADAPTIVE_PACER"))) {
	case "0", "false", "off", "no", "disable", "disabled":
		return false
	default:
		return true
	}
}
