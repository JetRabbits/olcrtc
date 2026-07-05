package vp8channel

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Dataplane stall watchdog.
//
// The adaptive pacer (pacer.go) fixes bufferbloat by controlling our send rate,
// but it cannot recover a media path that has already collapsed BELOW our layer.
// On the Telemost SFU, once the WebRTC bandwidth estimator/policer for a video
// track collapses (after an overdrive burst or sustained loss) it can blackhole
// the track in BOTH directions and not recover within the transfer window:
// KCP `in` freezes, `srtt` freezes, yet both peers keep emitting `out`
// (retransmits) into the void even at the pacer floor. Backing off cannot
// un-stick an SFU allocation that has already gone to zero — only a fresh
// WebRTC renegotiation (carrier reconnect) re-joins the room and re-allocates
// the track. Historically the old aggressive liveness churn healed this by
// accident; the now-tolerant liveness (90s) leaves it stuck.
//
// The watchdog re-introduces that recovery, but ONLY on a genuine stall, so it
// stays tolerant of transient bufferbloat:
//   - the peer must be confirmed (tunnel established),
//   - inbound KCP bytes must be frozen for stallTimeout,
//   - AND we must actually be trying to send (outbound bytes growing or the
//     writer queue non-empty) — a frozen `in` on an idle tunnel is not a stall.
//
// A minimum recovery gap rate-limits triggers so a persistent problem cannot
// turn into a reconnect storm.
const (
	// defaultStallTimeout is how long inbound may stay frozen while sending
	// before the dataplane is declared stalled. Long enough to clear transient
	// bufferbloat/HOL pauses, far shorter than the 90s liveness timeout.
	defaultStallTimeout = 8 * time.Second
	// stallMinRecoveryGap rate-limits recovery reconnects. Sized to comfortably
	// cover a full WebRTC/ICE renegotiation (~10-15s) plus warmup, so we never
	// stack a second recovery on top of one still in flight.
	stallMinRecoveryGap = 25 * time.Second
	// stallJitterMaxMS desynchronises the two ends' stall timers so the server
	// and client do not trigger dueling reconnects on the same stall; whichever
	// fires first re-joins the room and re-allocates both tracks, which resumes
	// the other end's inbound and disarms its detector.
	stallJitterMaxMS = 2000
	// sendIdleGrace is how long outbound progress may pause before we treat the
	// tunnel as genuinely idle (and therefore NOT stalled). This must be
	// generous relative to the control tick: at the pacer floor, KCP emits
	// original-data/keepalive bytes in bursts, so many individual ticks show no
	// outbound growth even though we are actively (and futilely) pushing a stuck
	// path. Without this window, a single idle tick would reset the stall timer
	// every ~500ms and the detector could never accrue its full stallTimeout —
	// which is exactly why real SFU blackholes went undetected (stall_recov:0).
	sendIdleGrace = 3 * time.Second
)

// stallDetector tracks inbound/outbound progress and reports when a recovery
// reconnect should be triggered. Not safe for concurrent use; a single
// goroutine (paceControlLoop) owns it.
type stallDetector struct {
	stallTimeout   time.Duration
	minRecoveryGap time.Duration

	started      bool
	lastInBytes  uint64
	lastOutSegs  uint64
	lastInGrowth time.Time
	lastSending  time.Time
	lastRecovery time.Time
}

func newStallDetector(stallTimeout, minRecoveryGap time.Duration) *stallDetector {
	if stallTimeout <= 0 {
		stallTimeout = defaultStallTimeout
	}
	if minRecoveryGap <= 0 {
		minRecoveryGap = stallMinRecoveryGap
	}
	return &stallDetector{stallTimeout: stallTimeout, minRecoveryGap: minRecoveryGap}
}

// observe folds one sample of the monotonic cumulative KCP inbound-delivered
// bytes plus the outbound *segment* counter (which includes retransmits, so it
// keeps climbing during a stall even when original-data byte growth stalls at
// the pacer floor) and liveness context, and returns true exactly on the tick a
// recovery reconnect should be triggered.
//
// outSegs (not raw outbound bytes) is the "we are trying to send" signal on
// purpose: during a real dataplane stall the app has nothing new to hand down
// (WireGuard is stuck too), so original-data BytesSent barely moves, but KCP
// keeps retransmitting the unacked backlog — OutSegs grows on every tick. Using
// bytes here made the detector reset its own timer each idle tick and never
// fire on genuine SFU blackholes.
func (d *stallDetector) observe(inBytes, outSegs uint64, queueNonEmpty, peerConfirmed bool, now time.Time) bool {
	if !d.started {
		d.started = true
		d.lastInBytes = inBytes
		d.lastOutSegs = outSegs
		d.lastInGrowth = now
		d.lastSending = now
		return false
	}
	inGrew := inBytes > d.lastInBytes
	outGrew := outSegs > d.lastOutSegs
	d.lastInBytes = inBytes
	d.lastOutSegs = outSegs

	// Any outbound progress (new segments/retransmits) or a backed-up writer
	// queue proves we are actively trying to push data. Remember when we last
	// saw that, so a momentary lull between bursts does not look like idle.
	if outGrew || queueNonEmpty {
		d.lastSending = now
	}

	// Not yet established, or making inbound progress: not stalled. Keep the
	// "last progress" stamp fresh so the timer only accrues during a real stall.
	if !peerConfirmed || inGrew {
		d.lastInGrowth = now
		return false
	}
	// Inbound frozen and we have not tried to send anything for a whole
	// sendIdleGrace window: idle tunnel, not a stall.
	if d.lastSending.IsZero() || now.Sub(d.lastSending) >= sendIdleGrace {
		d.lastInGrowth = now
		return false
	}
	// Sending, inbound frozen: accrue toward the stall timeout.
	if now.Sub(d.lastInGrowth) < d.stallTimeout {
		return false
	}
	// Stalled long enough; rate-limit against the previous recovery.
	if !d.lastRecovery.IsZero() && now.Sub(d.lastRecovery) < d.minRecoveryGap {
		return false
	}
	d.lastRecovery = now
	d.lastInGrowth = now // fresh window; give the recovery time to take effect
	return true
}

// envStallRecoveryEnabled reports whether the dataplane stall watchdog is
// active. Default ON; set OLCRTC_VP8_STALL_RECOVERY=0/false/off to disable it
// (operational rollback switch, no rebuild required).
func envStallRecoveryEnabled() bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv("OLCRTC_VP8_STALL_RECOVERY"))) {
	case "0", "false", "off", "no", "disable", "disabled":
		return false
	default:
		return true
	}
}

// envStallTimeout returns the stall timeout override from
// OLCRTC_VP8_STALL_TIMEOUT_MS, or 0 when unset/invalid (use default).
func envStallTimeout() time.Duration {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("OLCRTC_VP8_STALL_TIMEOUT_MS")))
	if err != nil || v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Millisecond
}
