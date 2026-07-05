package vp8channel

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

const (
	defaultFPS       = 30
	defaultBatchSize = 64
	// defaultMaxBytesPerSec paces the wire byte-rate just under the Telemost
	// SFU's measured per-slot policer knee (~1.4 MiB/s). Above it the SFU
	// drops bursts wholesale, collapsing goodput and starving keepalives;
	// staying under keeps loss near zero. See TestRealRawVP8Throughput.
	defaultMaxBytesPerSec = 1_200_000
)

// Options tunes the vp8channel transport. Zero values fall back to documented defaults.
type Options struct {
	FPS       int
	BatchSize int
	// MaxBytesPerSec caps the wire byte-rate fed to the video track. Zero
	// falls back to defaultMaxBytesPerSec.
	MaxBytesPerSec int
	// ReconnectOnNewParticipant causes the goolom session to reconnect when
	// a new participant with video joins. Used by the OLCRTC server to ensure
	// Telemost provides fresh SDP exchanges with proper MID binding.
	ReconnectOnNewParticipant bool
}

// TransportOptions marks Options as belonging to the transport options family.
func (Options) TransportOptions() {}

func optionsFrom(cfg transport.Config) (Options, error) {
	if cfg.Options == nil {
		return Options{}, nil
	}
	opts, ok := cfg.Options.(Options)
	if !ok {
		return Options{}, fmt.Errorf("%w: vp8channel: got %T", transport.ErrOptionsTypeMismatch, cfg.Options)
	}
	return opts, nil
}

// envMaxBytesPerSec returns the wire byte-rate cap configured via the
// OLCRTC_VP8_MAX_BYTES_PER_SEC environment variable, or 0 when unset/invalid.
//
// This is an operational tuning knob for the VP8/KCP pacer. On a real mobile
// carrier the fixed 1.2 MiB/s default can overdrive the path: KCP (nc=1) plus
// the pacer keep the wire full faster than the radio/SFU can drain, srtt
// balloons (observed 314ms -> ~1s) and the path collapses into an RTO
// retransmit stall. Lowering the cap to match the sustainable path rate avoids
// the bufferbloat. Kept as an env var so it can be tuned per deployment
// (kubectl set env) without a rebuild; unset preserves the historical default.
func envMaxBytesPerSec() int {
	raw := strings.TrimSpace(os.Getenv("OLCRTC_VP8_MAX_BYTES_PER_SEC"))
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}
