// Package vp8channel provides byte transport over VP8 video frames using KCP.
package vp8channel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	kcp "github.com/xtaci/kcp-go/v5"
)

// Both peers establish a KCP session with the same convid. KCP does not
// require a handshake - packets are matched by conv field, so a static
// constant gives us a symmetrical P2P setup.
const kcpConvID = 0xC0FFEE01

// KCP tuning targets a lossy, bursty carrier (VP8 over an SFU). The defaults
// are TCP-like and recover slowly after burst losses.
const (
	// kcp-go hardcodes mtuLimit=1500, so SetMtu() above this is silently
	// clamped. Stay below that with headroom for KCP overhead (24 bytes).
	kcpMTU = 1400

	// Send/receive window in segments, sized to the bandwidth-delay product
	// of the policed video path (~1.2 MB/s wire cap, ~100ms RTT), NOT to
	// "as much as possible". A large send window let the upper layer dump
	// megabytes into KCP instantly; with the wire paced to ~1.2 MB/s those
	// segments then sat queued for SECONDS, so KCP's RTO fired and triggered
	// a retransmit storm while control-plane pongs starved behind the same
	// queue (-> missed pongs -> reconnect). A small send window bounds
	// in-flight data to ~BDP (1.2MB/s * 0.1s / 1400B ≈ 86), keeping queuing
	// latency low. The receive window stays generous so the peer is never
	// the bottleneck.
	kcpSndWnd = 450
	kcpRcvWnd = 1024

	// Reed-Solomon FEC — DISABLED (kept documented after Exp #68).
	//
	// FEC was trialled (RS(13,10), 30% parity overhead) to recover lost
	// packets without an RTO round-trip. It measurably improved KCP-level
	// recovery (fast-retransmit began firing, RetransSegs -85%, RepeatSegs
	// -95%) BUT regressed the end-to-end transfer: the extra 30% packet volume
	// overloaded the capacity/policing-limited VP8/Telemost path, and even the
	// initial public-IP request stopped completing. FEC helps random-loss
	// links; this path is congestion/bufferbloat-limited, where adding packets
	// is counterproductive. The correct lever is rate control (pacer) + RTO
	// tuning, not FEC. Left at 0/0. Both peers must match, so do not enable one
	// side only.
	kcpFECDataShards   = 0
	kcpFECParityShards = 0

	// Length prefix for our message framing on top of KCP stream mode.
	// We use stream mode because UDPSession.Write fragments messages > MSS
	// outside of kcp.Send, which destroys the frg field that message mode
	// relies on for boundary preservation. Adding our own length-prefix
	// framing sidesteps that bug entirely.
	kcpLenPrefix = 4

	// Hard cap on a single message. Anything larger would require an
	// unbounded reassembly buffer on the receiver and is almost certainly
	// a protocol error upstream.
	kcpMaxMessage = 8 * 1024 * 1024
)

// ErrKCPMessageTooLarge is returned by send when the message exceeds
// kcpMaxMessage.
var ErrKCPMessageTooLarge = errors.New("vp8channel: kcp message exceeds maximum size")

// kcpRuntime owns the KCP session and the goroutine that pumps reassembled
// messages from KCP up to cfg.OnData.
type kcpRuntime struct {
	conn      *kcpConn
	sess      *kcp.UDPSession
	readDone  chan struct{}
	writeMu   sync.Mutex // serializes length-prefix + payload writes
	closeOnce sync.Once
	// deliveredBytes counts application payload bytes actually delivered to
	// onData (real in-order forward progress). Unlike kcp.DefaultSnmp.
	// BytesReceived — which counts raw input including duplicate/retransmit
	// segments and therefore keeps ticking up even when a path has stalled and
	// only dups arrive — this only advances when the tunnel makes genuine
	// inbound progress. The stall watchdog needs exactly that distinction.
	deliveredBytes atomic.Uint64
}

func startKCP(out chan<- []byte, onData func([]byte), epochHdr [epochHdrLen]byte, frameInterval time.Duration) (*kcpRuntime, error) {
	c := newKCPConn(out, inboundQueueSize, epochHdr)

	sess, err := kcp.NewConn3(kcpConvID, fakeUDPAddr(), nil, kcpFECDataShards, kcpFECParityShards, c)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("kcp new conn: %w", err)
	}

	// nodelay=1, interval=frameInterval, fast resend=2, congestion control OFF (nc=1).
	// KCP does NOT regulate the send rate here - the writerLoop byte pacer
	// does, fed at a fixed rate just under the carrier's policer knee. KCP's
	// own loss-based congestion control is the wrong controller for a hard
	// policer: with nc=0 the unavoidable ~4% drops collapsed cwnd and starved
	// the wire to ~45 KiB/s. With nc=1 KCP just keeps the BDP-sized window
	// full and retransmits the few losses; the pacer caps the rate so we
	// never overdrive the policer into its collapse zone.
	// Interval matches frameInterval so KCP flushes at the same cadence the
	// VP8 writer emits frames, avoiding queue buildup between KCP and pacer.
	kcpIntervalMs := int(frameInterval.Milliseconds())
	if kcpIntervalMs < 10 {
		kcpIntervalMs = 10
	}
	sess.SetNoDelay(1, kcpIntervalMs, 2, 1)
	sess.SetWindowSize(kcpSndWnd, kcpRcvWnd)
	sess.SetMtu(kcpMTU)
	// Upstream marked SetStreamMode deprecated without providing a replacement;
	// stream framing is still required for our wire format.
	sess.SetStreamMode(true) //nolint:staticcheck // SA1019: no replacement upstream.
	sess.SetACKNoDelay(true)
	sess.SetWriteDelay(false)

	rt := &kcpRuntime{
		conn:     c,
		sess:     sess,
		readDone: make(chan struct{}),
	}

	go rt.readLoop(onData)

	return rt, nil
}

func (r *kcpRuntime) readLoop(onData func([]byte)) {
	defer close(r.readDone)

	var hdr [kcpLenPrefix]byte
	for {
		if _, err := io.ReadFull(r.sess, hdr[:]); err != nil {
			return
		}
		size := binary.BigEndian.Uint32(hdr[:])
		if size == 0 {
			continue
		}
		if size > kcpMaxMessage {
			// Stream framing is now corrupted - there is no safe way to
			// resync without a session reset. Bail and let the upper layer
			// reconnect.
			return
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r.sess, payload); err != nil {
			return
		}
		if onData != nil {
			r.deliveredBytes.Add(uint64(len(payload)))
			onData(payload)
		}
	}
}

// deliver hands a wire payload (already reassembled out of VP8 RTP) to KCP.
func (r *kcpRuntime) deliver(payload []byte) {
	r.conn.deliver(payload)
}

// send queues an application message for reliable delivery. The length
// prefix + payload pair is written under a mutex so that interleaved
// concurrent senders cannot tear the framing.
func (r *kcpRuntime) send(msg []byte) error {
	if len(msg) > kcpMaxMessage {
		return ErrKCPMessageTooLarge
	}
	var hdr [kcpLenPrefix]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(msg))) //nolint:gosec,lll // G115: bounded conversion verified by surrounding logic

	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	// Per-message send tracing is Debug-only: it fires on the data hot path
	// (once per WireGuard/tunnel packet). On mobile the process logger writes
	// to a file on flash storage under a global mutex, so keeping this at Info
	// serialized every packet behind two synchronous writes and throttled
	// throughput to a trickle. Aggregate KCP counters are reported every few
	// seconds by logDiagnostics instead.
	logger.Debugf("vp8channel: KCP.send: queuing %d bytes", len(msg))
	if _, err := r.sess.Write(hdr[:]); err != nil {
		return fmt.Errorf("kcp write header: %w", err)
	}
	if _, err := r.sess.Write(msg); err != nil {
		return fmt.Errorf("kcp write payload: %w", err)
	}
	logger.Debugf("vp8channel: KCP.send: flushed %d bytes to UDPSession", len(msg))
	return nil
}

func (r *kcpRuntime) close() {
	r.closeOnce.Do(func() {
		_ = r.sess.Close()
		_ = r.conn.Close()
	})
}

// srtt returns KCP's smoothed round-trip time in milliseconds, the signal the
// delay-based pacer uses to detect bufferbloat. Returns 0 before the first RTT
// sample (no ACK yet).
func (r *kcpRuntime) srtt() int32 {
	return r.sess.GetSRTT()
}

// paceSampleCounters returns the counters the pacer and stall watchdog react
// to. inBytes is application-delivered payload (real inbound progress, per this
// runtime — resets on a KCP restart, which is fine: the watchdog treats a reset
// as "no progress"). outBytes/outSegs/retransSegs are process-global
// (kcp.DefaultSnmp) send/loss counters; only deltas are used.
func (r *kcpRuntime) paceSampleCounters() (inBytes, outBytes, outSegs, retransSegs uint64) {
	snmp := kcp.DefaultSnmp.Copy()
	return r.deliveredBytes.Load(), snmp.BytesSent, snmp.OutSegs, snmp.RetransSegs
}

type kcpRuntimeStats struct {
	SRTTMS int32
	RTOMS  uint32
	Conn   kcpConnStats

	KCPBytesSent     uint64
	KCPBytesReceived uint64
	KCPInSegs        uint64
	KCPOutSegs       uint64
	KCPRetransSegs   uint64
	KCPFastRetrans   uint64
	KCPEarlyRetrans  uint64
	KCPLostSegs      uint64
	KCPRepeatSegs    uint64
	KCPSndQueue      uint64
	KCPSndBuffer     uint64
	KCPRcvQueue      uint64
}

func (r *kcpRuntime) stats() kcpRuntimeStats {
	snmp := kcp.DefaultSnmp.Copy()
	return kcpRuntimeStats{
		SRTTMS:           r.sess.GetSRTT(),
		RTOMS:            r.sess.GetRTO(),
		Conn:             r.conn.stats(),
		KCPBytesSent:     snmp.BytesSent,
		KCPBytesReceived: snmp.BytesReceived,
		KCPInSegs:        snmp.InSegs,
		KCPOutSegs:       snmp.OutSegs,
		KCPRetransSegs:   snmp.RetransSegs,
		KCPFastRetrans:   snmp.FastRetransSegs,
		KCPEarlyRetrans:  snmp.EarlyRetransSegs,
		KCPLostSegs:      snmp.LostSegs,
		KCPRepeatSegs:    snmp.RepeatSegs,
		KCPSndQueue:      snmp.RingBufferSndQueue,
		KCPSndBuffer:     snmp.RingBufferSndBuffer,
		KCPRcvQueue:      snmp.RingBufferRcvQueue,
	}
}
