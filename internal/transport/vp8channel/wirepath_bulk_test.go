package vp8channel

// In-process simulation of the real vp8channel wire path, added while hunting
// the bulk-download corruption/starvation seen through the tunnel (TLS
// "bad decrypt", Speedtest failures) on a live Telemost SFU.
//
// chaos_test.go pumps raw KCP packets straight into kcpRuntime.deliver(), so it
// never exercises the layer that actually touches the SFU: the writer packing
// KCP packets into VP8 samples, pion fragmenting a sample into MTU-sized RTP
// packets, and the receiver reassembling them. That seam is where a single
// dropped RTP packet can take down everything the sample carried.
//
// Every simulated frame carries a tag with its index, so a splice or a shift of
// two frames is detectable at byte level.

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

const (
	simMTU       = 1200
	simFrameBody = 1000 // KCP payload bytes per simulated outbound KCP packet
	simTotal     = 2000 // frames per run (~2 MB of payload)
)

func simFrameTag(i int) []byte { return []byte(fmt.Sprintf("<#%06d>", i)) }

// buildSimFrames lays frames out as KCP output: epoch header + payload.
func buildSimFrames(n int) [][]byte {
	hdr := testEpochHdr(1)
	frames := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		f := make([]byte, 0, epochHdrLen+simFrameBody)
		f = append(f, hdr[:]...)
		f = append(f, simFrameTag(i)...)
		for len(f) < epochHdrLen+simFrameBody {
			f = append(f, byte(len(f)%251))
		}
		frames = append(frames, f)
	}
	return frames
}

// writerMode selects how the sender puts outbound KCP packets onto the track.
type writerMode int

const (
	// modeCoalesced packs up to batchSize KCP packets into one VP8 sample while
	// the sample still fits a single RTP packet (singleSampleLimit).
	modeCoalesced writerMode = iota
	// modePerPacket writes one sample per KCP packet, several samples per tick.
	// It carries the same bytes per tick; kept as the reference shape this
	// harness measures the batched writer against.
	modePerPacket
)

type simReceiver struct {
	state     vp8FrameState
	reorder   *reorderBuffer
	delivered bytes.Buffer
	frames    int
	samples   int
	maxSample int
}

func (r *simReceiver) accept(pkt *rtp.Packet) {
	r.reorder.push(pkt, func(ordered *rtp.Packet) {
		frame := r.state.processRTPPacket(ordered)
		if frame == nil {
			return
		}
		if len(frame) < epochHdrLen {
			return
		}
		r.frames++
		// Mirror handleIncomingFrame -> deliverKCPPayload: split the batch and
		// hand each embedded packet to KCP. Concatenating them reconstructs the
		// byte stream KCP would consume.
		splitKCPPayload(frame[epochHdrLen:], func(part []byte) {
			r.delivered.Write(part)
		})
	})
}

// runWirePath feeds frames through batch/reassembly, injecting RTP-level loss.
func runWirePath(t *testing.T, batchSize int, dropRatio float64, seed int64, mode writerMode) (got []byte, gotFrames, gotSamples, gotMaxSample int) {
	t.Helper()

	p := &streamTransport{batchSize: batchSize}
	frames := buildSimFrames(simTotal)

	src := make(chan *packetBuffer, 256)
	go func() {
		for _, f := range frames {
			src <- &packetBuffer{data: f}
		}
		close(src)
	}()

	pay := &codecs.VP8Payloader{}
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test fixture
	recv := &simReceiver{reorder: newReorderBuffer()}

	// tick sends one VP8 sample; endIdx marks the last RTP packet of each sample
	// the way pion's packetizer marks the frame boundary.
	sendSample := func(sample []byte, seq *uint16) {
		payloads := pay.Payload(simMTU, sample)
		recv.samples++
		if len(sample) > recv.maxSample {
			recv.maxSample = len(sample)
		}
		for i, payload := range payloads {
			pkt := &rtp.Packet{
				Header: rtp.Header{
					SequenceNumber: *seq,
					Marker:         i == len(payloads)-1,
				},
				Payload: payload,
			}
			*seq++
			if dropRatio > 0 && rng.Float64() < dropRatio {
				continue // SFU/policer ate this RTP packet
			}
			recv.accept(pkt)
		}
	}

	var seq uint16
	batchBuf := make([]byte, 0, defaultMaxPayloadSize)
	var carry *packetBuffer // frame deferred by the batcher, sent next tick

	for iterations := 0; ; iterations++ {
		if iterations > simTotal*2 {
			t.Fatalf("writer loop did not terminate after %d iterations", iterations)
		}
		first := carry
		carry = nil
		if first == nil {
			next, ok := <-src
			if !ok {
				return recv.delivered.Bytes(), recv.frames, recv.samples, recv.maxSample
			}
			first = next
		}

		switch mode {
		case modePerPacket:
			sendSample(first.data, &seq)
			for n := 1; n < batchSize; n++ {
				select {
				case next, more := <-src:
					if !more {
						return recv.delivered.Bytes(), recv.frames, recv.samples, recv.maxSample
					}
					sendSample(next.data, &seq)
				default:
					n = batchSize // nothing more queued this tick
				}
			}
		default:
			sample, pending := p.batchSampleFrom(src, first, batchBuf)
			batchBuf = sample[:0]
			sendSample(sample, &seq)
			carry = pending
		}
	}
}

// checkWholeFrames asserts the stream handed to KCP consists of complete frames
// with strictly increasing indices. A short tail is allowed (the last sample may
// have been cut); anything else means KCP was fed a garbled byte stream.
func checkWholeFrames(t *testing.T, got []byte, label string) {
	t.Helper()

	body := simFrameBody
	if len(got)%body != 0 {
		t.Logf("%s: trailing %d bytes (incomplete last frame); checking %d complete frames",
			label, len(got)%body, len(got)/body)
		got = got[:len(got)/body*body]
	}

	prev := -1
	for i := 0; i+body <= len(got); i += body {
		chunk := got[i : i+body]
		if chunk[0] != '<' || chunk[1] != '#' {
			t.Fatalf("%s: frame at offset %d does not start with a tag: %q (KCP would consume a spliced stream)",
				label, i, chunk[:12])
		}
		var idx int
		if _, err := fmt.Sscanf(string(chunk[2:8]), "%06d", &idx); err != nil {
			t.Fatalf("%s: unreadable tag at offset %d: %q", label, i, chunk[:10])
		}
		if idx <= prev {
			t.Fatalf("%s: frame index went backwards at offset %d: #%d after #%d (replayed or spliced stream)",
				label, i, idx, prev)
		}
		prev = idx
	}
}

func TestWirePathCleanTransferIsByteExact(t *testing.T) {
	got, gotFrames, gotSamples, gotMaxSample := runWirePath(t, 64, 0, 1, modeCoalesced)
	want := make([]byte, 0, simTotal*simFrameBody)
	for _, f := range buildSimFrames(simTotal) {
		want = append(want, f[epochHdrLen:]...)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("clean transfer corrupted: got %d bytes in %d frames/%d samples, want %d; first divergence at %d",
			len(got), gotFrames, gotSamples, len(want), firstDiff(got, want))
	}
	if gotMaxSample > singleSampleLimit {
		t.Fatalf("clean transfer used a %d-byte sample, max %d (fragmented => lost as a whole)",
			gotMaxSample, singleSampleLimit)
	}
	t.Logf("byte-exact: %d bytes, %d frames, %d samples, largest sample %d bytes",
		len(got), gotFrames, gotSamples, gotMaxSample)
}

func TestWirePathNoGarbledSplicesUnderLoss(t *testing.T) {
	for _, mode := range []writerMode{modeCoalesced, modePerPacket} {
		for _, batch := range []int{1, 8, 64} {
			for _, ratio := range []float64{0.01, 0.05} {
				label := fmt.Sprintf("mode=%d batch=%d loss=%.0f%%", mode, batch, ratio*100)
				got, frames, samples, maxSample := runWirePath(t, batch, ratio, 7, mode)
				checkWholeFrames(t, got, label)
				if maxSample > singleSampleLimit {
					t.Fatalf("%s: emitted a %d-byte sample, max %d", label, maxSample, singleSampleLimit)
				}
				t.Logf("%s: %d bytes over %d frames / %d samples, largest sample %d", label, len(got), frames, samples, maxSample)
			}
		}
	}
}

// TestCoalescedVersusPerPacketUnderEqualLoss is the headline measurement behind
// the writer change: at the same RTP loss ratio, packing 64 KCP packets into one
// fragmented sample loses almost everything, while one sample per packet loses
// roughly the injected ratio.
// TestCoalescedWriterSurvivesRTPPacketLoss is the regression guard for the loss
// amplification that broke bulk transfers. Batching that let one VP8 sample span
// several RTP packets lost the whole sample - and every KCP segment inside it -
// whenever any one of those packets was dropped. Measured here at a routine 1%
// RTP loss with batch_size=64, that shape delivered 0.75% of segments while
// per-packet samples delivered ~88%.
//
// Coalescing is still allowed (it amortises framing over small segments) but only
// while the sample stays inside one RTP packet, so the batched writer must now
// match the per-packet writer instead of collapsing next to it.
func TestCoalescedWriterSurvivesRTPPacketLoss(t *testing.T) {
	const ratio = 0.01
	_, coFrames, coSamples, coMax := runWirePath(t, 64, ratio, 7, modeCoalesced)
	_, perFrames, perSamples, perMax := runWirePath(t, 64, ratio, 7, modePerPacket)

	t.Logf("loss=%.0f%% batch=64 -> coalesced: %d/%d frames (%.2f%%) in %d samples (largest %d B) | per-packet: %d/%d frames (%.2f%%) in %d samples (largest %d B)",
		ratio*100, coFrames, simTotal, 100*float64(coFrames)/simTotal, coSamples, coMax,
		perFrames, simTotal, 100*float64(perFrames)/simTotal, perSamples, perMax)

	if coMax > singleSampleLimit || perMax > singleSampleLimit {
		t.Fatalf("writer fragmented a sample: coalesced=%d per-packet=%d, max %d",
			coMax, perMax, singleSampleLimit)
	}
	if coFrames < simTotal*80/100 {
		t.Fatalf("batched writer lost %d/%d frames at %.0f%% RTP loss: a sample must not span RTP packets",
			simTotal-coFrames, simTotal, ratio*100)
	}
	if coFrames < perFrames {
		t.Fatalf("coalescing still costs delivery: coalesced=%d per-packet=%d", coFrames, perFrames)
	}
}

func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}
