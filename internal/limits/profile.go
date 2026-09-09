// Package limits defines immutable per-session resource budgets.
package limits

import "time"

// Profile is copied into a client/server session at construction time.
// A zero Profile normalizes to Default.
type Profile struct {
	KCP     KCP
	VP8     VP8
	Smux    Smux
	MuxConn MuxConn
	SOCKS   SOCKS
}

type KCP struct {
	DataSendWindow       int
	DataReceiveWindow    int
	ControlSendWindow    int
	ControlReceiveWindow int
	DataInboundQueue     int
	ControlInboundQueue  int
}

type VP8 struct {
	DataOutboundQueue    int
	ControlOutboundQueue int
	RTPReorderWindow     int
}

type Smux struct {
	DataReceiveBuffer    int
	DataStreamBuffer     int
	ControlReceiveBuffer int
	ControlStreamBuffer  int
}

type MuxConn struct {
	DataInboundQueue    int
	ControlInboundQueue int
}

type SOCKS struct {
	MaxTCP                  int
	MaxUDP                  int
	MaxTotal                int
	HandshakeTimeout        time.Duration
	UDPAssociateIdleTimeout time.Duration
}

// Default preserves upstream server/desktop throughput defaults.
func Default() Profile {
	return Profile{
		KCP: KCP{
			DataSendWindow: 4096, DataReceiveWindow: 4096,
			ControlSendWindow: 4096, ControlReceiveWindow: 4096,
			DataInboundQueue: 4096, ControlInboundQueue: 4096,
		},
		VP8: VP8{
			DataOutboundQueue: 1536, ControlOutboundQueue: 2048, RTPReorderWindow: 256,
		},
		Smux: Smux{
			DataReceiveBuffer: 32 * 1024 * 1024, DataStreamBuffer: 4 * 1024 * 1024,
			ControlReceiveBuffer: 256 * 1024, ControlStreamBuffer: 32 * 1024,
		},
		MuxConn: MuxConn{DataInboundQueue: 128, ControlInboundQueue: 128},
		SOCKS: SOCKS{
			MaxTCP: 512, MaxUDP: 512, MaxTotal: 512,
			HandshakeTimeout:        30 * time.Second,
			UDPAssociateIdleTimeout: 2 * time.Minute,
		},
	}
}

// MobileLowMemory targets the iOS NetworkExtension 50 MiB budget.
func MobileLowMemory() Profile {
	return Profile{
		KCP: KCP{
			DataSendWindow: 512, DataReceiveWindow: 512,
			ControlSendWindow: 64, ControlReceiveWindow: 64,
			DataInboundQueue: 512, ControlInboundQueue: 128,
		},
		VP8: VP8{
			DataOutboundQueue: 256, ControlOutboundQueue: 128, RTPReorderWindow: 64,
		},
		Smux: Smux{
			DataReceiveBuffer: 1024 * 1024, DataStreamBuffer: 128 * 1024,
			ControlReceiveBuffer: 64 * 1024, ControlStreamBuffer: 16 * 1024,
		},
		MuxConn: MuxConn{DataInboundQueue: 16, ControlInboundQueue: 8},
		SOCKS: SOCKS{
			MaxTCP: 12, MaxUDP: 8, MaxTotal: 20,
			HandshakeTimeout:        10 * time.Second,
			UDPAssociateIdleTimeout: 45 * time.Second,
		},
	}
}

// Normalize fills every zero/unset field from Default so consumers can rely on
// a fully populated profile regardless of how it was configured.
func Normalize(p Profile) Profile {
	d := Default()
	ints := []struct{ got, def *int }{
		{&p.KCP.DataSendWindow, &d.KCP.DataSendWindow},
		{&p.KCP.DataReceiveWindow, &d.KCP.DataReceiveWindow},
		{&p.KCP.ControlSendWindow, &d.KCP.ControlSendWindow},
		{&p.KCP.ControlReceiveWindow, &d.KCP.ControlReceiveWindow},
		{&p.KCP.DataInboundQueue, &d.KCP.DataInboundQueue},
		{&p.KCP.ControlInboundQueue, &d.KCP.ControlInboundQueue},
		{&p.VP8.DataOutboundQueue, &d.VP8.DataOutboundQueue},
		{&p.VP8.ControlOutboundQueue, &d.VP8.ControlOutboundQueue},
		{&p.VP8.RTPReorderWindow, &d.VP8.RTPReorderWindow},
		{&p.Smux.DataReceiveBuffer, &d.Smux.DataReceiveBuffer},
		{&p.Smux.DataStreamBuffer, &d.Smux.DataStreamBuffer},
		{&p.Smux.ControlReceiveBuffer, &d.Smux.ControlReceiveBuffer},
		{&p.Smux.ControlStreamBuffer, &d.Smux.ControlStreamBuffer},
		{&p.MuxConn.DataInboundQueue, &d.MuxConn.DataInboundQueue},
		{&p.MuxConn.ControlInboundQueue, &d.MuxConn.ControlInboundQueue},
		{&p.SOCKS.MaxTCP, &d.SOCKS.MaxTCP},
		{&p.SOCKS.MaxUDP, &d.SOCKS.MaxUDP},
		{&p.SOCKS.MaxTotal, &d.SOCKS.MaxTotal},
	}
	durations := []struct{ got, def *time.Duration }{
		{&p.SOCKS.HandshakeTimeout, &d.SOCKS.HandshakeTimeout},
		{&p.SOCKS.UDPAssociateIdleTimeout, &d.SOCKS.UDPAssociateIdleTimeout},
	}
	for _, pair := range ints {
		if *pair.got <= 0 {
			*pair.got = *pair.def
		}
	}
	for _, pair := range durations {
		if *pair.got <= 0 {
			*pair.got = *pair.def
		}
	}
	return p
}
