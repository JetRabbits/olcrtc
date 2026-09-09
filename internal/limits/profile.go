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
		KCP:     KCP{DataSendWindow: 4096, DataReceiveWindow: 4096, ControlSendWindow: 4096, ControlReceiveWindow: 4096, DataInboundQueue: 4096, ControlInboundQueue: 4096},
		VP8:     VP8{DataOutboundQueue: 1536, ControlOutboundQueue: 2048, RTPReorderWindow: 256},
		Smux:    Smux{DataReceiveBuffer: 32 * 1024 * 1024, DataStreamBuffer: 4 * 1024 * 1024, ControlReceiveBuffer: 256 * 1024, ControlStreamBuffer: 32 * 1024},
		MuxConn: MuxConn{DataInboundQueue: 128, ControlInboundQueue: 128},
		SOCKS:   SOCKS{MaxTCP: 512, MaxUDP: 512, MaxTotal: 512, HandshakeTimeout: 30 * time.Second, UDPAssociateIdleTimeout: 2 * time.Minute},
	}
}

// MobileLowMemory targets the iOS NetworkExtension 50 MiB budget.
func MobileLowMemory() Profile {
	return Profile{
		KCP:     KCP{DataSendWindow: 512, DataReceiveWindow: 512, ControlSendWindow: 64, ControlReceiveWindow: 64, DataInboundQueue: 512, ControlInboundQueue: 128},
		VP8:     VP8{DataOutboundQueue: 256, ControlOutboundQueue: 128, RTPReorderWindow: 64},
		Smux:    Smux{DataReceiveBuffer: 1024 * 1024, DataStreamBuffer: 128 * 1024, ControlReceiveBuffer: 64 * 1024, ControlStreamBuffer: 16 * 1024},
		MuxConn: MuxConn{DataInboundQueue: 16, ControlInboundQueue: 8},
		SOCKS:   SOCKS{MaxTCP: 12, MaxUDP: 8, MaxTotal: 20, HandshakeTimeout: 10 * time.Second, UDPAssociateIdleTimeout: 45 * time.Second},
	}
}

func Normalize(p Profile) Profile {
	d := Default()
	if p.KCP.DataSendWindow <= 0 {
		p.KCP.DataSendWindow = d.KCP.DataSendWindow
	}
	if p.KCP.DataReceiveWindow <= 0 {
		p.KCP.DataReceiveWindow = d.KCP.DataReceiveWindow
	}
	if p.KCP.ControlSendWindow <= 0 {
		p.KCP.ControlSendWindow = d.KCP.ControlSendWindow
	}
	if p.KCP.ControlReceiveWindow <= 0 {
		p.KCP.ControlReceiveWindow = d.KCP.ControlReceiveWindow
	}
	if p.KCP.DataInboundQueue <= 0 {
		p.KCP.DataInboundQueue = d.KCP.DataInboundQueue
	}
	if p.KCP.ControlInboundQueue <= 0 {
		p.KCP.ControlInboundQueue = d.KCP.ControlInboundQueue
	}
	if p.VP8.DataOutboundQueue <= 0 {
		p.VP8.DataOutboundQueue = d.VP8.DataOutboundQueue
	}
	if p.VP8.ControlOutboundQueue <= 0 {
		p.VP8.ControlOutboundQueue = d.VP8.ControlOutboundQueue
	}
	if p.VP8.RTPReorderWindow <= 0 {
		p.VP8.RTPReorderWindow = d.VP8.RTPReorderWindow
	}
	if p.Smux.DataReceiveBuffer <= 0 {
		p.Smux.DataReceiveBuffer = d.Smux.DataReceiveBuffer
	}
	if p.Smux.DataStreamBuffer <= 0 {
		p.Smux.DataStreamBuffer = d.Smux.DataStreamBuffer
	}
	if p.Smux.ControlReceiveBuffer <= 0 {
		p.Smux.ControlReceiveBuffer = d.Smux.ControlReceiveBuffer
	}
	if p.Smux.ControlStreamBuffer <= 0 {
		p.Smux.ControlStreamBuffer = d.Smux.ControlStreamBuffer
	}
	if p.MuxConn.DataInboundQueue <= 0 {
		p.MuxConn.DataInboundQueue = d.MuxConn.DataInboundQueue
	}
	if p.MuxConn.ControlInboundQueue <= 0 {
		p.MuxConn.ControlInboundQueue = d.MuxConn.ControlInboundQueue
	}
	if p.SOCKS.MaxTCP <= 0 {
		p.SOCKS.MaxTCP = d.SOCKS.MaxTCP
	}
	if p.SOCKS.MaxUDP <= 0 {
		p.SOCKS.MaxUDP = d.SOCKS.MaxUDP
	}
	if p.SOCKS.MaxTotal <= 0 {
		p.SOCKS.MaxTotal = d.SOCKS.MaxTotal
	}
	if p.SOCKS.HandshakeTimeout <= 0 {
		p.SOCKS.HandshakeTimeout = d.SOCKS.HandshakeTimeout
	}
	if p.SOCKS.UDPAssociateIdleTimeout <= 0 {
		p.SOCKS.UDPAssociateIdleTimeout = d.SOCKS.UDPAssociateIdleTimeout
	}
	return p
}
