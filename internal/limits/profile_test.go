package limits

import (
	"testing"
	"time"
)

func TestDefaultProfilePreservesHistoricalBudgets(t *testing.T) {
	p := Default()
	if p.KCP.DataSendWindow != 4096 || p.KCP.DataReceiveWindow != 4096 ||
		p.KCP.ControlSendWindow != 4096 || p.KCP.ControlReceiveWindow != 4096 ||
		p.KCP.DataInboundQueue != 4096 || p.KCP.ControlInboundQueue != 4096 {
		t.Fatalf("KCP defaults = %+v", p.KCP)
	}
	if p.VP8.DataOutboundQueue != 1536 || p.VP8.ControlOutboundQueue != 2048 || p.VP8.RTPReorderWindow != 256 {
		t.Fatalf("VP8 defaults = %+v", p.VP8)
	}
	if p.Smux.DataReceiveBuffer != 8*1024*1024 || p.Smux.DataStreamBuffer != 512*1024 ||
		p.Smux.ControlReceiveBuffer != 256*1024 || p.Smux.ControlStreamBuffer != 32*1024 {
		t.Fatalf("smux defaults = %+v", p.Smux)
	}
	if p.MuxConn.DataInboundQueue != 128 || p.MuxConn.ControlInboundQueue != 128 {
		t.Fatalf("muxconn defaults = %+v", p.MuxConn)
	}
	if p.SOCKS != (SOCKS{}) {
		t.Fatalf("SOCKS defaults = %+v", p.SOCKS)
	}
}

func TestMobileLowMemoryProfileBudgets(t *testing.T) {
	p := MobileLowMemory()
	if p.KCP.DataSendWindow != 512 || p.KCP.DataReceiveWindow != 512 ||
		p.KCP.ControlSendWindow != 64 || p.KCP.ControlReceiveWindow != 64 ||
		p.KCP.DataInboundQueue != 512 || p.KCP.ControlInboundQueue != 128 {
		t.Fatalf("KCP mobile = %+v", p.KCP)
	}
	if p.VP8.DataOutboundQueue != 256 || p.VP8.ControlOutboundQueue != 128 || p.VP8.RTPReorderWindow != 64 {
		t.Fatalf("VP8 mobile = %+v", p.VP8)
	}
	if p.Smux.DataReceiveBuffer != 1024*1024 || p.Smux.DataStreamBuffer != 128*1024 ||
		p.Smux.ControlReceiveBuffer != 64*1024 || p.Smux.ControlStreamBuffer != 16*1024 {
		t.Fatalf("smux mobile = %+v", p.Smux)
	}
	if p.MuxConn.DataInboundQueue != 16 || p.MuxConn.ControlInboundQueue != 8 {
		t.Fatalf("muxconn mobile = %+v", p.MuxConn)
	}
	if p.SOCKS.MaxTCP != 12 || p.SOCKS.MaxUDP != 8 || p.SOCKS.MaxTotal != 20 ||
		p.SOCKS.HandshakeTimeout != 10*time.Second ||
		p.SOCKS.UDPAssociateIdleTimeout != 45*time.Second {
		t.Fatalf("SOCKS mobile = %+v", p.SOCKS)
	}
}

func TestNormalizeFillsUnsetFieldsFromDefaults(t *testing.T) {
	p := Normalize(Profile{KCP: KCP{DataSendWindow: 7}, SOCKS: SOCKS{MaxTCP: 2}})
	if p.KCP.DataSendWindow != 7 || p.KCP.DataReceiveWindow != Default().KCP.DataReceiveWindow {
		t.Fatalf("normalized KCP = %+v", p.KCP)
	}
	if p.SOCKS.MaxTCP != 2 || p.SOCKS.MaxUDP != 0 {
		t.Fatalf("normalized SOCKS = %+v", p.SOCKS)
	}
}
