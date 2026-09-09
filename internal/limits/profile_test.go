package limits

import (
	"testing"
	"time"
)

func TestDefaultProfilePreservesUpstreamBudgets(t *testing.T) {
	p := Default()
	if p.Smux.DataReceiveBuffer != 32*1024*1024 || p.Smux.DataStreamBuffer != 4*1024*1024 || p.VP8.DataOutboundQueue != 1536 || p.SOCKS.MaxTotal != 512 {
		t.Fatalf("Default() = %+v", p)
	}
}

func TestMobileLowMemoryProfileBudgets(t *testing.T) {
	p := MobileLowMemory()
	if p.KCP.DataSendWindow != 512 || p.KCP.ControlSendWindow != 64 || p.VP8.RTPReorderWindow != 64 || p.MuxConn.DataInboundQueue != 16 || p.SOCKS.MaxTCP != 12 || p.SOCKS.UDPAssociateIdleTimeout != 45*time.Second {
		t.Fatalf("MobileLowMemory() = %+v", p)
	}
}

func TestNormalizeFillsUnsetFieldsFromDefaults(t *testing.T) {
	p := Normalize(Profile{KCP: KCP{DataSendWindow: 7}, SOCKS: SOCKS{MaxTCP: 2}})
	if p.KCP.DataSendWindow != 7 || p.KCP.DataReceiveWindow != Default().KCP.DataReceiveWindow || p.SOCKS.MaxTCP != 2 || p.SOCKS.MaxUDP != Default().SOCKS.MaxUDP {
		t.Fatalf("Normalize() = %+v", p)
	}
}
