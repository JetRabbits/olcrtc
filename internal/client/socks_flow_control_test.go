package client

import (
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/limits"
)

func TestSetSocksFlowCeilingsRaisesAndShedsAdmission(t *testing.T) {
	c := &Client{resourceProfile: limits.Normalize(limits.Profile{SOCKS: limits.SOCKS{MaxTCP: 2, MaxUDP: 1, MaxTotal: 2}})}

	if !c.tryBeginFlow(flowKindTCP) || !c.tryBeginFlow(flowKindTCP) {
		t.Fatal("first two tcp flows should fill the initial ceiling")
	}
	if c.tryBeginFlow(flowKindTCP) {
		t.Fatal("third tcp flow accepted over the initial MaxTCP")
	}

	// Raising the ceiling must take effect on the next admission without
	// touching the flows that are already active.
	c.SetSocksFlowCeilings(4, 0, 4)
	if !c.tryBeginFlow(flowKindTCP) {
		t.Fatal("tcp flow refused after the ceiling was raised")
	}
	if got := c.maxSocksConns(); got != 4+maxSocksSlotWaiters {
		t.Fatalf("maxSocksConns() = %d, want MaxTotal %d plus %d slot waiters", got, 4, maxSocksSlotWaiters)
	}

	// Shedding below the active count must not disturb established flows; only
	// new admissions are refused.
	c.SetSocksFlowCeilings(1, 0, 1)
	if c.tryBeginFlow(flowKindTCP) {
		t.Fatal("tcp flow accepted after the ceiling was shed below the active count")
	}
	// Three flows are still active against a ceiling of 1: they must drain
	// before the shed ceiling admits anything again.
	c.endFlow(flowKindTCP)
	c.endFlow(flowKindTCP)
	if c.tryBeginFlow(flowKindTCP) {
		t.Fatal("tcp flow accepted while active flows still fill the shed ceiling")
	}
	c.endFlow(flowKindTCP)
	if !c.tryBeginFlow(flowKindTCP) {
		t.Fatal("tcp flow refused although the shed ceiling leaves room")
	}
}

func TestSetSocksFlowCeilingsKeepsNonPositiveFields(t *testing.T) {
	c := &Client{resourceProfile: limits.Normalize(limits.Profile{SOCKS: limits.SOCKS{MaxTCP: 3, MaxUDP: 5, MaxTotal: 9}})}

	c.SetSocksFlowCeilings(0, -1, 7)
	if c.resourceProfile.SOCKS.MaxTCP != 3 {
		t.Fatalf("MaxTCP = %d, want 3 (zero must leave the field unchanged)", c.resourceProfile.SOCKS.MaxTCP)
	}
	if c.resourceProfile.SOCKS.MaxUDP != 5 {
		t.Fatalf("MaxUDP = %d, want 5 (negative must leave the field unchanged)", c.resourceProfile.SOCKS.MaxUDP)
	}
	if c.resourceProfile.SOCKS.MaxTotal != 7 {
		t.Fatalf("MaxTotal = %d, want 7", c.resourceProfile.SOCKS.MaxTotal)
	}
}
