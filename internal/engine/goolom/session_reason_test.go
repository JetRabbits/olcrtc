package goolom

import "testing"

func TestSetConsumeReconnectReason(t *testing.T) {
	s := &Session{}

	if got := s.consumeReconnectReason(); got != "unspecified" {
		t.Fatalf("consumeReconnectReason() on fresh session = %q, want unspecified", got)
	}

	s.setReconnectReason("network-handover")
	if got := s.consumeReconnectReason(); got != "network-handover" {
		t.Fatalf("consumeReconnectReason() = %q, want network-handover", got)
	}
	if got := s.consumeReconnectReason(); got != "unspecified" {
		t.Fatalf("reason must be consumed once, second read = %q", got)
	}
}

func TestReconnectReasonLatestWins(t *testing.T) {
	s := &Session{}
	s.setReconnectReason("unspecified")
	s.setReconnectReason("network")
	if got := s.consumeReconnectReason(); got != "network" {
		t.Fatalf("consumeReconnectReason() = %q, want the latest reason network", got)
	}
}

func TestIsNetworkReconnectReason(t *testing.T) {
	cases := map[string]bool{
		"network":           true,
		"network-change":    true,
		"network-handover":  true,
		"handover":          true,
		"":                  false,
		"unspecified":       false,
		"liveness":          false,
		"provider":          false,
		"Network":           false,
		"ghost-participant": false,
	}
	for reason, want := range cases {
		if got := isNetworkReconnectReason(reason); got != want {
			t.Errorf("isNetworkReconnectReason(%q) = %v, want %v", reason, got, want)
		}
	}
}
