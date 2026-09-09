package client

import (
	"testing"

	internalclient "github.com/openlibrecommunity/olcrtc/internal/client"
)

type fakeRequester struct{ requests []string }

func (f *fakeRequester) RequestReconnect(reason string) error {
	f.requests = append(f.requests, reason)
	return nil
}

func TestToClientConfigMapsClientReadyFunc(t *testing.T) {
	var received ReconnectRequester
	cfg := toClientConfig(Config{
		OnClientReady: func(requester ReconnectRequester) { received = requester },
	})
	if cfg.OnClientReady == nil {
		t.Fatal("toClientConfig dropped OnClientReady")
	}

	requester := &fakeRequester{}
	cfg.OnClientReady(requester)
	if received != requester {
		t.Fatalf("OnClientReady received %v, want the original requester", received)
	}
	if err := received.RequestReconnect("network"); err != nil {
		t.Fatalf("forwarded RequestReconnect() error = %v", err)
	}
	if len(requester.requests) != 1 || requester.requests[0] != "network" {
		t.Fatalf("requester recorded %v", requester.requests)
	}
}

func TestToClientConfigWithoutClientReadyFunc(t *testing.T) {
	if cfg := toClientConfig(Config{}); cfg.OnClientReady != nil {
		t.Fatal("unset OnClientReady must map to nil")
	}
}

var _ internalclient.ReconnectRequester = (*fakeRequester)(nil)
