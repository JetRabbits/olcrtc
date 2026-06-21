package goolom

import (
	"testing"
	"time"
)

//nolint:cyclop // table-driven test naturally has many branches
func TestSessionReconnectAndEndedHelpers(t *testing.T) {
	s := &Session{
		reconnectCh:    make(chan struct{}, 2),
		closeCh:        make(chan struct{}),
		keepAliveCh:    make(chan struct{}),
		sessionCloseCh: make(chan struct{}),
		telemetryCh:    make(chan struct{}, 1),
	}

	keepAliveCh, sessionCloseCh := s.resetSession()
	if keepAliveCh == nil || sessionCloseCh == nil || keepAliveCh != s.keepAliveCh || sessionCloseCh != s.sessionCloseCh {
		t.Fatal("resetSession() did not replace session channels")
	}

	s.subscriberReady.Store(true)
	s.publisherReady.Store(true)
	s.resetMediaState()
	if s.subscriberReady.Load() || s.publisherReady.Load() || s.subscriberConn == nil || s.publisherConn == nil {
		t.Fatal("resetMediaState() did not reset readiness")
	}

	s.queueReconnect()
	select {
	case <-s.reconnectCh:
	default:
		t.Fatal("queueReconnect() did not enqueue")
	}

	s.SetShouldReconnect(func() bool { return false })
	s.queueReconnect()
	select {
	case <-s.reconnectCh:
		t.Fatal("queueReconnect() enqueued despite policy=false")
	default:
	}

	s.reconnectCh <- struct{}{}
	s.reconnectCh <- struct{}{}
	s.drainReconnectQueue()
	select {
	case <-s.reconnectCh:
		t.Fatal("drainReconnectQueue() left queued item")
	default:
	}

	s.telemetryActive.Store(true)
	s.stopTelemetry()
	select {
	case <-s.telemetryCh:
	default:
		t.Fatal("stopTelemetry() did not signal active telemetry")
	}

	ended := ""
	s.SetEndedCallback(func(reason string) { ended = reason })
	s.signalEnded("done")
	if !s.closed.Load() || ended != "done" {
		t.Fatalf("signalEnded() closed=%v reason=%q", s.closed.Load(), ended)
	}
}

func TestWaitForAckTimeoutAndClose(t *testing.T) {
	s := &Session{
		closeCh:    make(chan struct{}),
		ackWaiters: make(map[string]chan struct{}),
	}
	ch := s.registerAckWaiter("timeout")
	if s.waitForAck("timeout", ch, time.Millisecond) {
		t.Fatal("waitForAck(timeout) = true")
	}

	ch = s.registerAckWaiter("closed")
	close(s.closeCh)
	if s.waitForAck("closed", ch, time.Second) {
		t.Fatal("waitForAck(closeCh) = true")
	}
}

func TestRemoveDescriptionRearmsReconnectOnNewParticipant(t *testing.T) {
	s := &Session{
		closeCh:             make(chan struct{}),
		signalSummaryCounts: make(map[string]int),
	}

	s.handleCommonMessages(map[string]any{"removeDescription": map[string]any{
		"descriptionId": []any{"leaving-participant"},
	}}, "")

	if s.reconnectOnNewParticipant.Load() {
		t.Fatal("removeDescription re-armed reconnectOnNewParticipant while feature disabled")
	}

	s.SetReconnectOnNewParticipant(true)
	s.reconnectOnNewParticipant.Store(false)

	s.handleCommonMessages(map[string]any{"removeDescription": map[string]any{
		"descriptionId": []any{"leaving-participant"},
	}}, "")

	if !s.reconnectOnNewParticipant.Load() {
		t.Fatal("removeDescription did not re-arm reconnectOnNewParticipant")
	}

	s.reconnectOnNewParticipant.Store(false)
	s.closed.Store(true)
	s.handleCommonMessages(map[string]any{"removeDescription": map[string]any{
		"descriptionId": []any{"leaving-participant"},
	}}, "")

	if s.reconnectOnNewParticipant.Load() {
		t.Fatal("removeDescription re-armed reconnectOnNewParticipant after session closed")
	}
}

func TestIsOwnDescription(t *testing.T) {
	s := &Session{peerID: "server-peer", name: "server-name"}

	tests := []struct {
		name  string
		entry map[string]any
		want  bool
	}{
		{
			name:  "peer id matches",
			entry: map[string]any{"id": "server-peer"},
			want:  true,
		},
		{
			name: "meta name matches",
			entry: map[string]any{
				"id":   "telemost-description-id",
				"meta": map[string]any{keyName: "server-name"},
			},
			want: true,
		},
		{
			name: "participant attributes name matches",
			entry: map[string]any{
				"id":                    "telemost-description-id",
				"participantAttributes": map[string]any{keyName: "server-name"},
			},
			want: true,
		},
		{
			name: "remote participant",
			entry: map[string]any{
				"id":        "remote-description-id",
				"sendVideo": true,
				"meta":      map[string]any{keyName: "remote-name"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.isOwnDescription(tt.entry); got != tt.want {
				t.Fatalf("isOwnDescription() = %v, want %v", got, tt.want)
			}
		})
	}
}
