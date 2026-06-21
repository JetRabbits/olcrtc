package goolom

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

func (s *Session) sendHello() error {
	hello := map[string]any{
		keyUID: uuid.New().String(),
		"hello": map[string]any{
			"participantMeta": map[string]any{
				keyName:        s.name,
				"role":         "SPEAKER",
				keyDescription: "",
				"sendAudio":    false,
				"sendVideo":    s.hasLocalVideoTracks(),
			},
			"participantAttributes": map[string]any{
				keyName:        s.name,
				"role":         "SPEAKER",
				keyDescription: "",
			},
			"sendAudio":         false,
			"sendVideo":         s.hasLocalVideoTracks(),
			"sendSharing":       false,
			"participantId":     s.peerID,
			"roomId":            s.roomID,
			"serviceName":       "telemost",
			"credentials":       s.credentials,
			"capabilitiesOffer": goolomCapabilitiesOffer(),
			"sdkInfo": map[string]any{
				"implementation": "browser",
				"version":        "5.27.0",
				"userAgent":      "Mozilla/5.0 (X11; Linux x86_64; rv:149.0) Gecko/20100101 Firefox/149.0",
				"hwConcurrency":  runtime.NumCPU(),
			},
			"sdkInitializationId":    uuid.New().String(),
			"disablePublisher":       !s.hasLocalVideoTracks(),
			"disableSubscriber":      false,
			"disableSubscriberAudio": true,
		},
	}

	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if err := s.ws.WriteJSON(hello); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}
	return nil
}

func (s *Session) handleSignaling(ctx context.Context) {
	pubSent := false
	offerCount := 0

	for {
		var msg map[string]any
		if err := s.ws.ReadJSON(&msg); err != nil {
			if !s.closed.Load() {
				logger.Debugf("ws read error: %v", err)
				s.queueReconnect()
			}
			return
		}

		s.updateWSDeadline()

		// Log all incoming signaling messages for debugging
		msgKeys := make([]string, 0, len(msg))
		for k := range msg {
			if k != keyUID {
				msgKeys = append(msgKeys, k)
			}
		}
		sort.Strings(msgKeys)
		logger.Infof("goolom ws msg keys: %v", msgKeys)

		uid, _ := msg[keyUID].(string)
		s.handleMessageEvents(ctx, msg, uid)

		if isConferenceEndMessage(msg) {
			s.signalEnded("conference ended")
			return
		}

		if offer, ok := msg["subscriberSdpOffer"].(map[string]any); ok {
			offerCount++
			logger.Infof("goolom subscriberSdpOffer #%d received pubSent=%v", offerCount, pubSent)
			if err := s.handleSdpOffer(offer, uid, !pubSent); err != nil {
				logger.Debugf("sdp offer error: %v", err)
				continue
			}
			pubSent = true
		}

		s.handleSignalingResponses(msg, uid)
	}
}

func (s *Session) handleMessageEvents(ctx context.Context, msg map[string]any, uid string) {
	if _, ok := msg["ack"]; ok {
		s.resolveAck(uid)
	}

	if serverHello, ok := msg["serverHello"].(map[string]any); ok {
		s.applyServerHelloConfig(serverHello)
		s.startTelemetry(ctx, serverHello)
		s.sendAck(uid)
	}

	s.handleCommonMessages(msg, uid)
}

func (s *Session) handleSignalingResponses(msg map[string]any, uid string) {
	if answer, ok := msg["publisherSdpAnswer"].(map[string]any); ok {
		s.handleSdpAnswer(answer, uid)
	}
	if cand, ok := msg["webrtcIceCandidate"].(map[string]any); ok {
		s.handleICE(cand)
	}
}

func (s *Session) updateWSDeadline() {
	s.wsMu.Lock()
	if s.ws != nil {
		_ = s.ws.SetReadDeadline(time.Now().Add(wsReadTimeout))
	}
	s.wsMu.Unlock()
}

func (s *Session) handleCommonMessages(msg map[string]any, uid string) {
	if payload, ok := msg["updateDescription"]; ok {
		s.logSignalSummary("updateDescription", payload, 4)
		s.sendAck(uid)
		if s.reconnectOnNewParticipant.Load() {
			s.checkNewParticipant(payload)
		}
	}
	if payload, ok := msg["upsertDescription"]; ok {
		s.logSignalSummary("upsertDescription", payload, 8)
		s.sendAck(uid)
		if s.reconnectOnNewParticipant.Load() {
			s.checkNewParticipant(payload)
		}
	}
	if payload, ok := msg["removeDescription"]; ok {
		s.logSignalSummary("removeDescription", payload, 4)
		s.sendAck(uid)
		s.rearmReconnectOnParticipantLeave()
	}
	if payload, ok := msg["slotsConfig"]; ok {
		s.logSignalSummary("slotsConfig", payload, 6)
		s.sendAck(uid)
	}
	if payload, ok := msg["slotsMeta"]; ok {
		s.logSignalSummary("slotsMeta", payload, 8)
		s.sendAck(uid)
	}
	if _, ok := msg["vadActivity"]; ok {
		s.sendAck(uid)
	}
	if _, ok := msg["selfQualityReport"]; ok {
		s.sendAck(uid)
	}
	if _, ok := msg["ping"]; ok {
		s.sendPong(uid)
	}
	if _, ok := msg["pong"]; ok {
		s.sendAck(uid)
	}
}

func (s *Session) logSignalSummary(label string, payload any, maxCount int) {
	s.signalSummaryMu.Lock()
	if s.signalSummaryCounts == nil {
		s.signalSummaryCounts = make(map[string]int)
	}
	s.signalSummaryCounts[label]++
	count := s.signalSummaryCounts[label]
	s.signalSummaryMu.Unlock()

	if count > maxCount {
		return
	}
	logSignalingPayloadSummary(label, count, payload)
}

func (s *Session) sendAck(uid string) {
	if uid == "" {
		return
	}
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	_ = s.ws.WriteJSON(map[string]any{
		keyUID: uid,
		"ack": map[string]any{
			"status": map[string]any{"code": "OK"},
		},
	})
}

func (s *Session) sendPong(uid string) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	_ = s.ws.WriteJSON(map[string]any{
		keyUID: uid,
		"pong": map[string]any{},
	})
}

func (s *Session) registerAckWaiter(uid string) chan struct{} {
	ch := make(chan struct{})
	s.ackMu.Lock()
	s.ackWaiters[uid] = ch
	s.ackMu.Unlock()
	return ch
}

func (s *Session) removeAckWaiter(uid string) {
	s.ackMu.Lock()
	delete(s.ackWaiters, uid)
	s.ackMu.Unlock()
}

func (s *Session) waitForAck(uid string, ch <-chan struct{}, timeout time.Duration) bool {
	if uid == "" {
		return false
	}
	defer s.removeAckWaiter(uid)

	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	case <-s.closeCh:
		return false
	}
}

func (s *Session) resolveAck(uid string) {
	if uid == "" {
		return
	}
	s.ackMu.Lock()
	ch := s.ackWaiters[uid]
	if ch != nil {
		delete(s.ackWaiters, uid)
		close(ch)
	}
	s.ackMu.Unlock()
}

func (s *Session) sendLeave(uid string) bool {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()

	if s.ws == nil {
		return false
	}
	leave := map[string]any{
		keyUID:  uid,
		"leave": map[string]any{},
	}
	if err := s.ws.WriteJSON(leave); err != nil {
		return false
	}
	return true
}

func (s *Session) keepAlive(keepAliveCh <-chan struct{}) {
	wsTicker := time.NewTicker(30 * time.Second)
	defer wsTicker.Stop()
	appTicker := time.NewTicker(5 * time.Second)
	defer appTicker.Stop()

	for {
		select {
		case <-wsTicker.C:
			if !s.sendWSPing() {
				return
			}
		case <-appTicker.C:
			if !s.sendAppPing() {
				return
			}
		case <-keepAliveCh:
			return
		case <-s.closeCh:
			return
		}
	}
}

func (s *Session) sendWSPing() bool {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if s.ws != nil {
		if err := s.ws.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
			logger.Debugf("ws ping error: %v", err)
			s.queueReconnect()
			return false
		}
	}
	return true
}

func (s *Session) sendAppPing() bool {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if s.ws != nil {
		if err := s.ws.WriteJSON(map[string]any{
			keyUID: uuid.New().String(),
			"ping": map[string]any{},
		}); err != nil {
			logger.Debugf("app ping error: %v", err)
			s.queueReconnect()
			return false
		}
	}
	return true
}

func isConferenceEndMessage(msg map[string]any) bool {
	for _, key := range []string{"conferenceClosed", "conferenceEnded", "roomClosed", "roomEnded", "callEnded"} {
		if _, ok := msg[key]; ok {
			return true
		}
	}
	if raw, ok := msg["conference"].(map[string]any); ok {
		if state, _ := raw["state"].(string); isEndedState(state) {
			return true
		}
	}
	if raw, ok := msg["conferenceState"].(map[string]any); ok {
		if state, _ := raw["state"].(string); isEndedState(state) {
			return true
		}
	}
	return false
}

func isEndedState(state string) bool {
	switch strings.ToLower(state) {
	case "closed", "ended", "finished", stateTerminated:
		return true
	default:
		return false
	}
}

// checkNewParticipant inspects an updateDescription/upsertDescription payload
// for new participants with sendVideo=true. If found, triggers a goolom
// reconnect so both peers get fresh SDP exchanges.
// Only triggers ONCE per session lifetime to avoid infinite reconnect loops
// (each reconnect creates new participant IDs in Telemost).
//
// The repair reconnect is skipped only after the client proves the reverse path
// is alive (the server receives a CONTROL_PONG and closes deferredReconnectCh).
// If no proof arrives quickly, the server reconnects so the still-waiting
// Android subscriber can receive a fresh Telemost SDP/MID binding.
//
// If a control pong was seen within the last 75s, the reconnect is suppressed:
// the client is likely actively using WireGuard and reconnecting would kill
// in-flight UDP streams.
func (s *Session) checkNewParticipant(payload any) {
	if !s.reconnectOnNewParticipant.CompareAndSwap(true, false) {
		return // already triggered once
	}
	// Suppress reconnect if a client control pong was recent — WireGuard may
	// still be establishing and a reconnect would kill its UDP streams.
	const handshakeProtectWindow = 75 * time.Second
	if ts := s.lastHandshakeAt.Load(); ts != 0 {
		elapsed := time.Since(time.Unix(0, ts))
		if elapsed < handshakeProtectWindow {
			logger.Infof("goolom: skipping reconnect-on-new-participant: handshake was %s ago (protect window %s)",
				elapsed.Truncate(time.Second), handshakeProtectWindow)
			// Do not restore the flag: we consumed it. The next session cycle
			// (after the next reconnect) will re-arm it via resetSession().
			return
		}
	}
	desc, ok := payload.(map[string]any)
	if !ok {
		s.reconnectOnNewParticipant.Store(true) // restore flag
		return
	}
	descriptions, ok := desc["description"].([]any)
	if !ok {
		s.reconnectOnNewParticipant.Store(true) // restore flag
		return
	}
	for _, rawEntry := range descriptions {
		entry, ok := rawEntry.(map[string]any)
		if !ok {
			continue
		}
		// Skip disconnected participants — they carry sendVideo=true from
		// their last active state but are no longer in the room.
		if _, hasDisconnectedAt := entry["disconnectedAt"]; hasDisconnectedAt {
			continue
		}
		if s.isOwnDescription(entry) {
			continue
		}
		sendVideo, _ := entry["sendVideo"].(bool)
		if sendVideo {
			logger.Infof("goolom: new video participant detected, waiting for control-path proof before repair reconnect")
			s.skipCredentialRefresh.Store(true)
			s.skipMediaReady.Store(true)
			// Reset the deferred channel so this goroutine waits for a NEW
			// CONTROL_PONG from the CURRENT client, not an old one.
			// The previous channel may already be closed (from the last session).
			s.sessionMu.Lock()
			s.deferredReconnectCh = make(chan struct{})
			deferredCh := s.deferredReconnectCh
			s.sessionMu.Unlock()
			go func() {
				// Wait for proof that the client's control path is alive.
				// - If the first CONTROL_PONG arrives: SERVER_WELCOME reached Android,
				//   so do NOT reconnect. Reconnecting here would break WireGuard.
				// - If no proof arrives within 20s: mid="" (Telemost MID binding
				//   is likely broken). Reconnect so this/next retry gets proper binding.
				// 20s gives the client enough time for: SDP exchange (~2s) + ICE
				// connection (~3s) + smux handshake (~1s) + SERVER_WELCOME delivery
				// + first CONTROL_PING/CONTROL_PONG round-trip (~2s) + SignalHandshakeComplete
				// closing deferredReconnectCh. Total ~10-12s worst case, so 20s
				// provides sufficient headroom without excessive delay.
				select {
				case <-deferredCh:
					logger.Infof("goolom: control path alive, skipping deferred reconnect")
					return // Do NOT reconnect — client is live, WireGuard is establishing
				case <-time.After(20 * time.Second):
					// Client did not prove control liveness within 20s — likely the
					// Telemost MID binding is empty (mid=""). Reconnect immediately
					// so the waiting client or next retry gets proper MID binding.
					logger.Infof("goolom: deferred reconnect timeout (no control pong in 20s — reconnecting for MID binding)")
				case <-s.closeCh:
					return
				}
				if !s.closed.Load() {
					logger.Infof("goolom: triggering deferred reconnect for fresh SDP exchange")
					s.queueReconnect()
				}
			}()
			return
		}
	}
	s.reconnectOnNewParticipant.Store(true) // no video participant found, restore flag
}

func (s *Session) isOwnDescription(entry map[string]any) bool {
	if id, _ := entry["id"].(string); id != "" && id == s.peerID {
		return true
	}
	if hasName(entry["meta"], s.name) || hasName(entry["participantAttributes"], s.name) {
		return true
	}
	return false
}

func hasName(raw any, name string) bool {
	if name == "" {
		return false
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return false
	}
	got, _ := m[keyName].(string)
	return got == name
}

func (s *Session) rearmReconnectOnParticipantLeave() {
	if s.closed.Load() || !s.reconnectOnNewParticipantEnabled.Load() {
		return
	}
	// The one-shot flag is intentionally consumed by checkNewParticipant so an
	// active client cannot be disrupted by repeated Telemost upserts. Once a
	// participant leaves, re-arm it for the next Android join even if the engine
	// is currently reconnecting. removeDescription commonly arrives during the
	// reconnect/error cleanup window; skipping it there leaves the long-lived
	// server permanently disarmed and every later client again sees the server
	// publisher as pre-existing (mid="" / UNSPECIFIED).
	if !s.reconnectOnNewParticipant.Swap(true) {
		logger.Infof("goolom: participant removed, re-arming reconnect-on-new-participant for next join")
	}
}
