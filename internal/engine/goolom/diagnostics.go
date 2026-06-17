package goolom

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/pion/webrtc/v4"
)

const (
	maxSDPSummaryLines       = 96
	maxSignalSummaryParts    = 96
	maxSignalSummaryDepth    = 4
	maxSignalSummaryArrayLen = 4
)

var safeSDPLinePrefixes = []string{
	"m=video",
	"a=mid:",
	"a=rtpmap:",
	"a=fmtp:",
	"a=rtcp-fb:",
	"a=ssrc:",
	"a=msid:",
	"a=rid:",
	"a=simulcast:",
	"a=extmap:",
}

func logSDPSummary(label string, pcSeq int, sdp string) {
	summary := summarizeSDP(sdp)
	logger.Infof("goolom %s sdp pcSeq=%d media_sections=%d video_sections=%d lines=%d truncated=%v summary=%s",
		label,
		pcSeq,
		countSDPMediaSections(sdp),
		countSDPVideoSections(sdp),
		summary.LineCount,
		summary.Truncated,
		strings.Join(summary.Lines, " | "))
}

func summarizeSDP(sdp string) sdpSummary {
	if sdp == "" {
		return sdpSummary{}
	}

	lines := make([]string, 0, maxSDPSummaryLines)
	for _, rawLine := range strings.Split(sdp, "\n") {
		line := strings.TrimSpace(rawLine)
		if !isSafeSDPLine(line) {
			continue
		}
		lines = append(lines, line)
		if len(lines) >= maxSDPSummaryLines {
			return sdpSummary{Lines: lines, LineCount: len(lines), Truncated: true}
		}
	}

	return sdpSummary{Lines: lines, LineCount: len(lines)}
}

type sdpSummary struct {
	Lines     []string
	LineCount int
	Truncated bool
}

func isSafeSDPLine(line string) bool {
	for _, prefix := range safeSDPLinePrefixes {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func countSDPMediaSections(sdp string) int {
	return strings.Count("\n"+sdp, "\nm=")
}

func countSDPVideoSections(sdp string) int {
	return strings.Count("\n"+sdp, "\nm=video")
}

func logRemoteTrack(track *webrtc.TrackRemote) {
	if track == nil {
		return
	}
	codec := track.Codec()
	logger.Infof("goolom remote video track: codec=%s payload_type=%d ssrc=%d rid=%s stream=%s track=%s",
		codec.MimeType,
		codec.PayloadType,
		track.SSRC(),
		track.RID(),
		track.StreamID(),
		track.ID())
}

func logPublisherTrackDescriptions(tracks []map[string]any) {
	if len(tracks) == 0 {
		logger.Infof("goolom publisherSdpOffer tracks: count=0")
		return
	}

	parts := make([]string, 0, len(tracks))
	for i, track := range tracks {
		parts = append(parts, fmt.Sprintf("#%d kind=%v mid=%v transceiverMid=%v label=%v groupId=%v codecs=%T",
			i,
			track["kind"],
			track["mid"],
			track["transceiverMid"],
			track["label"],
			track["groupId"],
			track["codecs"]))
	}
	logger.Infof("goolom publisherSdpOffer tracks: count=%d %s", len(tracks), strings.Join(parts, " | "))
}

func logSignalingPayloadSummary(label string, count int, raw any) {
	parts := make([]string, 0, maxSignalSummaryParts)
	summarizeSignalingValue(label, raw, 0, &parts)
	truncated := len(parts) >= maxSignalSummaryParts
	logger.Infof("goolom %s summary #%d parts=%d truncated=%v %s",
		label,
		count,
		len(parts),
		truncated,
		strings.Join(parts, " | "))
}

func summarizeSignalingValue(path string, value any, depth int, parts *[]string) {
	if len(*parts) >= maxSignalSummaryParts {
		return
	}
	if isSensitiveSignalPath(path) {
		*parts = append(*parts, fmt.Sprintf("%s=<redacted>", path))
		return
	}
	if depth > maxSignalSummaryDepth {
		*parts = append(*parts, fmt.Sprintf("%s=<max-depth>", path))
		return
	}

	switch typed := value.(type) {
	case map[string]any:
		keys := sortedMapKeys(typed)
		*parts = append(*parts, fmt.Sprintf("%s.keys=%v", path, keys))
		for _, key := range keys {
			summarizeSignalingValue(path+"."+key, typed[key], depth+1, parts)
			if len(*parts) >= maxSignalSummaryParts {
				return
			}
		}
	case []any:
		*parts = append(*parts, fmt.Sprintf("%s.len=%d", path, len(typed)))
		limit := len(typed)
		if limit > maxSignalSummaryArrayLen {
			limit = maxSignalSummaryArrayLen
		}
		for i := 0; i < limit; i++ {
			summarizeSignalingValue(fmt.Sprintf("%s[%d]", path, i), typed[i], depth+1, parts)
			if len(*parts) >= maxSignalSummaryParts {
				return
			}
		}
	case []map[string]any:
		*parts = append(*parts, fmt.Sprintf("%s.len=%d", path, len(typed)))
		limit := len(typed)
		if limit > maxSignalSummaryArrayLen {
			limit = maxSignalSummaryArrayLen
		}
		for i := 0; i < limit; i++ {
			summarizeSignalingValue(fmt.Sprintf("%s[%d]", path, i), typed[i], depth+1, parts)
			if len(*parts) >= maxSignalSummaryParts {
				return
			}
		}
	case string:
		if isSafeLiteralSignalPath(path) {
			*parts = append(*parts, fmt.Sprintf("%s=%q", path, typed))
			return
		}
		*parts = append(*parts, fmt.Sprintf("%s=%s", path, safeStringDigest(typed)))
	case bool:
		*parts = append(*parts, fmt.Sprintf("%s=%t", path, typed))
	case nil:
		*parts = append(*parts, fmt.Sprintf("%s=null", path))
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		*parts = append(*parts, fmt.Sprintf("%s=%v", path, typed))
	default:
		*parts = append(*parts, fmt.Sprintf("%s=<%T>", path, typed))
	}
}

func isSafeLiteralSignalPath(path string) bool {
	lower := strings.ToLower(path)
	safeSuffixes := []string{
		".mid",
		".transceivermid",
		".limitationreason",
		".kind",
		".participantid",
		".participantId",
		".sourceid",
		".sourceId",
		".streamid",
		".streamId",
		".label",
	}
	for _, suffix := range safeSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func safeStringDigest(value string) string {
	if value == "" {
		return "str(len=0)"
	}
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("str(len=%d,sha256=%s)", len(value), hex.EncodeToString(sum[:4]))
}

func isSensitiveSignalPath(path string) bool {
	lower := strings.ToLower(path)
	sensitiveMarkers := []string{
		"sdp",
		"candidate",
		"credential",
		"token",
		"auth",
		"cookie",
		"fingerprint",
		"password",
		"pwd",
		"secret",
	}
	for _, marker := range sensitiveMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
