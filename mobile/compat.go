package mobile

import (
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

const defaultCompatibilityStopTimeoutMillis = 5000

var (
	singletonMu      sync.Mutex //nolint:gochecknoglobals // gomobile compatibility singleton
	singletonRuntime = New()    //nolint:gochecknoglobals // old mobile API exposed a process singleton
	singletonStats   client.FlowStats
)

// SetProviders preserves the legacy package-level gomobile API.
func SetProviders() {
	client.RegisterDefaults()
}

// SetProtector preserves the legacy process-wide Android socket protector API.
func SetProtector(p SocketProtector) {
	singletonRuntime.SetProtector(p)
}

// SetDebug preserves the legacy package-level debug toggle.
func SetDebug(enabled bool) {
	singletonRuntime.SetDebug(enabled)
	if enabled {
		log.SetFlags(log.Ltime | log.Lshortfile)
		return
	}
	log.SetFlags(log.Ltime)
}

// SetDNS preserves the legacy package-level DNS setter.
func SetDNS(dnsServer string) {
	_ = singletonRuntime.SetDNS(normalizeCompatDNS(dnsServer))
}

// SetSocksListenHost preserves the legacy package-level SOCKS bind host setter.
func SetSocksListenHost(host string) {
	_ = singletonRuntime.SetSocksListenHost(normalizeCompatHost(host))
}

// SetVP8Options preserves the legacy package-level vp8channel setter.
func SetVP8Options(fps, batchSize int) {
	_ = singletonRuntime.SetVP8Options(clampCompat(fps, 120), clampCompat(batchSize, 64))
}

// SetLowMemoryProfile preserves Galactic's old mobile API hook. Upstream no longer
// exposes per-session resource profiles, so apply a conservative process-wide Go
// runtime profile when requested by the iOS NetworkExtension wrapper.
func SetLowMemoryProfile(enabled bool) {
	if !enabled {
		return
	}
	debug.SetMemoryLimit(14 << 20)
	debug.SetGCPercent(15)
}

// Start preserves the legacy singleton start API.
func Start(carrierName, roomID, clientID, keyHex string, socksPort int, socksUser, socksPass string) error {
	return StartWithTransport(carrierName, defaultTransport, roomID, clientID, keyHex, socksPort, socksUser, socksPass)
}

// StartWithTransport preserves the legacy singleton start API expected by galactic_tun.
func StartWithTransport(
	carrierName, transportName, roomID, clientID, keyHex string,
	socksPort int,
	socksUser, socksPass string,
) error {
	singletonMu.Lock()
	defer singletonMu.Unlock()

	if singletonRuntime.IsRunning() {
		return ErrAlreadyRunning
	}
	if err := configureSingleton(carrierName, transportName, roomID, clientID, keyHex, socksPort, socksUser, socksPass); err != nil {
		return err
	}
	singletonStats = client.FlowStats{}
	singletonRuntime.mu.Lock()
	singletonRuntime.defaults.onFlowStats = updateSingletonFlowStats
	singletonRuntime.mu.Unlock()
	return singletonRuntime.Start()
}

func configureSingleton(
	carrierName, transportName, roomID, clientID, keyHex string,
	socksPort int,
	socksUser, socksPass string,
) error {
	if err := singletonRuntime.SetProvider(normalizeCompatProvider(carrierName)); err != nil {
		return err
	}
	if err := singletonRuntime.SetTransport(normalizeCompatTransport(transportName)); err != nil {
		return err
	}
	if err := singletonRuntime.SetRoom(strings.TrimSpace(roomID)); err != nil {
		return err
	}
	singletonRuntime.SetDeviceID(strings.TrimSpace(clientID))
	if err := singletonRuntime.SetKey(strings.TrimSpace(keyHex)); err != nil {
		return err
	}
	if err := singletonRuntime.SetSocksPort(socksPort); err != nil {
		return err
	}
	if err := singletonRuntime.SetSocksCredentials(socksUser, socksPass); err != nil {
		return err
	}
	return nil
}

// WaitReady preserves the legacy singleton readiness wait API.
func WaitReady(timeoutMillis int) error {
	return singletonRuntime.WaitReady(timeoutMillis)
}

// Stop preserves the legacy singleton stop API.
func Stop() error {
	err := singletonRuntime.Stop(defaultCompatibilityStopTimeoutMillis)
	singletonMu.Lock()
	singletonStats = client.FlowStats{}
	singletonMu.Unlock()
	return err
}

// IsRunning reports whether the legacy singleton runtime is active.
func IsRunning() bool {
	return singletonRuntime.IsRunning()
}

// ActiveTCPFlows returns the current legacy singleton client's active TCP SOCKS flows.
func ActiveTCPFlows() int64 {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	return singletonStats.TCP
}

// ActiveUDPFlows returns the current legacy singleton client's active UDP ASSOCIATE flows.
func ActiveUDPFlows() int64 {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	return singletonStats.UDP
}

// ActiveTotalFlows returns the current legacy singleton client's active SOCKS flows.
func ActiveTotalFlows() int64 {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	return singletonStats.Total
}

func updateSingletonFlowStats(stats client.FlowStats) {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	if stats.Seq >= singletonStats.Seq {
		singletonStats = stats
	}
}

func normalizeCompatProvider(provider string) string {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return "none"
	}
	return provider
}

func normalizeCompatTransport(transport string) string {
	switch strings.TrimSpace(transport) {
	case "data", "dc", "datachannel":
		return transportData
	case "sei", "seichannel":
		return transportSEI
	case "video", "videochannel":
		return transportVideo
	case "", "vp8", "vp8channel":
		return transportVP8
	default:
		return transportVP8
	}
}

func normalizeCompatHost(host string) string {
	host = cleanHost(host)
	if host == "" {
		return defaultSOCKSHost
	}
	return host
}

func normalizeCompatDNS(dnsServer string) string {
	dnsServer = strings.TrimSpace(dnsServer)
	if dnsServer == "" {
		return defaultDNSServer
	}
	if _, _, err := net.SplitHostPort(dnsServer); err == nil {
		return dnsServer
	}
	if ip := net.ParseIP(strings.Trim(dnsServer, "[]")); ip != nil {
		return net.JoinHostPort(ip.String(), "53")
	}
	return net.JoinHostPort(dnsServer, "53")
}

func clampCompat(value, maxValue int) int {
	if value < 1 {
		return 1
	}
	if maxValue > 0 && value > maxValue {
		return maxValue
	}
	return value
}

func socksListenAddr(host string, port int) string {
	return net.JoinHostPort(normalizeCompatHost(host), strconv.Itoa(port))
}
