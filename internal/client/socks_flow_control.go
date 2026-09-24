package client

// SocksFlowController is the narrowed capability handed to hosts that must
// retune SOCKS flow ceilings while a session is live. *Client implements it.
//
// iOS needs this because a fixed ceiling cannot satisfy both failure modes it
// sees in a Network Extension: a ceiling low enough to stay under the ~50 MB
// jetsam line refuses the ~50 concurrent connections a throughput test opens
// ("test failed to complete"), while a ceiling high enough for those flows lets
// phys_footprint cross the hard limit and the extension is killed. Re-reading
// the ceilings per admission lets a host raise them while memory has headroom
// and shed them as phys_footprint climbs.
type SocksFlowController interface {
	SetSocksFlowCeilings(maxTCP, maxUDP, maxTotal int)
}

// ConfigSocksFlowControlFunc publishes a live SocksFlowController to host code.
type ConfigSocksFlowControlFunc func(SocksFlowController)

// SetSocksFlowCeilings retunes the live client's SOCKS flow ceilings. A value of
// zero or less leaves that field unchanged, so a caller can adjust one dimension
// without restating the others.
//
// tryBeginFlow and maxSocksConns read the ceilings under c.socksMu on every
// admission, so a lower ceiling takes effect on the next accepted flow and never
// disturbs an established one. Bringing a ceiling below the current active count
// is intentional and supported: active flows drain normally while new ones wait
// (see SocksSlotWait) or are refused.
func (c *Client) SetSocksFlowCeilings(maxTCP, maxUDP, maxTotal int) {
	c.socksMu.Lock()
	defer c.socksMu.Unlock()
	if maxTCP > 0 {
		c.resourceProfile.SOCKS.MaxTCP = maxTCP
	}
	if maxUDP > 0 {
		c.resourceProfile.SOCKS.MaxUDP = maxUDP
	}
	if maxTotal > 0 {
		c.resourceProfile.SOCKS.MaxTotal = maxTotal
	}
}
