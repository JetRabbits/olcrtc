package client

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/framing"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
	socksVersion             = 5
	socksAddrIPv4            = 1
	socksAddrDomain          = 3
	socksAddrIPv6            = 4
	socksRepSuccess          = 0
	socksRepHostUnreachable  = 4
	socksCommandConnect      = 1
	socksCommandUDPAssociate = 3
	udpDialCommand           = "udp-dial"
	maxUDPPacketSize         = 65535
)

const (
	// socksNegotiationTimeout bounds everything before the target is known.
	// Until then there is nothing legitimate to wait for, and the listener is
	// allowed to be non-loopback when credentials are configured, so a peer
	// that connects and stays silent must not pin a goroutine and an fd.
	socksNegotiationTimeout = 30 * time.Second

	// maxSocksConns caps concurrent SOCKS clients. Each one costs a
	// goroutine, an fd and a tunnel stream.
	maxSocksConns = 512

	// acceptRetryDelay is the initial backoff after a failed Accept.
	// Retrying immediately turns a temporary fd exhaustion into a hot loop
	// that floods the log.
	acceptRetryDelay        = 10 * time.Millisecond
	maxAcceptRetryDelay     = time.Second
	udpAssociateIdleTimeout = 2 * time.Minute
)

type socksRequest struct {
	command byte
	addr    string
	port    int
}

type socksUDPDatagram struct {
	addr    string
	port    int
	payload []byte
}

func (c *Client) acceptLoop(ctx context.Context, listener net.Listener) {
	delay := time.Duration(0)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			delay = nextAcceptDelay(delay)
			logger.Warnf("Accept error (retry in %s): %v", delay, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		delay = 0
		if !c.registerSocksConn(conn) {
			_ = conn.Close()
			continue
		}
		c.goTracked(func() {
			defer c.unregisterSocksConn(conn)
			c.handleSocks5(ctx, conn)
		})
	}
}

func nextAcceptDelay(current time.Duration) time.Duration {
	if current <= 0 {
		return acceptRetryDelay
	}
	if next := current * 2; next < maxAcceptRetryDelay {
		return next
	}
	return maxAcceptRetryDelay
}

func (c *Client) handleSocks5(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(c.socksHandshakeTimeout()))
	if err := c.socks5Handshake(conn); err != nil {
		return
	}
	req, err := c.readSocks5Request(conn)
	if err != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	const sessionReadyTimeout = 60 * time.Second
	readyCtx, cancel := context.WithTimeout(ctx, sessionReadyTimeout)
	defer cancel()
	for {
		// The ready channel is taken in the same critical section as the
		// state it describes. Sampling it afterwards subscribes to the next
		// generation and misses the signal that just fired, which stalls the
		// request for the full timeout while the tunnel is up.
		session, sessionID, ready := c.sessionSnapshot()
		if session != nil && !session.IsClosed() && sessionID != "" {
			switch req.command {
			case socksCommandConnect:
				if !c.tryBeginFlow("tcp") {
					_, _ = conn.Write(replyHostUnreachable(req.addr))
					return
				}
				defer c.endFlow("tcp")
				c.tunnel(ctx, conn, session, req.addr, req.port)
			case socksCommandUDPAssociate:
				c.udpAssociate(ctx, conn, session)
			}
			return
		}
		select {
		case <-readyCtx.Done():
			_, _ = conn.Write(replyHostUnreachable(req.addr))
			return
		case <-ready:
		}
	}
}

func (c *Client) socks5Handshake(conn net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read socks5 header: %w", err)
	}
	if header[0] != socksVersion {
		return fmt.Errorf("%w: %d", ErrInvalidSOCKSVersion, header[0])
	}
	methods := make([]byte, header[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return fmt.Errorf("read socks5 methods: %w", err)
	}
	if c.socksUser != "" {
		if _, err := conn.Write([]byte{socksVersion, 2}); err != nil {
			return fmt.Errorf("write socks5 auth method: %w", err)
		}
		return c.socks5UserPassAuth(conn)
	}
	if _, err := conn.Write([]byte{socksVersion, 0}); err != nil {
		return fmt.Errorf("write socks5 auth: %w", err)
	}
	return nil
}

func (c *Client) socks5UserPassAuth(conn net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read socks5 auth header: %w", err)
	}
	if header[0] != 1 {
		return fmt.Errorf("%w: expected auth version 1, got %d", ErrInvalidSOCKSVersion, header[0])
	}
	user := make([]byte, header[1])
	if _, err := io.ReadFull(conn, user); err != nil {
		return fmt.Errorf("read socks5 username: %w", err)
	}
	passwordLength := make([]byte, 1)
	if _, err := io.ReadFull(conn, passwordLength); err != nil {
		return fmt.Errorf("read socks5 plen: %w", err)
	}
	password := make([]byte, passwordLength[0])
	if _, err := io.ReadFull(conn, password); err != nil {
		return fmt.Errorf("read socks5 password: %w", err)
	}
	// Both comparisons always run: short-circuiting on the username leaks
	// which half failed through timing.
	userOK := subtle.ConstantTimeCompare(user, []byte(c.socksUser)) == 1
	passOK := subtle.ConstantTimeCompare(password, []byte(c.socksPass)) == 1
	if !userOK || !passOK {
		_, _ = conn.Write([]byte{1, 1})
		return ErrSOCKSAuthFailed
	}
	if _, err := conn.Write([]byte{1, 0}); err != nil {
		return fmt.Errorf("write socks5 auth success: %w", err)
	}
	return nil
}

func (c *Client) socks5Request(conn net.Conn) (string, int, error) {
	req, err := c.readSocks5Request(conn)
	if err != nil {
		return "", 0, err
	}
	if req.command != socksCommandConnect {
		return "", 0, fmt.Errorf("%w: %d", ErrUnsupportedSOCKSCommand, req.command)
	}
	return req.addr, req.port, nil
}

func (c *Client) readSocks5Request(conn net.Conn) (socksRequest, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return socksRequest{}, fmt.Errorf("read socks5 request: %w", err)
	}
	if header[1] != socksCommandConnect && header[1] != socksCommandUDPAssociate {
		return socksRequest{}, fmt.Errorf("%w: %d", ErrUnsupportedSOCKSCommand, header[1])
	}
	addr, err := c.readSocks5Addr(conn, header[3])
	if err != nil {
		return socksRequest{}, err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return socksRequest{}, fmt.Errorf("read socks5 port: %w", err)
	}
	return socksRequest{command: header[1], addr: addr, port: int(binary.BigEndian.Uint16(portBytes))}, nil
}

func (c *Client) readSocks5Addr(conn net.Conn, addrType byte) (string, error) {
	switch addrType {
	case socksAddrIPv4:
		return readIP(conn, net.IPv4len, "ipv4")
	case socksAddrIPv6:
		return readIP(conn, net.IPv6len, "ipv6")
	case socksAddrDomain:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return "", fmt.Errorf("read socks5 domain len: %w", err)
		}
		if length[0] == 0 {
			return "", ErrEmptySOCKSDomain
		}
		buffer := make([]byte, length[0])
		if _, err := io.ReadFull(conn, buffer); err != nil {
			return "", fmt.Errorf("read socks5 domain: %w", err)
		}
		return string(buffer), nil
	default:
		return "", fmt.Errorf("%w: %d", ErrUnsupportedAddressType, addrType)
	}
}

func readIP(conn net.Conn, length int, label string) (string, error) {
	buffer := make([]byte, length)
	if _, err := io.ReadFull(conn, buffer); err != nil {
		return "", fmt.Errorf("read socks5 %s: %w", label, err)
	}
	return net.IP(buffer).String(), nil
}

func socks5Reply(rep byte, target string) []byte {
	addrLen := net.IPv4len
	addrType := byte(socksAddrIPv4)
	if ip := net.ParseIP(target); ip != nil && ip.To4() == nil {
		addrLen = net.IPv6len
		addrType = socksAddrIPv6
	}
	reply := make([]byte, 4+addrLen+2)
	reply[0], reply[1], reply[3] = socksVersion, rep, addrType
	return reply
}

func replySuccess(target string) []byte {
	return socks5Reply(socksRepSuccess, target)
}

func replyHostUnreachable(target string) []byte {
	return socks5Reply(socksRepHostUnreachable, target)
}

func replySuccessAddr(ip net.IP, port int) []byte {
	if ip4 := ip.To4(); ip4 != nil {
		reply := []byte{socksVersion, socksRepSuccess, 0, socksAddrIPv4, ip4[0], ip4[1], ip4[2], ip4[3], 0, 0}
		binary.BigEndian.PutUint16(reply[8:10], uint16(port)) //nolint:gosec // ephemeral UDP listener port
		return reply
	}
	if ip16 := ip.To16(); ip16 != nil {
		reply := make([]byte, 22)
		copy(reply[:4], []byte{socksVersion, socksRepSuccess, 0, socksAddrIPv6})
		copy(reply[4:20], ip16)
		binary.BigEndian.PutUint16(reply[20:22], uint16(port)) //nolint:gosec // ephemeral UDP listener port
		return reply
	}
	return replySuccess("0.0.0.0")
}

func (c *Client) udpAssociate(ctx context.Context, tcpConn net.Conn, sess *smux.Session) {
	udpConn, err := c.listenUDPRelay(tcpConn.LocalAddr())
	if err != nil {
		logger.Warnf("UDP ASSOCIATE listen failed: %v", err)
		_, _ = tcpConn.Write(replyHostUnreachable("0.0.0.0"))
		return
	}
	defer func() { _ = udpConn.Close() }()

	bound := udpConn.LocalAddr().(*net.UDPAddr)
	if !c.tryBeginFlow("udp") {
		_, _ = tcpConn.Write(replyHostUnreachable("0.0.0.0"))
		return
	}
	defer c.endFlow("udp")
	if _, err := tcpConn.Write(replySuccessAddr(bound.IP, bound.Port)); err != nil {
		return
	}

	var (
		streamMu      sync.Mutex
		stream        *smux.Stream
		targetAddr    string
		targetPort    int
		udpClientMu   sync.RWMutex
		udpClientAddr *net.UDPAddr
	)
	closeStream := func() {
		streamMu.Lock()
		if stream != nil {
			_ = stream.Close()
			stream = nil
		}
		streamMu.Unlock()
	}
	defer closeStream()

	done := make(chan struct{})
	defer close(done)
	go func() {
		_, _ = io.Copy(io.Discard, tcpConn)
		_ = udpConn.Close()
		closeStream()
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = udpConn.Close()
			closeStream()
		case <-done:
		}
	}()

	buf := make([]byte, maxUDPPacketSize)
	for {
		_ = udpConn.SetReadDeadline(time.Now().Add(c.udpAssociateIdleTimeout()))
		n, src, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		datagram, err := parseSocks5UDPDatagram(buf[:n])
		if err != nil {
			logger.Debugf("UDP ASSOCIATE parse failed: %v", err)
			continue
		}
		udpClientMu.Lock()
		udpClientAddr = src
		udpClientMu.Unlock()

		streamMu.Lock()
		if stream == nil || targetAddr != datagram.addr || targetPort != datagram.port {
			if stream != nil {
				_ = stream.Close()
			}
			stream, err = sess.OpenStream()
			if err == nil {
				targetAddr, targetPort = datagram.addr, datagram.port
				err = c.sendUDPDialRequest(stream, targetAddr, targetPort)
			}
			if err == nil {
				go c.forwardUDPReplies(udpConn, stream, targetAddr, targetPort, &udpClientMu, &udpClientAddr)
			}
		}
		current := stream
		if err == nil && current != nil {
			err = framing.WriteBytes(current, datagram.payload, maxUDPPacketSize)
		}
		streamMu.Unlock()
		if err != nil {
			logger.Debugf("UDP ASSOCIATE stream write failed: %v", err)
			closeStream()
		}
	}
}

func (c *Client) listenUDPRelay(tcpAddr net.Addr) (*net.UDPConn, error) {
	host := "127.0.0.1"
	if addr, ok := tcpAddr.(*net.TCPAddr); ok && addr.IP != nil && !addr.IP.IsUnspecified() {
		host = addr.IP.String()
	}
	udpAddr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(host, "0"))
	if err != nil {
		return nil, err
	}
	return net.ListenUDP("udp4", udpAddr)
}

func (c *Client) forwardUDPReplies(udpConn *net.UDPConn, stream *smux.Stream, targetAddr string, targetPort int, udpClientMu *sync.RWMutex, udpClientAddr **net.UDPAddr) {
	for {
		packet, err := framing.ReadBytes(stream, maxUDPPacketSize)
		if err != nil {
			return
		}
		out, err := marshalSocks5UDPDatagram(targetAddr, targetPort, packet)
		if err != nil {
			return
		}
		udpClientMu.RLock()
		addr := *udpClientAddr
		udpClientMu.RUnlock()
		if addr != nil {
			_, _ = udpConn.WriteToUDP(out, addr)
		}
	}
}

func parseSocks5UDPDatagram(data []byte) (socksUDPDatagram, error) {
	if len(data) < 4 {
		return socksUDPDatagram{}, io.ErrUnexpectedEOF
	}
	if data[0] != 0 || data[1] != 0 || data[2] != 0 {
		return socksUDPDatagram{}, ErrUnsupportedSOCKSCommand
	}
	addr, off, err := parseSocks5AddrBytes(data, 3)
	if err != nil {
		return socksUDPDatagram{}, err
	}
	if len(data) < off+2 {
		return socksUDPDatagram{}, io.ErrUnexpectedEOF
	}
	return socksUDPDatagram{addr: addr, port: int(binary.BigEndian.Uint16(data[off : off+2])), payload: data[off+2:]}, nil
}

func parseSocks5AddrBytes(data []byte, off int) (string, int, error) {
	if len(data) <= off {
		return "", 0, io.ErrUnexpectedEOF
	}
	switch data[off] {
	case socksAddrIPv4:
		if len(data) < off+1+net.IPv4len {
			return "", 0, io.ErrUnexpectedEOF
		}
		return net.IP(data[off+1 : off+1+net.IPv4len]).String(), off + 1 + net.IPv4len, nil
	case socksAddrDomain:
		if len(data) < off+2 {
			return "", 0, io.ErrUnexpectedEOF
		}
		addrLen := int(data[off+1])
		if len(data) < off+2+addrLen {
			return "", 0, io.ErrUnexpectedEOF
		}
		if addrLen == 0 {
			return "", 0, ErrEmptySOCKSDomain
		}
		return string(data[off+2 : off+2+addrLen]), off + 2 + addrLen, nil
	case socksAddrIPv6:
		if len(data) < off+1+net.IPv6len {
			return "", 0, io.ErrUnexpectedEOF
		}
		return net.IP(data[off+1 : off+1+net.IPv6len]).String(), off + 1 + net.IPv6len, nil
	default:
		return "", 0, fmt.Errorf("%w: %d", ErrUnsupportedAddressType, data[off])
	}
}

func marshalSocks5UDPDatagram(addr string, port int, payload []byte) ([]byte, error) {
	out := make([]byte, 0, 4+len(addr)+2+len(payload))
	out = append(out, 0, 0, 0)
	ip := net.ParseIP(addr)
	if ip4 := ip.To4(); ip4 != nil {
		out = append(out, socksAddrIPv4)
		out = append(out, ip4...)
	} else if ip16 := ip.To16(); ip16 != nil {
		out = append(out, socksAddrIPv6)
		out = append(out, ip16...)
	} else {
		if len(addr) > 255 {
			return nil, fmt.Errorf("udp address too long: %d", len(addr))
		}
		out = append(out, socksAddrDomain, byte(len(addr)))
		out = append(out, addr...)
	}
	var portBuf [2]byte
	binary.BigEndian.PutUint16(portBuf[:], uint16(port)) //nolint:gosec // port is parsed from SOCKS or configured endpoint
	out = append(out, portBuf[:]...)
	out = append(out, payload...)
	return out, nil
}

func (c *Client) socksHandshakeTimeout() time.Duration {
	if c.resourceProfile.SOCKS.HandshakeTimeout > 0 {
		return c.resourceProfile.SOCKS.HandshakeTimeout
	}
	return socksNegotiationTimeout
}

func (c *Client) udpAssociateIdleTimeout() time.Duration {
	if c.resourceProfile.SOCKS.UDPAssociateIdleTimeout > 0 {
		return c.resourceProfile.SOCKS.UDPAssociateIdleTimeout
	}
	return udpAssociateIdleTimeout
}
