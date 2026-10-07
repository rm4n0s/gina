//go:build linux

package websocket_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	ctls "crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gina"
	ghttp "gina/extensions/http"
	"gina/extensions/http2"
	gtls "gina/extensions/tls"
	ws "gina/extensions/websocket"
)

// ---- servers ----

type proto int

const (
	h1 proto = iota
	h1TLS
	h2c
	h2TLS
)

func (p proto) String() string { return [...]string{"ws/h1", "wss/h1", "ws/h2c", "wss/h2"}[p] }

var allProtos = []proto{h1, h1TLS, h2c, h2TLS}

type server struct {
	t     *testing.T
	proto proto
	sys   *gina.System
	port  uint16
	pool  *x509.CertPool
	ep    *ws.Endpoint
}

func freePort(t testing.TB) uint16 {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return uint16(l.Addr().(*net.TCPAddr).Port)
}

// opts tweaks a server before it starts: extra routes and isolates, config changes.
type opts struct {
	shards     int
	h1cfg      func(*ghttp.Config)
	h2cfg      func(*http2.Config)
	routes     func(*ghttp.Router, *ws.Endpoint)
	spec       func(*gina.SystemSpec) // before the HTTP server is installed
	post       func(*gina.SystemSpec) // after: add isolates and shards the server does not know about
	grace      time.Duration
	noExtended bool // HTTP/2 without ExtendedConnect
}

func startServer(t *testing.T, p proto, wcfg ws.Config, o opts) *server {
	t.Helper()
	if o.shards == 0 {
		o.shards = 1
	}
	ep := ws.New(wcfg)
	r := ghttp.NewRouter()
	r.GET("/ws", ep.Serve)
	r.GET("/plain", func(c *ghttp.Context) { c.String(200, "plain") })
	if o.routes != nil {
		o.routes(r, ep)
	}
	port := freePort(t)
	var tlsCfg *gtls.Config
	var pool *x509.CertPool
	if p == h1TLS || p == h2TLS {
		cert, err := gtls.SelfSigned("localhost", "127.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := x509.ParseCertificate(cert.Certificate[0])
		pool = x509.NewCertPool()
		pool.AddCert(leaf)
		tlsCfg = &gtls.Config{Certificates: []ctls.Certificate{cert}}
	}
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, o.shards)}
	if o.spec != nil {
		o.spec(&spec)
	}
	var err error
	switch p {
	case h1, h1TLS:
		cfg := ghttp.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: o.shards > 1, MaxConns: 128, ConnMailbox: ws.MailboxCapacity, TLS: tlsCfg}
		if o.h1cfg != nil {
			o.h1cfg(&cfg)
		}
		err = ghttp.New(cfg, r).Install(&spec)
	default:
		cfg := http2.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: o.shards > 1, MaxConns: 128, ConnMailbox: ws.MailboxCapacity,
			ExtendedConnect: !o.noExtended, TLS: tlsCfg}
		if o.h2cfg != nil {
			o.h2cfg(&cfg)
		}
		err = http2.New(cfg, r).Install(&spec)
	}
	if err != nil {
		t.Fatal(err)
	}
	if o.post != nil {
		o.post(&spec)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if o.grace == 0 {
		o.grace = 3 * time.Second
	}
	sys.Start(gina.RunOptions{ShutdownGrace: o.grace})
	s := &server{t: t, proto: p, sys: sys, port: port, pool: pool, ep: ep}
	t.Cleanup(func() { sys.Stop(); sys.Close() })
	return s
}

func (s *server) addr() string { return "127.0.0.1:" + strconv.Itoa(int(s.port)) }

func (s *server) tcp() net.Conn {
	s.t.Helper()
	var c net.Conn
	var err error
	if s.proto == h1TLS || s.proto == h2TLS {
		cfg := &ctls.Config{RootCAs: s.pool, ServerName: "localhost"}
		if s.proto == h2TLS {
			cfg.NextProtos = []string{"h2"}
		}
		c, err = ctls.Dial("tcp", s.addr(), cfg)
	} else {
		c, err = net.Dial("tcp", s.addr())
	}
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { c.Close() })
	return c
}

// ---- WebSocket client over any byte stream ----

type client struct {
	t   *testing.T
	rw  io.ReadWriter
	br  *bufio.Reader
	raw net.Conn
	mu  sync.Mutex
}

func newClient(t *testing.T, raw net.Conn, rw io.ReadWriter, br io.Reader) *client {
	return &client{t: t, rw: rw, raw: raw, br: bufio.NewReader(br)}
}

func (c *client) deadline(d time.Duration) { c.raw.SetDeadline(time.Now().Add(d)) }

// writeFrame sends a masked frame.
func (c *client) writeFrame(fin bool, op ws.Opcode, payload []byte) {
	c.t.Helper()
	if err := c.tryWriteFrame(fin, op, payload); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *client) tryWriteFrame(fin bool, op ws.Opcode, payload []byte) error {
	var key [4]byte
	rand.Read(key[:])
	b0 := byte(op)
	if fin {
		b0 |= 0x80
	}
	f := []byte{b0}
	switch n := len(payload); {
	case n < 126:
		f = append(f, 0x80|byte(n))
	case n <= 0xffff:
		f = append(f, 0x80|126, byte(n>>8), byte(n))
	default:
		f = append(f, 0x80|127)
		f = binary.BigEndian.AppendUint64(f, uint64(n))
	}
	f = append(f, key[:]...)
	for i, b := range payload {
		f = append(f, b^key[i&3])
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.rw.Write(f)
	return err
}

// readFrame returns the next server frame.
func (c *client) readFrame() (fin bool, op ws.Opcode, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	fin, op = h[0]&0x80 != 0, ws.Opcode(h[0]&0x0f)
	if h[0]&0x70 != 0 || h[1]&0x80 != 0 {
		err = fmt.Errorf("server frame with rsv/mask bits: %x", h)
		return
	}
	n := uint64(h[1] & 0x7f)
	switch n {
	case 126:
		var e [2]byte
		if _, err = io.ReadFull(c.br, e[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		if _, err = io.ReadFull(c.br, e[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(e[:])
	}
	payload = make([]byte, n)
	_, err = io.ReadFull(c.br, payload)
	return
}

// readMsg returns the next data message (control frames are skipped, pings answered).
func (c *client) readMsg() (ws.Opcode, []byte) {
	c.t.Helper()
	op, p, err := c.tryReadMsg()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return op, p
}

func (c *client) tryReadMsg() (ws.Opcode, []byte, error) {
	var msg []byte
	var mop ws.Opcode
	for {
		fin, op, p, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch op {
		case ws.OpPing:
			c.tryWriteFrame(true, ws.OpPong, p)
		case ws.OpPong:
		case ws.OpClose:
			return ws.OpClose, p, nil
		case ws.OpText, ws.OpBinary:
			mop, msg = op, p
			if fin {
				return mop, msg, nil
			}
		case ws.OpContinuation:
			msg = append(msg, p...)
			if fin {
				return mop, msg, nil
			}
		}
	}
}

func (c *client) expectText(want string) {
	c.t.Helper()
	op, p := c.readMsg()
	if op != ws.OpText || string(p) != want {
		c.t.Fatalf("got op %d %q, want text %q", op, p, want)
	}
}

// expectClose reads until a close frame and returns its code and reason.
func (c *client) expectClose() (uint16, string) {
	c.t.Helper()
	op, p := c.readMsg()
	if op != ws.OpClose {
		c.t.Fatalf("got op %d %q, want close", op, p)
	}
	if len(p) < 2 {
		return 1005, ""
	}
	return binary.BigEndian.Uint16(p), string(p[2:])
}

// expectEOF checks that the stream ends.
func (c *client) expectEOF() {
	c.t.Helper()
	var b [1]byte
	if _, err := c.br.Read(b[:]); err == nil {
		c.t.Fatal("expected the connection to end")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		c.t.Fatal("connection did not end")
	}
}

// ---- dialing ----

type dialOpts struct {
	path     string
	headers  map[string]string // extra or replacement request headers (value "" removes)
	subproto string
}

type handshake struct {
	status int
	header http.Header
}

// dial opens a WebSocket over the server's protocol. A non-101/200 answer comes
// back as handshake.status with a nil client.
func (s *server) dial(d dialOpts) (*client, handshake) {
	s.t.Helper()
	if d.path == "" {
		d.path = "/ws"
	}
	if s.proto == h2c || s.proto == h2TLS {
		return s.dialH2(d)
	}
	raw := s.tcp()
	var keyb [16]byte
	rand.Read(keyb[:])
	key := base64.StdEncoding.EncodeToString(keyb[:])
	hdr := map[string]string{
		"Host": s.addr(), "Upgrade": "websocket", "Connection": "Upgrade",
		"Sec-WebSocket-Key": key, "Sec-WebSocket-Version": "13",
	}
	if d.subproto != "" {
		hdr["Sec-WebSocket-Protocol"] = d.subproto
	}
	for k, v := range d.headers {
		hdr[k] = v
	}
	var req bytes.Buffer
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\n", d.path)
	for k, v := range hdr {
		if v != "" {
			fmt.Fprintf(&req, "%s: %s\r\n", k, v)
		}
	}
	req.WriteString("\r\n")
	raw.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := raw.Write(req.Bytes()); err != nil {
		s.t.Fatal(err)
	}
	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		s.t.Fatalf("handshake response: %v", err)
	}
	hs := handshake{status: resp.StatusCode, header: resp.Header}
	if resp.StatusCode != 101 {
		io.Copy(io.Discard, resp.Body)
		return nil, hs
	}
	if resp.Header.Get("Sec-WebSocket-Accept") != acceptFor(hdr["Sec-WebSocket-Key"]) {
		s.t.Fatalf("bad Sec-WebSocket-Accept %q", resp.Header.Get("Sec-WebSocket-Accept"))
	}
	c := &client{t: s.t, rw: raw, raw: raw, br: br}
	return c, hs
}

func acceptFor(key string) string {
	// RFC 6455 §1.3 sample: dGhlIHNhbXBsZSBub25jZQ== -> s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
	return wsAccept(key)
}

// ---- raw HTTP/2 client: just enough for extended CONNECT ----

type h2conn struct {
	t        *testing.T
	raw      net.Conn
	br       *bufio.Reader
	mu       sync.Mutex
	setting8 bool // the server advertised SETTINGS_ENABLE_CONNECT_PROTOCOL
	sendWin  map[uint32]int64
	connWin  int64
	initWin  int64
	recvInit uint32 // our advertised INITIAL_WINDOW_SIZE
	manual   bool   // do not send WINDOW_UPDATEs for DATA we read (the test grants)
	streams  map[uint32]*h2stream
	goaway   bool
}

func (c *h2conn) writeFrame(typ, flags byte, id uint32, p []byte) error {
	f := []byte{byte(len(p) >> 16), byte(len(p) >> 8), byte(len(p)), typ, flags, byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)}
	f = append(f, p...)
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.raw.Write(f)
	return err
}

func (c *h2conn) readFrame() (typ, flags byte, id uint32, p []byte, err error) {
	var h [9]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	n := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
	typ, flags, id = h[3], h[4], binary.BigEndian.Uint32(h[5:])&0x7fffffff
	p = make([]byte, n)
	_, err = io.ReadFull(c.br, p)
	return
}

func lit(name, value string) []byte { // literal header field without indexing, new name, no Huffman
	b := []byte{0, byte(len(name))}
	b = append(b, name...)
	b = append(b, byte(len(value)))
	return append(b, value...)
}

// statusOf finds :status in a response header block (static-table index or a
// literal with name index 8, which is how the server writes it).
func statusOf(blk []byte) int {
	if len(blk) == 0 {
		return 0
	}
	if b := blk[0]; b >= 0x88 && b <= 0x8e {
		return [...]int{200, 204, 206, 304, 400, 404, 500}[b-0x88]
	}
	if blk[0] == 0x08 && len(blk) >= 5 && blk[1] == 3 {
		n, _ := strconv.Atoi(string(blk[2:5]))
		return n
	}
	return -1
}

// h2stream is the DATA of one stream as an io.ReadWriter.
type h2stream struct {
	c      *h2conn
	id     uint32
	buf    []byte
	eof    bool
	reset  bool
	status int
}

func (s *h2stream) Read(p []byte) (int, error) {
	for len(s.buf) == 0 {
		if s.eof {
			return 0, io.EOF
		}
		if err := s.c.pump(); err != nil {
			return 0, err
		}
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

// pump reads one frame and routes it to its stream.
func (c *h2conn) pump() error {
	typ, flags, id, p, err := c.readFrame()
	if err != nil {
		return err
	}
	st := c.streams[id]
	switch typ {
	case 0: // DATA
		if st != nil {
			st.buf = append(st.buf, p...)
			if flags&1 != 0 {
				st.eof = true
			}
		}
		if len(p) > 0 && !c.manual {
			var u [4]byte
			binary.BigEndian.PutUint32(u[:], uint32(len(p)))
			c.writeFrame(8, 0, 0, u[:])
			if st != nil && !st.eof {
				c.writeFrame(8, 0, id, u[:])
			}
		}
	case 1: // HEADERS
		if st != nil {
			st.status = statusOf(p)
			if flags&1 != 0 {
				st.eof = true
			}
		}
	case 3: // RST_STREAM
		if st != nil {
			st.eof, st.reset = true, true
		}
	case 4: // SETTINGS
		if flags&1 == 0 {
			c.writeFrame(4, 1, 0, nil)
		}
	case 6: // PING
		if flags&1 == 0 {
			c.writeFrame(6, 1, 0, p)
		}
	case 7: // GOAWAY
		c.goaway = true
		for _, st := range c.streams {
			st.eof = true
		}
	case 8: // WINDOW_UPDATE
		inc := int64(binary.BigEndian.Uint32(p) & 0x7fffffff)
		c.mu.Lock()
		if id == 0 {
			c.connWin += inc
		} else {
			c.sendWin[id] += inc
		}
		c.mu.Unlock()
	}
	return nil
}

func (s *h2stream) Write(p []byte) (int, error) {
	c := s.c
	for off := 0; off < len(p); {
		n := min(len(p)-off, 16384)
		if err := c.writeFrame(0, 0, s.id, p[off:off+n]); err != nil {
			return off, err
		}
		off += n
	}
	return len(p), nil
}

// grant gives the server n more bytes of window on the stream and the connection.
func (s *h2stream) grant(n int) {
	var u [4]byte
	binary.BigEndian.PutUint32(u[:], uint32(n))
	s.c.writeFrame(8, 0, 0, u[:])
	s.c.writeFrame(8, 0, s.id, u[:])
}

type h2opts struct {
	initWin uint32 // our SETTINGS_INITIAL_WINDOW_SIZE (default 1 MiB)
	manual  bool
}

// openH2 connects, exchanges SETTINGS, and returns the connection.
func (s *server) openH2(o h2opts) *h2conn {
	s.t.Helper()
	raw := s.tcp()
	raw.SetDeadline(time.Now().Add(10 * time.Second))
	c := &h2conn{t: s.t, raw: raw, br: bufio.NewReader(raw), sendWin: map[uint32]int64{}, streams: map[uint32]*h2stream{}, connWin: 65535, initWin: 65535, manual: o.manual}
	if o.initWin == 0 {
		o.initWin = 1 << 20
	}
	c.recvInit = o.initWin
	var st []byte
	st = binary.BigEndian.AppendUint16(st, 4) // INITIAL_WINDOW_SIZE
	st = binary.BigEndian.AppendUint32(st, o.initWin)
	if _, err := raw.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")); err != nil {
		s.t.Fatal(err)
	}
	c.writeFrame(4, 0, 0, st)
	if !o.manual {
		var u [4]byte
		binary.BigEndian.PutUint32(u[:], 1<<20)
		c.writeFrame(8, 0, 0, u[:]) // connection window
	}
	// read until the server's SETTINGS has arrived
	for {
		typ, flags, _, p, err := c.readFrame()
		if err != nil {
			s.t.Fatalf("h2 preface: %v", err)
		}
		if typ == 4 && flags&1 == 0 {
			for i := 0; i+6 <= len(p); i += 6 {
				if binary.BigEndian.Uint16(p[i:]) == 8 && binary.BigEndian.Uint32(p[i+2:]) == 1 {
					c.setting8 = true
				}
			}
			c.writeFrame(4, 1, 0, nil)
			break
		}
	}
	return c
}

func (s *server) dialH2(d dialOpts) (*client, handshake) {
	c := s.openH2(h2opts{})
	st, hs := c.connect(s, 1, d)
	if st == nil {
		return nil, hs
	}
	return &client{t: s.t, rw: st, raw: c.raw, br: bufio.NewReader(st)}, hs
}

// connect sends the extended CONNECT on stream id and waits for the answer.
func (c *h2conn) connect(s *server, id uint32, d dialOpts) (*h2stream, handshake) {
	s.t.Helper()
	if d.path == "" {
		d.path = "/ws"
	}
	scheme := "http"
	if s.proto == h2TLS {
		scheme = "https"
	}
	hdr := map[string]string{"sec-websocket-version": "13"}
	if d.subproto != "" {
		hdr["sec-websocket-protocol"] = d.subproto
	}
	for k, v := range d.headers {
		hdr[strings.ToLower(k)] = v
	}
	var blk []byte
	blk = append(blk, lit(":method", "CONNECT")...)
	blk = append(blk, lit(":scheme", scheme)...)
	blk = append(blk, lit(":authority", s.addr())...)
	blk = append(blk, lit(":path", d.path)...)
	blk = append(blk, lit(":protocol", "websocket")...)
	for k, v := range hdr {
		if v != "" {
			blk = append(blk, lit(k, v)...)
		}
	}
	if err := c.writeFrame(1, 4, id, blk); err != nil { // HEADERS, END_HEADERS, no END_STREAM
		s.t.Fatal(err)
	}
	st := &h2stream{c: c, id: id}
	c.streams[id] = st
	for st.status == 0 && !st.eof {
		if err := c.pump(); err != nil {
			s.t.Fatalf("h2 connect answer: %v", err)
		}
	}
	hs := handshake{status: st.status}
	if st.status/100 != 2 {
		return nil, hs
	}
	return st, hs
}

func bufioReader(r io.Reader) *bufio.Reader { return bufio.NewReader(r) }
