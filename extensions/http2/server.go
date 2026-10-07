// Package http2 is an HTTP/2 server for Gina, the counterpart of extensions/http:
// one listener isolate per shard (SO_REUSEPORT capable) and one isolate per
// connection, no goroutines of its own and no net/http. It serves the same
// handlers: routes are an http.Router, and a handler receives an http.Context
// and cannot tell which protocol carried the request.
//
// Two ways to run it:
//
//   - With Config.TLS set it speaks h2 over TLS 1.3 (extensions/tls), negotiating
//     "h2" through ALPN. This is what browsers and curl use.
//   - Without TLS it speaks h2c with prior knowledge: the client opens the
//     connection by sending the HTTP/2 preface directly. The h2c Upgrade
//     mechanism of RFC 7540 §3.2 was removed from the protocol and is not supported.
//
// By default a port serves HTTP/2 only: a client that offers no "h2" ALPN
// protocol fails the handshake, and a plain-text connection that does not start
// with the preface is closed. Set Config.HTTP1Fallback to serve HTTP/1.1 on the
// same port (ALPN, or the first bytes without TLS, decide).
//
// Implemented: all frame types and flow control in both directions, HPACK with
// Huffman strings, SETTINGS negotiation, PING, GOAWAY, RST_STREAM, CONTINUATION
// and padding, request bodies (buffered, up to Config.MaxBodyBytes), concurrent
// streams, and the validity rules of RFC 9113 §8 (pseudo-header order, forbidden
// connection-specific fields, content-length consistency). Defences: stream and
// header limits, a rate limit on cheap control frames (rapid reset, PING and
// SETTINGS floods), and read/write/idle timeouts.
//
// Not implemented: server push, prioritisation (PRIORITY is accepted and
// ignored), plain CONNECT, Server-Sent Events (a handler that calls
// Context.EventStream gets a 501), trailers (accepted and dropped) and a dynamic
// table on the response side (response headers are always sent as literals).
//
// Extended CONNECT (RFC 8441) is supported when Config.ExtendedConnect is set: a
// handler may hand such a stream to an http.Tunnel, which then sends and receives
// the stream's DATA with flow control, and other isolates may push to it. That is
// how extensions/websocket runs over HTTP/2.
//
// Each connection is half duplex: the isolate reads, answers everything it has
// read, writes, then reads again. Handlers run synchronously inside that turn,
// as in extensions/http.
package http2

import (
	"errors"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// Config configures a Server. Zero values take the documented defaults; for the
// timeouts a negative value disables the timeout.
type Config struct {
	Addr      [4]byte    // default 0.0.0.0
	IP        netip.Addr // when valid, overrides Addr: an IPv6 address (or "::", dual-stack) listens on IPv6
	Port      uint16     // 0 = ephemeral (each shard gets its own; read with Port())
	ReusePort bool       // SO_REUSEPORT: every shard binds the same port
	Backlog   int        // default 1024

	MaxConns             int // per shard (default 1024); further connections are accepted and closed
	MaxConcurrentStreams int // per connection (default 100); more are refused with REFUSED_STREAM
	MaxFrameSize         int // largest frame accepted, 16384..16777215 (default 16384)
	InitialWindowSize    int // per-stream receive window, at least 65535 (default 262144)
	MaxHeaderBytes       int // decoded size of one header list, RFC 9113's accounting (default 16384)
	MaxURIBytes          int // default 8192
	MaxBodyBytes         int // per request (default 1 MiB); more is answered 413

	// ExtendedConnect advertises SETTINGS_ENABLE_CONNECT_PROTOCOL (RFC 8441) and
	// accepts CONNECT requests that carry a :protocol pseudo-header. A handler sees
	// one as a GET with Request.Protocol set, and may hand the stream to a tunnel
	// (Context.SetTunnel): this is how WebSocket runs over HTTP/2. Without it
	// :protocol is a malformed request, as before.
	ExtendedConnect bool

	// ConnMailbox is the mailbox capacity of a connection isolate (default 4). A
	// connection only gets mail when it carries a tunnel: raise it so a burst of
	// pushes between two of its turns is not dropped, and give the System enough
	// PoolSlots to hold the queued messages.
	ConnMailbox int

	IdleTimeout  time.Duration // a connection with no streams (default 120s)
	ReadTimeout  time.Duration // TLS handshake, preface, and a stalled request in flight (default 10s)
	WriteTimeout time.Duration // a send, or a response blocked on the peer's flow-control window (default 10s)

	// TLS, when set, serves h2 over TLS 1.3 (see gina/extensions/tls). The config is
	// copied and its ALPN list forced to "h2" (plus "http/1.1" with HTTP1Fallback).
	// Certificates can still be replaced on the original with SetCertificates. Nil
	// serves h2c with prior knowledge.
	TLS *gtls.Config

	// HTTP1Fallback serves HTTP/1.1 on the same port. With TLS, ALPN decides: "h2"
	// gets HTTP/2, "http/1.1" or no ALPN at all (many bots and old clients) gets
	// HTTP/1.1, so "existing clients keep working" needs no second port. Without
	// TLS the first bytes decide: the HTTP/2 preface means h2c, anything else is
	// HTTP/1.1. Handlers, routes and per-route limits are shared; the HTTP/1.1
	// side takes its limits and timeouts from this Config (a ReadTimeout or
	// IdleTimeout left at zero gets the HTTP/1.1 server's default there, not
	// this one's). Costs nothing for an h2 connection beyond one flag test per
	// connection; an HTTP/1.1 connection pays one pooled allocation.
	HTTP1Fallback bool

	TypeIDBase gina.TypeID // isolate type ids TypeIDBase and TypeIDBase+1 (default 210)
}

func (c *Config) defaults() {
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	defd := func(v *time.Duration, d time.Duration) {
		if *v == 0 {
			*v = d
		}
	}
	def(&c.Backlog, 1024)
	def(&c.MaxConns, 1024)
	def(&c.MaxConcurrentStreams, 100)
	def(&c.MaxFrameSize, 16384)
	def(&c.InitialWindowSize, 256<<10)
	def(&c.MaxHeaderBytes, 16384)
	def(&c.MaxURIBytes, 8192)
	def(&c.MaxBodyBytes, 1<<20)
	def(&c.ConnMailbox, 4)
	defd(&c.IdleTimeout, 120*time.Second)
	defd(&c.ReadTimeout, 10*time.Second)
	defd(&c.WriteTimeout, 10*time.Second)
	if c.TypeIDBase == 0 {
		c.TypeIDBase = 210
	}
}

func (c *Config) validate() error {
	switch {
	case c.MaxFrameSize < 16384 || c.MaxFrameSize > 1<<24-1:
		return errors.New("http2: MaxFrameSize must be in 16384..16777215")
	case c.InitialWindowSize < 65535 || c.InitialWindowSize > 1<<31-1:
		return errors.New("http2: InitialWindowSize must be in 65535..2147483647")
	case c.MaxConcurrentStreams < 1:
		return errors.New("http2: MaxConcurrentStreams must be positive")
	}
	return nil
}

// Server is an HTTP/2 server made of Gina isolates.
type Server struct {
	cfg    Config
	router *ghttp.Router
	tls    *gtls.Config  // private copy with ALPN fixed to h2
	h1     *ghttp.Server // HTTP1Fallback: serves the connections that do not speak h2

	st        []shardState // indexed by shard id; each is touched only by its own shard's thread
	listenErr atomic.Pointer[error]
}

// shardState is everything the server mutates per shard. With one thread per
// shard nothing here is shared; the counters are atomic only so other threads can
// read them.
type shardState struct {
	requests, rejected atomic.Uint64
	conns              atomic.Int64
	port               atomic.Uint32
	bufs               [][]byte
	dateSec            int64
	dateBuf            []byte
	_                  [64]byte // keep neighbouring shards' counters off one cache line
}

// New creates a server that answers with r's handlers.
func New(cfg Config, r *ghttp.Router) *Server {
	var h1 *ghttp.Server
	if cfg.HTTP1Fallback { // before defaults: a zero timeout means "the HTTP/1.1 default" there
		h1 = ghttp.New(ghttp.Config{
			MaxConns: cfg.MaxConns, MaxHeaderBytes: cfg.MaxHeaderBytes, MaxURIBytes: cfg.MaxURIBytes, MaxBodyBytes: cfg.MaxBodyBytes,
			IdleTimeout: cfg.IdleTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout, ConnMailbox: cfg.ConnMailbox,
		}, r)
	}
	cfg.defaults()
	return &Server{cfg: cfg, router: r, h1: h1}
}

// Port returns the port a shard's listener bound (0 before it started).
func (s *Server) Port(shard int) uint16 { return uint16(s.st[shard].port.Load()) }

// ListenErr returns the last bind/listen error (e.g. EADDRINUSE), if any.
func (s *Server) ListenErr() error {
	if e := s.listenErr.Load(); e != nil {
		return *e
	}
	return nil
}

func (s *Server) setListenErr(err error) { s.listenErr.Store(&err) }

// Requests is the number of requests answered (all shards); Conns the open
// connections; Rejected the connections shed because a shard was full.
func (s *Server) Requests() uint64 {
	n := sumU(s.st, func(t *shardState) uint64 { return t.requests.Load() })
	if s.h1 != nil {
		n += s.h1.Requests()
	}
	return n
}
func (s *Server) Conns() int {
	var n int64
	for i := range s.st {
		n += s.st[i].conns.Load()
	}
	if s.h1 != nil {
		n += int64(s.h1.Conns())
	}
	return int(n)
}
func (s *Server) Rejected() uint64 {
	return sumU(s.st, func(t *shardState) uint64 { return t.rejected.Load() })
}

func sumU(st []shardState, f func(*shardState) uint64) uint64 {
	var n uint64
	for i := range st {
		n += f(&st[i])
	}
	return n
}

func (s *Server) sh(g *gina.Ctx) *shardState { return &s.st[g.ShardID()] }

// Install adds the server's isolate types and one listener per shard to spec,
// and sizes the pools it needs. Create the shards first (len(spec.Shards)).
func (s *Server) Install(spec *gina.SystemSpec) error {
	n := len(spec.Shards)
	if n == 0 {
		return errors.New("http2: spec has no shards")
	}
	if err := s.cfg.validate(); err != nil {
		return err
	}
	if t := s.cfg.TLS; t != nil {
		protos := []string{"h2"}
		if s.h1 != nil {
			protos = append(protos, "http/1.1")
		}
		for _, p := range t.NextProtos {
			switch p {
			case "h2":
			case "http/1.1":
				if s.h1 == nil {
					return errors.New("http2: TLS.NextProtos lists \"http/1.1\" but Config.HTTP1Fallback is off")
				}
			case gtls.ACMETLS1:
				protos = append(protos, p) // TLS-ALPN-01 validation connections: closed after the handshake
			default:
				return errors.New("http2: TLS.NextProtos may only list \"h2\" and \"acme-tls/1\" (this server speaks no other protocol)")
			}
		}
		cp, err := t.WithNextProtos(protos...) // shares t's certificate store: t.SetCertificates reaches the server
		if err != nil {
			return err
		}
		s.tls = cp
	}
	if n > 1 && !s.cfg.ReusePort {
		return errors.New("http2: more than one shard needs Config.ReusePort (each shard binds its own listener)")
	}
	s.st = make([]shardState, n)
	if s.h1 != nil {
		s.h1.InitEmbedded(n)
	}
	lid, cid := s.cfg.TypeIDBase, s.cfg.TypeIDBase+1
	spec.Types = append(spec.Types,
		gina.RegisterType(lid, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 4}, s.listenerInit, s.listenerHandler),
		gina.RegisterType(cid, gina.TypeOptions{SlotCount: s.cfg.MaxConns, MailboxCapacity: s.cfg.ConnMailbox, ChunkSize: 64}, s.connInit, s.connHandler),
	)
	spec.PoolSlots = max(spec.PoolSlots, 8*s.cfg.MaxConns, s.cfg.MaxConns*s.cfg.ConnMailbox/2, 4096)
	spec.TimerEntries = max(spec.TimerEntries, s.cfg.MaxConns+64)
	spec.MaxFDs = max(spec.MaxFDs, s.cfg.MaxConns+16)
	for i := range spec.Shards {
		spec.Shards[i].Boot = append(spec.Shards[i].Boot, gina.SpawnSpec{Type: lid, Group: gina.GroupRoot})
	}
	return nil
}

// ---- listener isolate ----

type listener struct{ fd gina.FDHandle }

const tagBackoff = gina.TagUserBase

func (s *Server) listenerInit(l *listener, g *gina.Ctx, _ []byte) gina.Effect {
	fd, err := g.Listen(gina.ListenSpec{Addr: s.cfg.Addr, IP: s.cfg.IP, Port: s.cfg.Port, ReusePort: s.cfg.ReusePort, Backlog: s.cfg.Backlog})
	if err != nil {
		s.setListenErr(err)
		return gina.Crash(gina.FaultInitFailed)
	}
	l.fd = fd
	s.sh(g).port.Store(uint32(g.LocalPort(fd)))
	g.IOAccept(fd, 0)
	return gina.WaitIO()
}

func (s *Server) listenerHandler(l *listener, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case gina.TagIOAccept:
		res := gina.PayloadAs[gina.IOResult](m).Result
		if res >= 0 {
			fd := gina.FDHandle(res)
			if g.IsShuttingDown() {
				g.CloseFD(fd)
				return gina.Done()
			}
			_, err := g.Spawn(gina.SpawnSpec{Type: s.cfg.TypeIDBase + 1, Group: gina.GroupNone, Restart: gina.RestartTemporary, HandoffFD: fd})
			if err != gina.SpawnErrNone {
				g.CloseFD(fd) // shard is full: shed the connection
				s.sh(g).rejected.Add(1)
			}
		} else {
			switch syscall.Errno(-res) {
			case syscall.ECANCELED:
				return gina.Done()
			case syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM:
				g.RegisterTimer(10*time.Millisecond, tagBackoff) // out of descriptors: back off, don't spin
				return gina.WaitMessage()
			}
		}
		if g.IsShuttingDown() {
			return gina.Done()
		}
		g.IOAccept(l.fd, 0)
		return gina.WaitIO()
	case tagBackoff:
		g.IOAccept(l.fd, 0)
		return gina.WaitIO()
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// ---- per-shard buffers and the Date header ----

func (st *shardState) getBuf(n int) []byte {
	if k := len(st.bufs); k > 0 {
		b := st.bufs[k-1]
		st.bufs = st.bufs[:k-1]
		if cap(b) >= n {
			return b[:n]
		}
	}
	return make([]byte, n)
}

func (st *shardState) putBuf(b []byte, max int) {
	if b != nil && cap(b) <= 64<<10 && len(st.bufs) < max {
		st.bufs = append(st.bufs, b[:cap(b)])
	}
}

const timeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func (st *shardState) date() []byte {
	if sec := time.Now().Unix(); sec != st.dateSec {
		st.dateSec = sec
		st.dateBuf = time.Unix(sec, 0).UTC().AppendFormat(st.dateBuf[:0], timeFormat)
	}
	return st.dateBuf
}

func timeoutOr0(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}
