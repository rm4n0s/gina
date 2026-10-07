package http

import (
	"errors"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"gina"
	gtls "gina/extensions/tls"
)

// Config configures a Server. Zero values take the documented defaults; for the
// timeouts a negative value disables the timeout.
type Config struct {
	Addr      [4]byte // default 0.0.0.0
	Port      uint16  // 0 = ephemeral (each shard gets its own; read with Port())
	ReusePort bool    // SO_REUSEPORT: every shard binds the same port
	Backlog   int     // default 1024

	MaxConns       int // per shard (default 1024); further connections are accepted and closed
	ReadBufSize    int // initial per-connection buffer (default 4096); grows up to header+body limits
	MaxHeaderBytes int // default 16384
	MaxURIBytes    int // default 8192
	MaxBodyBytes   int // default 1 MiB

	IdleTimeout        time.Duration // keep-alive wait for the next request (default 60s)
	ReadTimeout        time.Duration // whole request, from first byte (default 10s)
	WriteTimeout       time.Duration // default 10s
	MaxRequestsPerConn int           // 0 = unlimited

	// ConnMailbox is the mailbox capacity of a connection isolate (default 4). A
	// connection only gets mail when it streams (SSE) or carries a tunnel
	// (WebSocket): raise it so a burst of pushes between two of its turns is not
	// dropped, and give the System enough PoolSlots to hold the queued messages.
	ConnMailbox int

	// TLS, when set, serves HTTPS: every connection runs a TLS 1.3 handshake first
	// (see gina/extensions/tls for what is and is not supported).
	TLS *gtls.Config

	TypeIDBase gina.TypeID // isolate type ids TypeIDBase and TypeIDBase+1 (default 200)
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
	def(&c.ReadBufSize, 4096)
	def(&c.MaxHeaderBytes, 16384)
	def(&c.MaxURIBytes, 8192)
	def(&c.MaxBodyBytes, 1<<20)
	def(&c.ConnMailbox, 4)
	defd(&c.IdleTimeout, 60*time.Second)
	defd(&c.ReadTimeout, 10*time.Second)
	defd(&c.WriteTimeout, 10*time.Second)
	if c.TypeIDBase == 0 {
		c.TypeIDBase = 200
	}
}

// Server is an HTTP/1.1 server made of Gina isolates. Its state is shared by the
// shards of one process, which is safe because a System is single-threaded.
type Server struct {
	cfg    Config
	router *Router
	lim    limits

	st        []shardState // indexed by shard id; each is touched only by its own shard's thread
	listenErr atomic.Pointer[error]
}

// shardState is everything the server mutates per shard. With one thread per
// shard nothing here is shared, so the buffer pool and date cache need no locks;
// the counters are atomic only so other threads can read them (Requests, Conns).
type shardState struct {
	requests, rejected atomic.Uint64
	conns              atomic.Int64
	port               atomic.Uint32
	bufs               [][]byte
	dateSec            int64
	dateBuf            []byte
	_                  [64]byte // keep neighbouring shards' counters off one cache line
}

func New(cfg Config, r *Router) *Server {
	cfg.defaults()
	return &Server{cfg: cfg, router: r, lim: limits{cfg.MaxHeaderBytes, cfg.MaxURIBytes, cfg.MaxBodyBytes}}
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

// Requests is the number of completed requests (all shards); Conns the open
// connections; Rejected the connections shed because a shard was full.
func (s *Server) Requests() uint64 {
	return sumU(s.st, func(t *shardState) uint64 { return t.requests.Load() })
}
func (s *Server) Conns() int {
	var n int64
	for i := range s.st {
		n += s.st[i].conns.Load()
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

// sh returns the calling shard's state.
func (s *Server) sh(g *gina.Ctx) *shardState { return &s.st[g.ShardID()] }

// Install adds the server's isolate types and one listener per shard to spec,
// and sizes the pools it needs. Create the shards first (len(spec.Shards)).
func (s *Server) Install(spec *gina.SystemSpec) error {
	n := len(spec.Shards)
	if n == 0 {
		return errors.New("http: spec has no shards")
	}
	if s.cfg.TLS != nil {
		if err := s.cfg.TLS.Validate(); err != nil {
			return err
		}
	}
	if n > 1 && !s.cfg.ReusePort {
		return errors.New("http: more than one shard needs Config.ReusePort (each shard binds its own listener)")
	}
	s.st = make([]shardState, n)
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
	fd, err := g.Listen(gina.ListenSpec{Addr: s.cfg.Addr, Port: s.cfg.Port, ReusePort: s.cfg.ReusePort, Backlog: s.cfg.Backlog})
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

// ---- connection isolate ----

type connState struct {
	fd          gina.FDHandle
	rbuf, wbuf  []byte // plaintext: request bytes / response
	cbuf        []byte // TLS only: ciphertext read buffer
	tls         *gtls.Conn
	tlsSent     int // TLS only: bytes of tls.Outgoing() in the send that is in flight
	rlen        int
	scanned     int
	reqStart    uint64 // ns when the current request's first byte arrived; 0 when idle
	requests    int
	closeAfter  bool
	stream      bool        // SSE: the connection only pushes events, it no longer reads requests
	notify      gina.Handle // SSE: told (TagStreamClosed) when the stream ends
	tun         Tunnel      // upgraded: the connection belongs to this protocol
	tunOpen     bool        // tun.Open has been called (Closed is owed)
	tunShut     bool        // tun.Shutdown has been called
	tunNotified bool        // TLS close_notify queued
	tunSent     int         // plaintext tunnel only: bytes of tun.Outgoing() in the send that is in flight
	req         Request
	hdrs        [64]Header
	c           Context
}

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

func (s *Server) connInit(cs *connState, g *gina.Ctx, _ []byte) gina.Effect {
	cs.fd = g.OwnedFD()
	st := s.sh(g)
	cs.rbuf = st.getBuf(s.cfg.ReadBufSize)
	cs.wbuf = st.getBuf(1024)[:0]
	if s.cfg.TLS != nil {
		t, err := gtls.NewServer(s.cfg.TLS)
		if err != nil {
			g.CloseFD(cs.fd)
			return gina.Crash(gina.FaultInitFailed)
		}
		cs.tls, cs.cbuf = t, st.getBuf(16<<10)
	}
	st.conns.Add(1)
	return s.recv(cs, g)
}

func (s *Server) closeConn(cs *connState, g *gina.Ctx) gina.Effect {
	if cs.tun != nil && cs.tunOpen {
		cs.tunOpen = false
		cs.tun.Closed(g)
	}
	cs.tun = nil
	if cs.stream && cs.notify != 0 {
		self := g.Self()
		gina.Send(g, cs.notify, TagStreamClosed, &self)
	}
	g.CloseFD(cs.fd)
	st, keep := s.sh(g), 4*s.cfg.MaxConns
	st.putBuf(cs.rbuf, keep)
	st.putBuf(cs.wbuf, keep)
	st.putBuf(cs.cbuf, keep)
	cs.rbuf, cs.wbuf, cs.cbuf, cs.tls = nil, nil, nil, nil
	st.conns.Add(-1)
	return gina.Done()
}

func timeoutOr0(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

// recv stages the next read: the handshake deadline while a TLS handshake is in
// progress, the idle timeout between requests, and the remaining read timeout
// for a request already in progress (so a slow drip cannot extend it).
func (s *Server) recv(cs *connState, g *gina.Ctx) gina.Effect {
	timeout := timeoutOr0(s.cfg.IdleTimeout)
	switch {
	case cs.tls != nil && !cs.tls.HandshakeComplete():
		timeout = timeoutOr0(s.cfg.ReadTimeout)
	case cs.rlen > 0 && s.cfg.ReadTimeout > 0:
		rem := s.cfg.ReadTimeout - time.Duration(g.Now()-cs.reqStart)
		if rem <= 0 {
			return s.fail(cs, g, 408)
		}
		timeout = rem
	case cs.rlen > 0:
		timeout = 0
	}
	if cs.tls != nil {
		g.IORecv(cs.fd, cs.cbuf, timeout)
	} else {
		g.IORecv(cs.fd, cs.rbuf[cs.rlen:], timeout)
	}
	return gina.WaitIO()
}

func (s *Server) connHandler(cs *connState, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case gina.TagIORecv:
		n := gina.PayloadAs[gina.IOResult](m).Result
		if cs.tun != nil {
			return s.tunRecv(cs, g, n)
		}
		if n == -int64(syscall.ETIMEDOUT) && cs.rlen > 0 {
			return s.fail(cs, g, 408)
		}
		if n <= 0 { // EOF, error, idle timeout or cancelled
			return s.closeConn(cs, g)
		}
		if cs.tls != nil {
			if err := cs.tls.Feed(cs.cbuf[:n]); err != nil {
				cs.closeAfter = true // a fatal alert (if any) is queued: send it, then close
				return s.flush(cs, g)
			}
			if cs.tls.OutLen() > 0 { // handshake flight or KeyUpdate answer
				return s.flush(cs, g)
			}
			return s.process(cs, g)
		}
		if cs.rlen == 0 {
			cs.reqStart = g.Now()
		}
		cs.rlen += int(n)
		return s.process(cs, g)
	case gina.TagIOSend:
		if gina.PayloadAs[gina.IOResult](m).Result < 0 {
			return s.closeConn(cs, g)
		}
		if cs.tls != nil {
			cs.tls.ConsumeOut(cs.tlsSent)
			cs.tlsSent = 0
			if cs.tls.OutLen() > 0 && cs.tun == nil {
				return s.flush(cs, g)
			}
		} else if cs.tunSent > 0 {
			cs.tun.Sent(cs.tunSent)
			cs.tunSent = 0
		}
		if cs.tun != nil {
			return s.tunFlush(cs, g)
		}
		if cs.closeAfter || g.IsShuttingDown() {
			return s.closeConn(cs, g)
		}
		if cs.stream {
			return gina.WaitMessage() // sent; sleep until the next event
		}
		return s.process(cs, g)
	case TagEvent:
		if !cs.stream || cs.closeAfter {
			return gina.WaitMessage()
		}
		cs.wbuf = appendEvent(cs.wbuf[:0], m.Payload[:m.PayloadSize])
		return s.sendResponse(cs, g)
	case TagTunnel:
		if cs.tun == nil {
			return gina.WaitMessage() // not (or no longer) a tunnel: nobody to give it to
		}
		if cs.tunOpen && !cs.closeAfter && m.PayloadSize >= 4 {
			cs.tun.Push(g, m.Payload[4:m.PayloadSize])
		}
		return gina.Yield() // more mail may be queued: take it all before writing
	case gina.TagYield:
		if cs.tun != nil {
			return s.tunFlush(cs, g)
		}
		return gina.WaitMessage()
	case gina.TagShutdown:
		if cs.tun != nil && cs.tunOpen && !cs.tunShut {
			cs.tunShut, cs.closeAfter = true, true
			cs.tun.Shutdown(g)
			return s.tunFlush(cs, g)
		}
		return s.closeConn(cs, g)
	}
	if cs.tun != nil {
		return s.tunFlush(cs, g)
	}
	return gina.WaitIO()
}

// flush sends whatever TLS has queued; with nothing queued it carries on.
func (s *Server) flush(cs *connState, g *gina.Ctx) gina.Effect {
	out := cs.tls.Outgoing()
	if len(out) == 0 {
		if cs.closeAfter {
			return s.closeConn(cs, g)
		}
		return s.process(cs, g)
	}
	cs.tlsSent = len(out)
	g.IOSend(cs.fd, out, timeoutOr0(s.cfg.WriteTimeout))
	return gina.WaitIO()
}

// pull moves decrypted TLS bytes into the request buffer.
func (s *Server) pull(cs *connState, g *gina.Ctx) {
	for cs.tls != nil && cs.tls.PlainLen() > 0 && cs.rlen < len(cs.rbuf) {
		if cs.rlen == 0 {
			cs.reqStart = g.Now()
		}
		cs.rlen += cs.tls.ReadPlain(cs.rbuf[cs.rlen:])
	}
}

// process parses buffered bytes: answer a complete request, or ask for more.
func (s *Server) process(cs *connState, g *gina.Ctx) gina.Effect {
	var st parseState
	var consumed, need, status int
	for {
		s.pull(cs, g)
		if cs.rlen == 0 {
			cs.reqStart = 0
			if cs.tls != nil && cs.tls.PeerClosed() {
				return s.closeConn(cs, g)
			}
			return s.recv(cs, g)
		}
		st, consumed, need, status = parseRequest(cs.rbuf[:cs.rlen], cs.scanned, &s.lim, &cs.req, cs.hdrs[:0])
		if st != psMore {
			break
		}
		if need == 0 { // headers still incomplete: next time resume the terminator search here
			cs.scanned = cs.rlen
		}
		grew := false
		if need > len(cs.rbuf) { // body bigger than the buffer: grow once to the exact size
			cs.rbuf, grew = grow(cs.rbuf, need), true
		} else if need == 0 && cs.rlen == len(cs.rbuf) { // headers fill the buffer: double it
			cs.rbuf, grew = grow(cs.rbuf, min(2*len(cs.rbuf), s.cfg.MaxHeaderBytes)), true
		}
		if grew && cs.tls != nil && cs.tls.PlainLen() > 0 {
			continue // more decrypted bytes are already waiting
		}
		return s.recv(cs, g)
	}
	if st == psErr {
		return s.fail(cs, g, status)
	}
	c := &cs.c
	c.reset(g, &cs.req)
	c.tls = cs.tls
	s.dispatch(c)
	cs.requests++
	ss := s.sh(g)
	ss.requests.Add(1)
	cs.closeAfter = !cs.req.KeepAlive || c.close || g.IsShuttingDown() ||
		(s.cfg.MaxRequestsPerConn > 0 && cs.requests >= s.cfg.MaxRequestsPerConn)
	if c.stream {
		cs.stream, cs.notify, cs.closeAfter = true, c.notify, g.IsShuttingDown()
	}
	if c.tunnel != nil && c.status == 101 { // protocol switch: the connection now belongs to the tunnel
		cs.tun, cs.closeAfter = c.tunnel, g.IsShuttingDown()
	}
	c.tunnel = nil
	s.buildResponse(cs, cs.req.Method == "HEAD", ss)
	copy(cs.rbuf, cs.rbuf[consumed:cs.rlen]) // keep pipelined bytes, after the response is built
	cs.rlen -= consumed
	cs.scanned = 0
	cs.reqStart = 0
	if cs.rlen > 0 {
		cs.reqStart = g.Now()
	}
	return s.sendResponse(cs, g)
}

// sendResponse sends cs.wbuf, through TLS when enabled.
func (s *Server) sendResponse(cs *connState, g *gina.Ctx) gina.Effect {
	if cs.tls != nil {
		if err := cs.tls.Write(cs.wbuf); err != nil {
			return s.closeConn(cs, g)
		}
		if cs.closeAfter {
			cs.tls.CloseNotify()
		}
		return s.flush(cs, g)
	}
	g.IOSend(cs.fd, cs.wbuf, timeoutOr0(s.cfg.WriteTimeout))
	return gina.WaitIO()
}

func grow(b []byte, n int) []byte {
	nb := make([]byte, n)
	copy(nb, b)
	return nb
}

// dispatch runs the router; a panicking handler becomes a 500 and the
// connection survives (closed after the response).
func (s *Server) dispatch(c *Context) { s.router.Dispatch(c) }

// fail answers a protocol error and closes the connection.
func (s *Server) fail(cs *connState, g *gina.Ctx, code int) gina.Effect {
	c := &cs.c
	c.reset(g, &cs.req)
	c.String(code, statusText(code)+"\n")
	cs.closeAfter = true
	cs.req.Method, cs.req.Minor = "GET", 1
	s.buildResponse(cs, false, s.sh(g))
	return s.sendResponse(cs, g)
}

const timeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func (st *shardState) date() []byte {
	if sec := time.Now().Unix(); sec != st.dateSec {
		st.dateSec = sec
		st.dateBuf = time.Unix(sec, 0).UTC().AppendFormat(st.dateBuf[:0], timeFormat)
	}
	return st.dateBuf
}

func (s *Server) buildResponse(cs *connState, head bool, st *shardState) {
	c := &cs.c
	w := cs.wbuf[:0]
	w = append(w, "HTTP/1.1 "...)
	w = strconv.AppendInt(w, int64(c.status), 10)
	w = append(w, ' ')
	w = append(w, statusText(c.status)...)
	w = append(w, "\r\nDate: "...)
	w = append(w, st.date()...)
	w = append(w, "\r\nServer: gina\r\n"...)
	allowed := bodyAllowed(c.status)
	if allowed {
		if len(c.body) > 0 || c.ctype != "" {
			ct := c.ctype
			if ct == "" {
				ct = "text/plain; charset=utf-8"
			}
			w = append(w, "Content-Type: "...)
			w = append(w, ct...)
			w = append(w, "\r\n"...)
		}
		if c.stream { // open-ended: the body is delimited by the connection closing
			w = append(w, "Cache-Control: no-cache\r\n"...)
		} else {
			w = append(w, "Content-Length: "...)
			w = strconv.AppendInt(w, int64(len(c.body)), 10)
			w = append(w, "\r\n"...)
		}
	}
	w = append(w, c.hdr...)
	if cs.closeAfter {
		w = append(w, "Connection: close\r\n"...)
	} else if cs.req.Minor == 0 {
		w = append(w, "Connection: keep-alive\r\n"...)
	}
	w = append(w, "\r\n"...)
	if allowed && !head {
		w = append(w, c.body...)
	}
	cs.wbuf = w
}

// ---- tunnel mode ----
//
// After a 101 the connection is a byte stream between the peer and cs.tun. It
// still works half duplex, one operation in flight: write what the tunnel has
// queued, else read, and a read parks with WaitIOOrMessage so that messages from
// other isolates (TagTunnel) interrupt it.

// tunRecv handles the completion of a tunnel read.
func (s *Server) tunRecv(cs *connState, g *gina.Ctx, n int64) gina.Effect {
	switch {
	case n == -int64(syscall.ECANCELED): // interrupted by mail, or shutdown: look at the mailbox
		return gina.Yield()
	case n == -int64(syscall.ETIMEDOUT):
		cs.tun.Tick(g)
		return s.tunFlush(cs, g)
	case n <= 0:
		return s.closeConn(cs, g)
	}
	if cs.tls != nil {
		if err := cs.tls.Feed(cs.cbuf[:n]); err != nil {
			return s.closeConn(cs, g) // the alert, if any, is not worth a write: the tunnel is gone
		}
	} else {
		cs.rlen = int(n)
	}
	return s.tunStep(cs, g)
}

// tunStep feeds buffered plaintext to the tunnel, then writes or reads.
func (s *Server) tunStep(cs *connState, g *gina.Ctx) gina.Effect {
	if !cs.tunOpen {
		return s.tunFlush(cs, g)
	}
	for {
		s.pull(cs, g)
		if cs.rlen == 0 {
			break
		}
		n := cs.rlen
		cs.rlen = 0
		cs.tun.Receive(g, cs.rbuf[:n])
	}
	if cs.tls != nil && cs.tls.PeerClosed() {
		return s.closeConn(cs, g)
	}
	return s.tunFlush(cs, g)
}

// tunFlush writes what is queued (the tunnel's output, and any TLS records), and
// when nothing is left either closes, or waits for the peer or for mail.
func (s *Server) tunFlush(cs *connState, g *gina.Ctx) gina.Effect {
	t := cs.tun
	if cs.tls != nil {
		if out := t.Outgoing(); len(out) > 0 && cs.tunOpen {
			if cs.tls.Write(out) != nil {
				return s.closeConn(cs, g)
			}
			t.Sent(len(out))
		}
		if wire := cs.tls.Outgoing(); len(wire) > 0 {
			cs.tlsSent = len(wire)
			g.IOSend(cs.fd, wire, timeoutOr0(s.cfg.WriteTimeout))
			return gina.WaitIO()
		}
	} else if out := t.Outgoing(); len(out) > 0 && cs.tunOpen {
		cs.tunSent = len(out)
		g.IOSend(cs.fd, out, timeoutOr0(s.cfg.WriteTimeout))
		return gina.WaitIO()
	}
	if !cs.tunOpen { // the 101 has gone out: start the tunnel, then deal with what it queued and what the peer already sent
		cs.tunOpen = true
		t.Open(g, 0)
		return s.tunStep(cs, g)
	}
	if t.Done() || cs.closeAfter || g.IsShuttingDown() {
		if cs.tls != nil && !cs.tunNotified {
			cs.tunNotified = true
			cs.tls.CloseNotify()
			if cs.tls.OutLen() > 0 {
				return s.tunFlush(cs, g)
			}
		}
		return s.closeConn(cs, g)
	}
	d, ok := t.Idle(g.Now())
	var timeout time.Duration
	if ok {
		timeout = max(d, time.Millisecond)
	}
	if cs.tls != nil {
		g.IORecv(cs.fd, cs.cbuf, timeout)
	} else {
		g.IORecv(cs.fd, cs.rbuf, timeout)
	}
	return gina.WaitIOOrMessage()
}
