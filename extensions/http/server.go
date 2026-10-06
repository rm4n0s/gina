package http

import (
	"errors"
	"strconv"
	"syscall"
	"time"

	"gina"
)

// Config configures a Server. Zero values take the documented defaults; for the
// timeouts a negative value disables the timeout.
type Config struct {
	Addr      [4]byte // default 0.0.0.0
	Port      uint16  // 0 = ephemeral (each shard gets its own; read with Port())
	ReusePort bool    // SO_REUSEPORT: every shard (and every Prefork worker) binds the same port
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

	ports     []uint16
	listenErr error
	requests  uint64
	conns     int
	rejected  uint64
	bufs      [][]byte

	dateSec int64
	dateBuf []byte
}

func New(cfg Config, r *Router) *Server {
	cfg.defaults()
	return &Server{cfg: cfg, router: r, lim: limits{cfg.MaxHeaderBytes, cfg.MaxURIBytes, cfg.MaxBodyBytes}}
}

// Port returns the port a shard's listener bound (0 before it started).
func (s *Server) Port(shard int) uint16 { return s.ports[shard] }

// ListenErr returns the last bind/listen error (e.g. EADDRINUSE), if any.
func (s *Server) ListenErr() error { return s.listenErr }

func (s *Server) Requests() uint64 { return s.requests } // completed requests in this process
func (s *Server) Conns() int       { return s.conns }    // open connections in this process
func (s *Server) Rejected() uint64 { return s.rejected } // connections shed because a shard was full

// Install adds the server's isolate types and one listener per shard to spec,
// and sizes the pools it needs. Create the shards first (len(spec.Shards)).
func (s *Server) Install(spec *gina.SystemSpec) error {
	n := len(spec.Shards)
	if n == 0 {
		return errors.New("http: spec has no shards")
	}
	if n > 1 && !s.cfg.ReusePort {
		return errors.New("http: more than one shard needs Config.ReusePort (each shard binds its own listener)")
	}
	s.ports = make([]uint16, n)
	lid, cid := s.cfg.TypeIDBase, s.cfg.TypeIDBase+1
	spec.Types = append(spec.Types,
		gina.RegisterType(lid, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 4}, s.listenerInit, s.listenerHandler),
		gina.RegisterType(cid, gina.TypeOptions{SlotCount: s.cfg.MaxConns, MailboxCapacity: 4, ChunkSize: 64}, s.connInit, s.connHandler),
	)
	spec.PoolSlots = max(spec.PoolSlots, 8*s.cfg.MaxConns, 4096)
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
		s.listenErr = err
		return gina.Crash(gina.FaultInitFailed)
	}
	l.fd = fd
	s.ports[g.ShardID()] = g.LocalPort(fd)
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
				s.rejected++
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
	fd         gina.FDHandle
	rbuf, wbuf []byte
	rlen       int
	scanned    int
	reqStart   uint64 // ns when the current request's first byte arrived; 0 when idle
	requests   int
	closeAfter bool
	req        Request
	hdrs       [64]Header
	c          Context
}

func (s *Server) getBuf(n int) []byte {
	if k := len(s.bufs); k > 0 {
		b := s.bufs[k-1]
		s.bufs = s.bufs[:k-1]
		if cap(b) >= n {
			return b[:n]
		}
	}
	return make([]byte, n)
}

func (s *Server) putBuf(b []byte) {
	if b != nil && cap(b) <= 64<<10 && len(s.bufs) < 4*s.cfg.MaxConns {
		s.bufs = append(s.bufs, b[:cap(b)])
	}
}

func (s *Server) connInit(cs *connState, g *gina.Ctx, _ []byte) gina.Effect {
	cs.fd = g.OwnedFD()
	cs.rbuf = s.getBuf(s.cfg.ReadBufSize)
	cs.wbuf = s.getBuf(1024)[:0]
	s.conns++
	return s.recv(cs, g)
}

func (s *Server) closeConn(cs *connState, g *gina.Ctx) gina.Effect {
	g.CloseFD(cs.fd)
	s.putBuf(cs.rbuf)
	s.putBuf(cs.wbuf)
	cs.rbuf, cs.wbuf = nil, nil
	s.conns--
	return gina.Done()
}

func timeoutOr0(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	return d
}

// recv stages the next read: idle timeout between requests, remaining read
// timeout for a request already in progress (so a slow drip cannot extend it).
func (s *Server) recv(cs *connState, g *gina.Ctx) gina.Effect {
	timeout := timeoutOr0(s.cfg.IdleTimeout)
	if cs.rlen > 0 && s.cfg.ReadTimeout > 0 {
		rem := s.cfg.ReadTimeout - time.Duration(g.Now()-cs.reqStart)
		if rem <= 0 {
			return s.fail(cs, g, 408)
		}
		timeout = rem
	} else if cs.rlen > 0 {
		timeout = 0
	}
	g.IORecv(cs.fd, cs.rbuf[cs.rlen:], timeout)
	return gina.WaitIO()
}

func (s *Server) connHandler(cs *connState, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case gina.TagIORecv:
		n := gina.PayloadAs[gina.IOResult](m).Result
		if n == -int64(syscall.ETIMEDOUT) && cs.rlen > 0 {
			return s.fail(cs, g, 408)
		}
		if n <= 0 { // EOF, error, idle timeout or cancelled
			return s.closeConn(cs, g)
		}
		if cs.rlen == 0 {
			cs.reqStart = g.Now()
		}
		cs.rlen += int(n)
		return s.process(cs, g)
	case gina.TagIOSend:
		if gina.PayloadAs[gina.IOResult](m).Result < 0 || cs.closeAfter || g.IsShuttingDown() {
			return s.closeConn(cs, g)
		}
		return s.process(cs, g)
	case gina.TagShutdown:
		return s.closeConn(cs, g)
	}
	return gina.WaitIO()
}

// process parses buffered bytes: answer a complete request, or ask for more.
func (s *Server) process(cs *connState, g *gina.Ctx) gina.Effect {
	if cs.rlen == 0 {
		cs.reqStart = 0
		return s.recv(cs, g)
	}
	st, consumed, need, status := parseRequest(cs.rbuf[:cs.rlen], cs.scanned, &s.lim, &cs.req, cs.hdrs[:0])
	switch st {
	case psMore:
		if need == 0 { // headers still incomplete: next time resume the terminator search here
			cs.scanned = cs.rlen
		}
		if need > len(cs.rbuf) { // body bigger than the buffer: grow once to the exact size
			cs.rbuf = grow(cs.rbuf, need)
		} else if need == 0 && cs.rlen == len(cs.rbuf) { // headers fill the buffer: double it
			cs.rbuf = grow(cs.rbuf, min(2*len(cs.rbuf), s.cfg.MaxHeaderBytes))
		}
		return s.recv(cs, g)
	case psErr:
		return s.fail(cs, g, status)
	}
	c := &cs.c
	c.reset(g, &cs.req)
	s.dispatch(c)
	cs.requests++
	s.requests++
	cs.closeAfter = !cs.req.KeepAlive || c.close || g.IsShuttingDown() ||
		(s.cfg.MaxRequestsPerConn > 0 && cs.requests >= s.cfg.MaxRequestsPerConn)
	s.buildResponse(cs, cs.req.Method == "HEAD")
	copy(cs.rbuf, cs.rbuf[consumed:cs.rlen]) // keep pipelined bytes, after the response is built
	cs.rlen -= consumed
	cs.scanned = 0
	cs.reqStart = 0
	if cs.rlen > 0 {
		cs.reqStart = g.Now()
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
func (s *Server) dispatch(c *Context) {
	defer func() {
		if r := recover(); r != nil {
			c.status, c.ctype, c.close = 500, "text/plain; charset=utf-8", true
			c.hdr = c.hdr[:0]
			c.body = append(c.body[:0], "internal server error\n"...)
		}
	}()
	s.router.serve(c)
}

// fail answers a protocol error and closes the connection.
func (s *Server) fail(cs *connState, g *gina.Ctx, code int) gina.Effect {
	c := &cs.c
	c.reset(g, &cs.req)
	c.String(code, statusText(code)+"\n")
	cs.closeAfter = true
	cs.req.Method, cs.req.Minor = "GET", 1
	s.buildResponse(cs, false)
	g.IOSend(cs.fd, cs.wbuf, timeoutOr0(s.cfg.WriteTimeout))
	return gina.WaitIO()
}

const timeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func (s *Server) date() []byte {
	if sec := time.Now().Unix(); sec != s.dateSec {
		s.dateSec = sec
		s.dateBuf = time.Unix(sec, 0).UTC().AppendFormat(s.dateBuf[:0], timeFormat)
	}
	return s.dateBuf
}

func (s *Server) buildResponse(cs *connState, head bool) {
	c := &cs.c
	w := cs.wbuf[:0]
	w = append(w, "HTTP/1.1 "...)
	w = strconv.AppendInt(w, int64(c.status), 10)
	w = append(w, ' ')
	w = append(w, statusText(c.status)...)
	w = append(w, "\r\nDate: "...)
	w = append(w, s.date()...)
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
		w = append(w, "Content-Length: "...)
		w = strconv.AppendInt(w, int64(len(c.body)), 10)
		w = append(w, "\r\n"...)
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
