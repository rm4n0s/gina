package http2

import (
	"encoding/binary"
	"syscall"
	"time"

	"gina"
	ghttp "gina/extensions/http"
	gtls "gina/extensions/tls"
)

const (
	// outHigh is how much output a connection queues before it stops answering
	// requests and sends what it has. It bounds memory per connection.
	outHigh = 128 << 10
	// quantum is how much one stream may send in one round, so a large response
	// does not starve the others.
	quantum = 64 << 10
	// connWindow is the receive window we open on the connection (stream 0).
	connWindow = 1 << 20

	// Cheap frames (RST_STREAM, PING, SETTINGS, PRIORITY, empty DATA) cost one
	// token each; a connection earns ctlRate tokens a second up to ctlBurst, and
	// is closed with ENHANCE_YOUR_CALM when it runs dry. That bounds the work a
	// peer can force without ever doing anything useful (rapid reset, PING and
	// SETTINGS floods).
	ctlBurst = 2000
	ctlRate  = 500
)

type span struct{ off, end int32 }

// fieldSpan locates one regular header inside stream.hbuf: the name is
// hbuf[nameOff:valOff] and the value hbuf[valOff:end].
type fieldSpan struct{ nameOff, valOff, end int32 }

// Pseudo-header bits in stream.seen.
const (
	seenMethod = 1 << iota
	seenScheme
	seenAuthority
	seenPath
	seenProtocol // extended CONNECT (RFC 8441)
)

// stream is one request/response exchange (RFC 9113 §5.1). A stream is in the
// connection's map from its first HEADERS until both directions are finished.
type stream struct {
	id               uint32
	sendWin, recvWin int64 // what the peer lets us send / what we let it send

	hbuf                       []byte // decoded request header names and values, back to back
	hf                         []fieldSpan
	method, scheme, auth, path span
	proto                      span // :protocol of an extended CONNECT
	seen                       uint8
	sawRegular                 bool
	clen                       int64 // declared content-length, -1 if none
	body                       []byte

	gotHeaders bool // the request header block has been processed
	recvEnded  bool // END_STREAM received: half-closed (remote)
	discard    bool // we already answered; ignore the rest of the request
	closed     bool // removed from the connection; sendq may still point at it
	queued     bool // in conn.sendq

	resp    []byte // response body still to send
	respBuf []byte // private copy of resp (the Context's buffer is reused)

	tun       ghttp.Tunnel // extended CONNECT accepted: DATA belongs to this tunnel
	tunClosed bool         // tun.Closed has been called
}

// conn is the state of one connection isolate.
type conn struct {
	fd  gina.FDHandle
	tls *gtls.Conn

	cbuf    []byte // TLS: ciphertext read buffer
	rbuf    []byte // plaintext received, parsed in place
	rlen    int
	out     []byte // plaintext frames queued for the peer
	sendLen int    // bytes of the wire data in flight (0 = no send pending)

	started    bool // the client preface has been read and ours sent
	dead       bool // protocol error: stop reading, flush, close
	closeAfter bool
	notified   bool // TLS close_notify queued
	goneAway   bool // GOAWAY sent
	peerGoAway bool

	hp *hpackDecoder

	// peer settings that govern what we send
	maxFrame int
	initWin  int64
	sendWin  int64 // connection-level window for our DATA
	recvWin  int64 // connection-level window the peer may still use

	streams map[uint32]*stream
	free    []*stream
	spare   []byte    // a recycled header buffer
	sendq   []*stream // streams with response body waiting for flow control
	tuns    []*stream // streams that carry a tunnel
	g       *gina.Ctx // the running turn's context; valid only while a handler runs
	baseAt  uint64    // when the read timeout that is not a tunnel's expires (0 = none)
	lastID  uint32    // highest client stream id seen

	// header block being assembled (HEADERS + CONTINUATION*)
	cont      bool
	hbID      uint32
	hbEnd     bool
	hbStream  *stream // nil: refused or invalid, decode and discard
	hbReject  errCode // answer the stream with this once the block is decoded
	hbFrag    []byte
	cur       *stream // stream the decoder is filling in
	curErr    uint8
	trailers  bool
	listSize  int
	tokens    int
	tokAt     uint64
	hdrs      []ghttp.Header
	cookie    []byte
	hblk      []byte // response header block under construction
	ntmp      []byte
	req       ghttp.Request
	c         ghttp.Context
	onFieldFn func(name, value []byte)
}

// curErr values
const (
	hdrOK uint8 = iota
	hdrMalformed
	hdrTooLarge
)

func (s *Server) connInit(cs *conn, g *gina.Ctx, _ []byte) gina.Effect {
	fd := g.OwnedFD()
	*cs = conn{fd: fd, g: g}
	st := s.sh(g)
	cs.rbuf = st.getBuf(4096)
	cs.out = st.getBuf(1024)[:0]
	cs.hp = newHpackDecoder(4096, s.cfg.MaxHeaderBytes)
	cs.streams = make(map[uint32]*stream)
	cs.maxFrame, cs.initWin = 16384, defaultWindow
	cs.sendWin, cs.recvWin = defaultWindow, defaultWindow
	cs.tokens, cs.tokAt = ctlBurst, g.Now()
	cs.onFieldFn = func(name, value []byte) { s.onField(cs, name, value) }
	if s.tls != nil {
		t, err := gtls.NewServer(s.tls)
		if err != nil {
			g.CloseFD(fd)
			return gina.Crash(gina.FaultInitFailed)
		}
		cs.tls, cs.cbuf = t, st.getBuf(16<<10)
	}
	st.conns.Add(1)
	return s.step(cs, g)
}

func (s *Server) closeConn(cs *conn, g *gina.Ctx) gina.Effect {
	for _, st := range cs.tuns {
		if t := st.tun; t != nil && !st.tunClosed {
			st.tunClosed = true
			t.Closed(g)
		}
	}
	g.CloseFD(cs.fd)
	st, keep := s.sh(g), 4*s.cfg.MaxConns
	st.putBuf(cs.rbuf, keep)
	st.putBuf(cs.out, keep)
	st.putBuf(cs.cbuf, keep)
	*cs = conn{}
	st.conns.Add(-1)
	return gina.Done()
}

func (s *Server) connHandler(cs *conn, g *gina.Ctx, m *gina.Message) gina.Effect {
	cs.g = g
	switch m.Tag {
	case gina.TagIORecv:
		n := gina.PayloadAs[gina.IOResult](m).Result
		if n == -int64(syscall.ETIMEDOUT) {
			if len(cs.tuns) > 0 && (cs.baseAt == 0 || g.Now() < cs.baseAt) { // a tunnel's timer, not the connection's
				s.tickTunnels(cs, g)
				return s.step(cs, g)
			}
			return s.onTimeout(cs, g)
		}
		if n == -int64(syscall.ECANCELED) && len(cs.tuns) > 0 { // interrupted by mail, or shutdown: look at the mailbox
			return gina.Yield()
		}
		if n <= 0 { // EOF, error or cancelled
			return s.closeConn(cs, g)
		}
		if cs.tls != nil {
			if err := cs.tls.Feed(cs.cbuf[:n]); err != nil {
				cs.dead, cs.closeAfter = true, true // a fatal alert (if any) is queued: send it, then close
			}
		} else {
			cs.rlen += int(n)
		}
		return s.step(cs, g)
	case gina.TagIOSend:
		if gina.PayloadAs[gina.IOResult](m).Result < 0 {
			return s.closeConn(cs, g)
		}
		if cs.tls != nil {
			cs.tls.ConsumeOut(cs.sendLen)
		} else {
			cs.out = cs.out[:0]
		}
		cs.sendLen = 0
		return s.step(cs, g)
	case ghttp.TagTunnel:
		if !cs.closeAfter {
			if st := cs.streams[m.Correlation]; st != nil && st.tun != nil && !st.tunClosed {
				st.tun.Push(g, g.Data())
			}
		}
		return gina.Yield() // more mail may be queued: take it all before writing
	case gina.TagYield:
		return s.step(cs, g)
	case gina.TagShutdown:
		if cs.sendLen > 0 || !cs.started {
			return s.closeConn(cs, g)
		}
		s.shutdownTunnels(cs, g)
		s.goAway(cs, errNo)
		cs.closeAfter = true
		return s.flushOrRecv(cs, g)
	}
	return gina.WaitIO()
}

func (s *Server) onTimeout(cs *conn, g *gina.Ctx) gina.Effect {
	if !cs.started || cs.dead {
		return s.closeConn(cs, g)
	}
	s.shutdownTunnels(cs, g)
	s.goAway(cs, errNo)
	cs.closeAfter = true
	return s.flushOrRecv(cs, g)
}

// step runs the connection as far as it will go without the network: decrypt,
// parse and answer every complete frame, queue what flow control allows, then
// send or wait for more input.
func (s *Server) step(cs *conn, g *gina.Ctx) gina.Effect {
	if g.IsShuttingDown() && !cs.goneAway && cs.started {
		s.shutdownTunnels(cs, g)
		s.goAway(cs, errNo)
		cs.closeAfter = true
	}
	for !cs.dead && !cs.closeAfter {
		s.pull(cs)
		progressed, stopped := s.parseFrames(cs, g)
		s.produce(cs, g)
		if cs.dead || stopped || len(cs.out) >= outHigh {
			break
		}
		if !progressed || cs.tls == nil || cs.tls.PlainLen() == 0 {
			break
		}
	}
	return s.flushOrRecv(cs, g)
}

// pull moves decrypted TLS bytes into the frame buffer.
func (s *Server) pull(cs *conn) {
	for cs.tls != nil && cs.tls.PlainLen() > 0 && cs.rlen < len(cs.rbuf) {
		cs.rlen += cs.tls.ReadPlain(cs.rbuf[cs.rlen:])
	}
}

// flushOrRecv sends queued bytes (through TLS when enabled), then closes if the
// connection is finished, else waits for input.
func (s *Server) flushOrRecv(cs *conn, g *gina.Ctx) gina.Effect {
	var wire []byte
	if cs.tls != nil {
		if len(cs.out) > 0 && cs.tls.HandshakeComplete() {
			if cs.tls.Write(cs.out) != nil {
				return s.closeConn(cs, g)
			}
			cs.out = cs.out[:0]
		}
		if cs.closeAfter && !cs.notified && cs.tls.HandshakeComplete() {
			cs.tls.CloseNotify()
			cs.notified = true
		}
		wire = cs.tls.Outgoing()
	} else {
		wire = cs.out
	}
	if len(wire) > 0 {
		cs.sendLen = len(wire)
		g.IOSend(cs.fd, wire, timeoutOr0(s.cfg.WriteTimeout))
		return gina.WaitIO()
	}
	if cs.closeAfter || (cs.peerGoAway && len(cs.streams) == 0) {
		return s.closeConn(cs, g)
	}
	return s.recv(cs, g)
}

// recv stages the next read. The timeout depends on what the connection is
// waiting for: the handshake and preface, a response blocked on the peer's
// window, a request still arriving, or nothing at all.
func (s *Server) recv(cs *conn, g *gina.Ctx) gina.Effect {
	var timeout time.Duration
	switch {
	case !cs.started || (cs.tls != nil && !cs.tls.HandshakeComplete()):
		timeout = s.cfg.ReadTimeout
	case len(cs.sendq) > 0 || cs.tunPending():
		timeout = s.cfg.WriteTimeout
	case cs.incomplete() > 0:
		timeout = s.cfg.ReadTimeout
	case len(cs.tuns) > 0:
		timeout = 0 // only tunnels are open: they keep their own time
	default:
		timeout = s.cfg.IdleTimeout
	}
	eff := timeoutOr0(timeout)
	cs.baseAt = 0
	if eff > 0 {
		cs.baseAt = g.Now() + uint64(eff)
	}
	if len(cs.tuns) > 0 {
		if d, ok := cs.tunIdle(g.Now()); ok {
			if d = max(d, time.Millisecond); eff == 0 || d < eff {
				eff = d
			}
		}
	}
	if cs.tls != nil {
		g.IORecv(cs.fd, cs.cbuf, eff)
	} else {
		g.IORecv(cs.fd, cs.rbuf[cs.rlen:], eff)
	}
	if len(cs.tuns) > 0 {
		return gina.WaitIOOrMessage() // mail for a tunnel must not wait for the peer to speak
	}
	return gina.WaitIO()
}

// incomplete counts streams whose request has not fully arrived.
func (cs *conn) incomplete() int {
	n := 0
	for _, st := range cs.streams {
		if !st.recvEnded && st.tun == nil {
			n++
		}
	}
	return n
}

// ---- frame parsing ----

// parseFrames handles every complete frame in the receive buffer. progressed
// reports that it consumed bytes or grew the buffer; stopped that it left frames
// unread because too much output is queued.
func (s *Server) parseFrames(cs *conn, g *gina.Ctx) (progressed, stopped bool) {
	pos, need := 0, 0
	buf := cs.rbuf[:cs.rlen]
	for !cs.dead {
		if !cs.started {
			n := min(len(buf)-pos, len(clientPreface))
			if string(buf[pos:pos+n]) != clientPreface[:n] { // not HTTP/2: say nothing, hang up
				cs.dead, cs.closeAfter = true, true
				break
			}
			if n < len(clientPreface) {
				break
			}
			pos += n
			progressed, cs.started = true, true
			s.writeServerPreface(cs)
			continue
		}
		h := buf[pos:]
		if len(h) < frameHeaderLen {
			break
		}
		length := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
		if length > s.cfg.MaxFrameSize {
			s.connErr(cs, errFrameSize)
			break
		}
		if len(h) < frameHeaderLen+length {
			need = frameHeaderLen + length
			break
		}
		if len(cs.out) >= outHigh {
			stopped = true
			break
		}
		id := binary.BigEndian.Uint32(h[5:9]) & 0x7fffffff
		s.handleFrame(cs, g, h[3], h[4], id, h[frameHeaderLen:frameHeaderLen+length])
		pos += frameHeaderLen + length
		progressed = true
	}
	if pos > 0 {
		copy(cs.rbuf, cs.rbuf[pos:cs.rlen])
		cs.rlen -= pos
	}
	if need > len(cs.rbuf) { // a frame bigger than the buffer: grow once, to fit
		nb := make([]byte, need)
		copy(nb, cs.rbuf[:cs.rlen])
		cs.rbuf, progressed = nb, true
	}
	return progressed, stopped
}

func (s *Server) writeServerPreface(cs *conn) {
	var at int
	cs.out, at = beginFrame(cs.out, frameSettings, 0, 0)
	cs.out = appendSetting(cs.out, setMaxConcurrentStreams, uint32(s.cfg.MaxConcurrentStreams))
	if s.cfg.InitialWindowSize != defaultWindow {
		cs.out = appendSetting(cs.out, setInitialWindowSize, uint32(s.cfg.InitialWindowSize))
	}
	if s.cfg.MaxFrameSize != 16384 {
		cs.out = appendSetting(cs.out, setMaxFrameSize, uint32(s.cfg.MaxFrameSize))
	}
	cs.out = appendSetting(cs.out, setMaxHeaderListSize, uint32(s.cfg.MaxHeaderBytes))
	if s.cfg.ExtendedConnect {
		cs.out = appendSetting(cs.out, setEnableConnectProtocol, 1)
	}
	endFrame(cs.out, at)
	if connWindow > defaultWindow {
		cs.out = appendWindowUpdate(cs.out, 0, connWindow-defaultWindow)
		cs.recvWin = connWindow
	}
}

// connErr ends the connection with GOAWAY (RFC 9113 §5.4.1).
func (s *Server) connErr(cs *conn, code errCode) {
	s.goAway(cs, code)
	cs.dead, cs.closeAfter = true, true
}

func (s *Server) goAway(cs *conn, code errCode) {
	if !cs.goneAway {
		cs.goneAway = true
		cs.out = appendGoAway(cs.out, cs.lastID, code)
	}
}

// charge takes n tokens for a cheap frame; false means the peer is flooding.
func (s *Server) charge(cs *conn, g *gina.Ctx, n int) bool {
	if now := g.Now(); now > cs.tokAt {
		if add := int((now - cs.tokAt) * ctlRate / uint64(time.Second)); add > 0 {
			cs.tokens, cs.tokAt = min(ctlBurst, cs.tokens+add), now
		}
	}
	cs.tokens -= n
	if cs.tokens < 0 {
		s.connErr(cs, errEnhanceYourCalm)
		return false
	}
	return true
}

// idle reports whether a client stream id has never been opened.
func (cs *conn) idle(id uint32) bool { return id&1 == 0 || id > cs.lastID }

func (s *Server) handleFrame(cs *conn, g *gina.Ctx, typ, flags byte, id uint32, p []byte) {
	if cs.cont && (typ != frameContinuation || id != cs.hbID) {
		s.connErr(cs, errProtocol) // a header block must be contiguous (§4.3)
		return
	}
	switch typ {
	case frameData:
		s.onData(cs, g, flags, id, p)
	case frameHeaders:
		s.onHeaders(cs, g, flags, id, p)
	case framePriority:
		if id == 0 {
			s.connErr(cs, errProtocol)
		} else if len(p) != 5 {
			cs.out = appendRST(cs.out, id, errFrameSize)
		} else {
			s.charge(cs, g, 1) // accepted and ignored
		}
	case frameRSTStream:
		switch {
		case len(p) != 4:
			s.connErr(cs, errFrameSize)
		case id == 0 || cs.idle(id):
			s.connErr(cs, errProtocol)
		default:
			if st := cs.streams[id]; st != nil {
				cs.drop(st)
			}
			s.charge(cs, g, 1)
		}
	case frameSettings:
		s.onSettings(cs, g, flags, id, p)
	case framePushPromise:
		s.connErr(cs, errProtocol) // clients cannot push
	case framePing:
		switch {
		case id != 0:
			s.connErr(cs, errProtocol)
		case len(p) != 8:
			s.connErr(cs, errFrameSize)
		case flags&flagAck == 0:
			if s.charge(cs, g, 1) {
				var at int
				cs.out, at = beginFrame(cs.out, framePing, flagAck, 0)
				cs.out = append(cs.out, p...)
				endFrame(cs.out, at)
			}
		}
	case frameGoAway:
		switch {
		case id != 0:
			s.connErr(cs, errProtocol)
		case len(p) < 8:
			s.connErr(cs, errFrameSize)
		default:
			cs.peerGoAway = true
		}
	case frameWindowUpdate:
		s.onWindowUpdate(cs, id, p)
	case frameContinuation:
		s.onContinuation(cs, g, flags, id, p)
	}
	// unknown frame types are ignored (§4.1)
}

func (s *Server) onSettings(cs *conn, g *gina.Ctx, flags byte, id uint32, p []byte) {
	switch {
	case id != 0:
		s.connErr(cs, errProtocol)
		return
	case flags&flagAck != 0:
		if len(p) != 0 {
			s.connErr(cs, errFrameSize)
		}
		return
	case len(p)%6 != 0:
		s.connErr(cs, errFrameSize)
		return
	}
	if !s.charge(cs, g, 1) {
		return
	}
	for i := 0; i+6 <= len(p); i += 6 {
		k, v := binary.BigEndian.Uint16(p[i:]), binary.BigEndian.Uint32(p[i+2:])
		switch k {
		case setEnablePush:
			if v > 1 {
				s.connErr(cs, errProtocol)
				return
			}
		case setInitialWindowSize:
			if v > maxWindow {
				s.connErr(cs, errFlowControl)
				return
			}
			delta := int64(v) - cs.initWin
			cs.initWin = int64(v)
			for _, st := range cs.streams {
				if st.sendWin += delta; st.sendWin > maxWindow {
					s.connErr(cs, errFlowControl)
					return
				}
			}
		case setMaxFrameSize:
			if v < 16384 || v > 1<<24-1 {
				s.connErr(cs, errProtocol)
				return
			}
			cs.maxFrame = int(v)
		}
		// HEADER_TABLE_SIZE needs no action (we never index response headers);
		// MAX_CONCURRENT_STREAMS limits pushes, which we do not do; unknown
		// settings are ignored.
	}
	var at int
	cs.out, at = beginFrame(cs.out, frameSettings, flagAck, 0)
	endFrame(cs.out, at)
}

func (s *Server) onWindowUpdate(cs *conn, id uint32, p []byte) {
	if len(p) != 4 {
		s.connErr(cs, errFrameSize)
		return
	}
	inc := int64(binary.BigEndian.Uint32(p) & 0x7fffffff)
	if id == 0 {
		if inc == 0 {
			s.connErr(cs, errProtocol)
		} else if cs.sendWin += inc; cs.sendWin > maxWindow {
			s.connErr(cs, errFlowControl)
		}
		return
	}
	if cs.idle(id) {
		s.connErr(cs, errProtocol)
		return
	}
	st := cs.streams[id]
	switch {
	case st == nil: // closed: updates still in flight are harmless
	case inc == 0:
		s.streamErr(cs, st, errProtocol)
	case st.sendWin+inc > maxWindow:
		s.streamErr(cs, st, errFlowControl)
	default:
		st.sendWin += inc
	}
}

// ---- streams ----

func (cs *conn) newStream(id uint32, recvWin int) *stream {
	var st *stream
	if k := len(cs.free); k > 0 {
		st = cs.free[k-1]
		cs.free = cs.free[:k-1]
	} else {
		st = new(stream)
	}
	st.id, st.sendWin, st.recvWin, st.clen = id, cs.initWin, int64(recvWin), -1
	if st.hbuf == nil {
		st.hbuf, cs.spare = cs.spare[:0], nil
	}
	cs.streams[id] = st
	return st
}

// drop removes a stream from the connection; its buffers are recycled once
// nothing refers to it.
func (cs *conn) drop(st *stream) {
	if st.closed {
		return
	}
	st.closed = true
	delete(cs.streams, st.id)
	if st.tun != nil {
		cs.untunnel(st)
	}
	if !st.queued {
		cs.release(st)
	}
}

func (cs *conn) release(st *stream) {
	hbuf, hf, body, rb := st.hbuf[:0], st.hf[:0], st.body[:0], st.respBuf[:0]
	if cap(hbuf) > 16<<10 {
		hbuf = nil
	}
	if cap(body) > 64<<10 {
		body = nil
	}
	if cap(rb) > 64<<10 {
		rb = nil
	}
	*st = stream{hbuf: hbuf, hf: hf, body: body, respBuf: rb}
	if len(cs.free) < 8 {
		cs.free = append(cs.free, st)
	}
}

// streamErr resets one stream (RFC 9113 §5.4.2).
func (s *Server) streamErr(cs *conn, st *stream, code errCode) {
	cs.out = appendRST(cs.out, st.id, code)
	cs.drop(st)
}

// ---- HEADERS and CONTINUATION ----

func (s *Server) onHeaders(cs *conn, g *gina.Ctx, flags byte, id uint32, p []byte) {
	if id == 0 || id&1 == 0 {
		s.connErr(cs, errProtocol)
		return
	}
	if flags&flagPadded != 0 {
		if len(p) < 1 {
			s.connErr(cs, errFrameSize)
			return
		}
		pad := int(p[0])
		p = p[1:]
		if pad > len(p) {
			s.connErr(cs, errProtocol)
			return
		}
		p = p[:len(p)-pad]
	}
	if flags&flagPriority != 0 {
		if len(p) < 5 {
			s.connErr(cs, errFrameSize)
			return
		}
		if binary.BigEndian.Uint32(p)&0x7fffffff == id { // a stream cannot depend on itself
			s.connErr(cs, errProtocol)
			return
		}
		p = p[5:]
	}
	st := cs.streams[id]
	cs.hbReject = 0
	switch {
	case st == nil:
		if id <= cs.lastID {
			s.connErr(cs, errStreamClosed) // a closed stream cannot carry new headers
			return
		}
		cs.lastID = id
		if len(cs.streams) >= s.cfg.MaxConcurrentStreams {
			cs.hbReject = errRefusedStream
		} else {
			st = cs.newStream(id, s.cfg.InitialWindowSize)
		}
	case st.recvEnded:
		cs.hbReject, st = errStreamClosed, nil // half-closed (remote): HEADERS are not allowed
	case !st.gotHeaders:
		s.connErr(cs, errProtocol)
		return
	}
	cs.hbID, cs.hbEnd, cs.hbStream = id, flags&flagEndStream != 0, st
	cs.hbFrag = append(cs.hbFrag[:0], p...)
	if flags&flagEndHeaders != 0 {
		s.finishBlock(cs, g)
	} else {
		cs.cont = true
	}
}

func (s *Server) onContinuation(cs *conn, g *gina.Ctx, flags byte, id uint32, p []byte) {
	if !cs.cont || id != cs.hbID {
		s.connErr(cs, errProtocol)
		return
	}
	if len(cs.hbFrag)+len(p) > 2*s.cfg.MaxHeaderBytes {
		s.connErr(cs, errEnhanceYourCalm)
		return
	}
	cs.hbFrag = append(cs.hbFrag, p...)
	if flags&flagEndHeaders != 0 {
		cs.cont = false
		s.finishBlock(cs, g)
	}
}

// finishBlock decodes a complete header block and acts on it. Every block is
// decoded, even for a stream we are refusing: HPACK state is per connection.
func (s *Server) finishBlock(cs *conn, g *gina.Ctx) {
	cs.cont = false
	st := cs.hbStream
	cs.trailers = st != nil && st.gotHeaders
	cs.cur, cs.curErr, cs.listSize = st, hdrOK, 0
	if cs.trailers {
		cs.cur = nil // trailers are checked for pseudo-headers and then dropped
	}
	err := cs.hp.decode(cs.hbFrag, cs.onFieldFn)
	cs.cur = nil
	if len(cs.hbFrag) > 16<<10 {
		cs.hbFrag = nil
	}
	if err != nil {
		s.connErr(cs, errCompression)
		return
	}
	if st == nil {
		if cs.hbReject != 0 {
			cs.out = appendRST(cs.out, cs.hbID, cs.hbReject)
		}
		return
	}
	if cs.trailers {
		if cs.curErr != hdrOK || !cs.hbEnd {
			s.streamErr(cs, st, errProtocol)
			return
		}
		st.recvEnded = true
		if st.tun != nil { // trailers end a tunnel stream like END_STREAM does
			s.closeTunnel(cs, g, st)
			return
		}
		s.complete(cs, g, st)
		return
	}
	st.gotHeaders = true
	if cs.hbEnd {
		st.recvEnded = true // set first: answering below may finish and recycle the stream
	}
	switch {
	case cs.curErr == hdrMalformed || !st.validRequest():
		s.streamErr(cs, st, errProtocol)
	case cs.curErr == hdrTooLarge:
		s.reject(cs, g, st, 431) // answered; the rest of the request is ignored
	case st.path.end-st.path.off > int32(s.cfg.MaxURIBytes):
		s.reject(cs, g, st, 414)
	case st.clen > int64(s.cfg.MaxBodyBytes):
		s.reject(cs, g, st, 413)
	case cs.hbEnd || st.seen&seenProtocol != 0: // an extended CONNECT is answered now: its stream stays open
		s.complete(cs, g, st)
	}
	// st may have been answered and recycled by now: do not touch it again
}

// ---- DATA ----

func (s *Server) onData(cs *conn, g *gina.Ctx, flags byte, id uint32, p []byte) {
	if id == 0 {
		s.connErr(cs, errProtocol)
		return
	}
	flow := int64(len(p)) // padding counts against flow control too (§6.1)
	if cs.recvWin -= flow; cs.recvWin < 0 {
		s.connErr(cs, errFlowControl)
		return
	}
	data := p
	if flags&flagPadded != 0 {
		if len(p) < 1 || int(p[0]) > len(p)-1 {
			s.connErr(cs, errProtocol)
			return
		}
		data = p[1 : len(p)-int(p[0])]
	}
	st := cs.streams[id]
	switch {
	case cs.idle(id):
		s.connErr(cs, errProtocol)
		return
	case st == nil: // closed (reset, or answered early): drop the data, keep the window moving
		s.creditConn(cs)
		return
	case st.recvEnded:
		s.creditConn(cs)
		s.streamErr(cs, st, errStreamClosed)
		return
	}
	if st.recvWin -= flow; st.recvWin < 0 {
		s.creditConn(cs)
		s.streamErr(cs, st, errFlowControl)
		return
	}
	if len(data) == 0 && flags&flagEndStream == 0 && !s.charge(cs, g, 1) {
		return
	}
	if st.tun != nil { // the stream is a tunnel: its DATA is the protocol's, not a request body
		if len(data) > 0 && !st.tunClosed {
			st.tun.Receive(g, data)
		}
		s.creditConn(cs)
		if flags&flagEndStream != 0 {
			st.recvEnded = true
			s.closeTunnel(cs, g, st) // the peer is done: abnormal for the protocol; we end our side once drained
		} else {
			s.creditStream(cs, st)
		}
		return
	}
	if st.discard { // already answered: ignore the data but keep the windows moving
		s.creditConn(cs)
		s.creditStream(cs, st)
		if flags&flagEndStream != 0 {
			st.recvEnded = true
		}
		return
	}
	if len(st.body)+len(data) > s.cfg.MaxBodyBytes {
		s.creditConn(cs)
		s.reject(cs, g, st, 413) // st may be gone after this
		return
	}
	if st.body == nil && st.clen > 0 {
		st.body = make([]byte, 0, min(st.clen, 64<<10)) // not the declared size: it may be a lie
	}
	st.body = append(st.body, data...)
	s.creditConn(cs)
	if flags&flagEndStream != 0 {
		st.recvEnded = true
		s.complete(cs, g, st)
		return
	}
	s.creditStream(cs, st)
}

// creditConn and creditStream hand window back to the peer once its data has been
// consumed (here: buffered), batching updates to half a window.
func (s *Server) creditConn(cs *conn) {
	if cs.recvWin < connWindow/2 {
		cs.out = appendWindowUpdate(cs.out, 0, uint32(connWindow-cs.recvWin))
		cs.recvWin = connWindow
	}
}

func (s *Server) creditStream(cs *conn, st *stream) {
	if w := int64(s.cfg.InitialWindowSize); st.recvWin < w/2 {
		cs.out = appendWindowUpdate(cs.out, st.id, uint32(w-st.recvWin))
		st.recvWin = w
	}
}
