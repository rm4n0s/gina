package websocket

import (
	"encoding/binary"
	"time"
	"unicode/utf8"

	"gina"
)

// parser phases
const (
	phHeader  = iota // collecting the 2..14 byte frame header
	phPayload        // inside a frame's payload
)

// Peer names a WebSocket connection to other isolates, on any shard: the
// connection isolate and, over HTTP/2, the stream within it. It is a plain value
// (padding-free, so it can travel in a message payload) and stays usable after
// the connection ends: pushes to it are then dropped.
type Peer struct {
	Conn   gina.Handle
	Stream uint64 // 0 over HTTP/1.1
}

// Conn is one WebSocket connection. It implements http.Tunnel and lives inside
// its connection isolate, so its methods may be called only from that isolate's
// turns: from the callbacks of Config, which is where applications use it. To
// reach a connection from elsewhere, send to its Peer (Push).
type Conn struct {
	// Data is free for the application, typically set in Config.OnOpen.
	Data any

	e      *Endpoint
	g      *gina.Ctx // the running turn; nil outside callbacks
	now    uint64    // g.Now() at the start of the turn
	self   gina.Handle
	stream uint32
	h2     bool
	sub    string

	out  []byte // queued frames; out[sent:] is waiting to be written
	sent int

	opened, closed bool // Open and Closed have run
	closeSent      bool // our close frame is queued
	closeRecv      bool // the peer's close frame has arrived
	done           bool // the connection is finished once the output drains
	localCode      uint16
	peerCode       uint16
	peerReason     []byte

	// timers (ns, g.Now() clock)
	lastRecv uint64
	pingOut  bool
	pingBy   uint64
	closeBy  uint64

	// frame parser
	ph      int
	hdr     [14]byte
	hlen    int
	op      Opcode // opcode of the frame in progress
	fin     bool
	mask    [4]byte
	mpos    int    // payload bytes of this frame already unmasked
	remain  uint64 // payload bytes still to come
	ctl     [125]byte
	clen    int
	msg     []byte // the data message being assembled from fragments
	msgOp   Opcode
	inMsg   bool
	vpos    int // msg[:vpos] is validated UTF-8
	scratch [2 + 123]byte
}

// Peer returns the connection's address for other isolates. It is valid from
// OnOpen on.
func (c *Conn) Peer() Peer { return Peer{Conn: c.self, Stream: uint64(c.stream)} }

// Gina returns the isolate context of the running turn, for sending messages to
// other isolates from a callback. It is nil outside callbacks.
func (c *Conn) Gina() *gina.Ctx { return c.g }

// Subprotocol is the subprotocol selected during the handshake ("" if none).
func (c *Conn) Subprotocol() string { return c.sub }

// IsHTTP2 reports whether the connection is carried by an HTTP/2 stream rather
// than by a connection of its own.
func (c *Conn) IsHTTP2() bool { return c.h2 }

// Send queues a text or binary message. Text must be valid UTF-8. The message is
// copied; it goes out when the isolate's turn ends.
func (c *Conn) Send(op Opcode, data []byte) error {
	if op != OpText && op != OpBinary {
		return ErrBadOpcode
	}
	if op == OpText && !utf8.Valid(data) {
		return ErrBadMessage
	}
	return c.queue(op, data)
}

// SendText queues a text message.
func (c *Conn) SendText(s string) error {
	if !utf8.ValidString(s) {
		return ErrBadMessage
	}
	return c.queue(OpText, nil, s)
}

// SendBinary queues a binary message.
func (c *Conn) SendBinary(b []byte) error { return c.queue(OpBinary, b) }

// Ping queues a ping (at most 125 bytes of payload). The peer answers with a pong
// (Config.OnPong).
func (c *Conn) Ping(data []byte) error {
	if len(data) > 125 {
		return ErrTooLong
	}
	return c.queue(OpPing, data)
}

// Close starts the closing handshake: it queues a close frame with the status
// code and reason (reason is cut to 123 bytes) and waits up to Config.CloseTimeout
// for the peer's. Nothing can be sent afterwards.
func (c *Conn) Close(code uint16, reason string) {
	if c.closeSent || c.done {
		return
	}
	p := c.scratch[:0]
	if code != CloseNoStatus {
		p = binary.BigEndian.AppendUint16(p, code)
		if len(reason) > 123 {
			reason = reason[:123]
			for len(reason) > 0 && !utf8.ValidString(reason) { // do not cut a rune in half
				reason = reason[:len(reason)-1]
			}
		}
		p = append(p, reason...)
	}
	c.localCode = code
	c.queue(OpClose, p) // control frames are not subject to MaxQueued
	c.closeSent = true
	c.closeBy = c.now + uint64(c.e.closeTimeout)
	if c.closeRecv {
		c.done = true
	}
}

// queue appends one frame. data and s are alternatives (s is used when data is nil).
func (c *Conn) queue(op Opcode, data []byte, s ...string) error {
	n := len(data)
	if data == nil && len(s) > 0 {
		n = len(s[0])
	}
	if c.done || (c.closeSent && op != OpClose) {
		return ErrClosed
	}
	if op < OpClose && len(c.out)-c.sent+n+10 > c.e.maxQueued {
		return ErrQueueFull
	}
	c.out = appendHeader(c.out, op, n)
	if data == nil && len(s) > 0 {
		c.out = append(c.out, s[0]...)
	} else {
		c.out = append(c.out, data...)
	}
	return nil
}

// fail closes the connection after a protocol error: a close frame with code, and
// no further input is read.
func (c *Conn) fail(code uint16, reason string) {
	c.Close(code, reason)
	c.done = true
}

// ---- http.Tunnel ----

// Open implements http.Tunnel.
func (c *Conn) Open(g *gina.Ctx, stream uint32) {
	c.enter(g)
	c.stream, c.self, c.opened = stream, g.Self(), true
	c.lastRecv = c.now
	c.e.conns.Add(1)
	c.callOpen()
	c.leave()
}

// Receive implements http.Tunnel.
func (c *Conn) Receive(g *gina.Ctx, b []byte) {
	c.enter(g)
	c.receive(b)
	c.leave()
}

// Push implements http.Tunnel: a message from Push, Peer.
func (c *Conn) Push(g *gina.Ctx, p []byte) {
	if len(p) == 0 {
		return
	}
	c.enter(g)
	switch op := Opcode(p[0]); op {
	case OpText:
		c.queue(OpText, p[1:])
	case OpBinary:
		c.queue(OpBinary, p[1:])
	case OpPing:
		c.Ping(p[1:])
	case OpClose:
		if len(p) >= 3 {
			c.Close(binary.BigEndian.Uint16(p[1:]), string(p[3:]))
		} else {
			c.Close(CloseNormal, "")
		}
	}
	c.leave()
}

// Outgoing implements http.Tunnel.
func (c *Conn) Outgoing() []byte { return c.out[c.sent:] }

// Sent implements http.Tunnel.
func (c *Conn) Sent(n int) {
	if c.sent += n; c.sent >= len(c.out) {
		c.out, c.sent = c.out[:0], 0
		if cap(c.out) > 64<<10 {
			c.out = nil
		}
	}
}

// Idle implements http.Tunnel: the next ping, pong deadline or close deadline.
func (c *Conn) Idle(now uint64) (time.Duration, bool) {
	var due uint64
	switch {
	case c.done:
		return 0, false
	case c.closeSent:
		due = c.closeBy
	case c.pingOut:
		due = c.pingBy
	case c.e.pingInterval > 0:
		due = c.lastRecv + uint64(c.e.pingInterval)
	default:
		return 0, false
	}
	if due <= now {
		return 0, true
	}
	return time.Duration(due - now), true
}

// Tick implements http.Tunnel.
func (c *Conn) Tick(g *gina.Ctx) {
	c.enter(g)
	c.tick()
	c.leave()
}

// tick acts on whichever deadline has passed: a ping to send, a pong that never
// came, or a close that was never answered.
func (c *Conn) tick() {
	switch {
	case c.done:
	case c.closeSent:
		if c.now >= c.closeBy { // the peer never answered our close
			c.done = true
		}
	case c.pingOut:
		if c.now >= c.pingBy {
			c.fail(CloseGoingAway, "ping timeout")
		}
	case c.e.pingInterval > 0 && c.now >= c.lastRecv+uint64(c.e.pingInterval):
		if c.queue(OpPing, nil) == nil {
			c.pingOut, c.pingBy = true, c.now+uint64(c.e.pongTimeout)
		}
	}
}

// Done implements http.Tunnel.
func (c *Conn) Done() bool { return c.done }

// Shutdown implements http.Tunnel: the server is stopping.
func (c *Conn) Shutdown(g *gina.Ctx) {
	c.enter(g)
	c.Close(CloseGoingAway, "server shutting down")
	c.done = true // do not wait for the answer
	c.leave()
}

// Closed implements http.Tunnel: the transport is gone.
func (c *Conn) Closed(g *gina.Ctx) {
	if c.closed || !c.opened {
		return
	}
	c.enter(g)
	c.closed, c.done = true, true
	c.e.conns.Add(-1)
	code := CloseAbnormal
	switch {
	case c.closeRecv:
		code = c.peerCode
	case c.localCode != 0 && c.closeSent: // we closed (or failed) and the peer went without answering
		code = c.localCode
	}
	c.callClose(code)
	c.out, c.msg, c.peerReason = nil, nil, nil
	c.sent = 0
	c.leave()
}

func (c *Conn) enter(g *gina.Ctx) { c.g, c.now = g, g.Now() }
func (c *Conn) leave()            { c.g = nil }

// ---- frame parser ----

// receive consumes b, which it may modify. Frames can be split anywhere between
// calls; the 14-byte header and the pieces of a message are kept in c.
func (c *Conn) receive(b []byte) {
	c.lastRecv, c.pingOut = c.now, false // any traffic shows the peer is alive
	for len(b) > 0 && !c.done {
		if c.ph == phHeader {
			want := 2
			if c.hlen >= 2 {
				want = headerLen(c.hdr[1])
			}
			n := min(len(b), want-c.hlen)
			copy(c.hdr[c.hlen:], b[:n])
			c.hlen += n
			b = b[n:]
			if c.hlen >= 2 && c.hdr[1]&0x80 == 0 { // do not wait for a mask that is not coming
				c.fail(CloseProtocolError, "client frames must be masked")
				return
			}
			if c.hlen < 2 || c.hlen < headerLen(c.hdr[1]) {
				continue
			}
			if !c.startFrame() {
				return
			}
			if c.remain == 0 { // an empty payload: handled like any other, just with nothing in it
				c.payload(nil, true)
				if !c.done {
					c.endFrame()
				}
			}
			continue
		}
		n := uint64(len(b))
		whole := c.mpos == 0 && n >= c.remain // the rest of the frame is right here
		if n > c.remain {
			n = c.remain
		}
		chunk := b[:n]
		b = b[n:]
		unmask(chunk, c.mask, c.mpos)
		c.mpos += int(n)
		c.remain -= n
		c.payload(chunk, whole)
		if c.remain == 0 && !c.done {
			c.endFrame()
		}
	}
}

// headerLen is the size of a client frame header given its second byte: 2, the
// extended length and the mask (clients always mask).
func headerLen(b1 byte) int {
	switch b1 & 0x7f {
	case 126:
		return 2 + 2 + 4
	case 127:
		return 2 + 8 + 4
	}
	return 2 + 4
}

// startFrame validates a complete header and gets ready for the payload.
func (c *Conn) startFrame() bool {
	h := c.hdr[:c.hlen]
	c.hlen = 0
	b0, b1 := h[0], h[1]
	c.fin, c.op = b0&0x80 != 0, Opcode(b0&0x0f)
	switch {
	case b0&0x70 != 0:
		c.fail(CloseProtocolError, "reserved bits set (no extension was negotiated)")
		return false
	case b1&0x80 == 0:
		c.fail(CloseProtocolError, "client frames must be masked")
		return false
	}
	n := uint64(b1 & 0x7f)
	rest := h[2:]
	switch n {
	case 126:
		n, rest = uint64(binary.BigEndian.Uint16(rest)), rest[2:]
	case 127:
		n, rest = binary.BigEndian.Uint64(rest), rest[8:]
		if n>>63 != 0 {
			c.fail(CloseProtocolError, "frame length has its top bit set")
			return false
		}
	}
	copy(c.mask[:], rest)
	switch c.op {
	case OpContinuation:
		if !c.inMsg {
			c.fail(CloseProtocolError, "continuation without a message")
			return false
		}
	case OpText, OpBinary:
		if c.inMsg {
			c.fail(CloseProtocolError, "new message inside a fragmented one")
			return false
		}
	case OpClose, OpPing, OpPong:
		if !c.fin || n > 125 {
			c.fail(CloseProtocolError, "control frames must be short and unfragmented")
			return false
		}
	default:
		c.fail(CloseProtocolError, "unknown opcode")
		return false
	}
	if c.op < OpClose && (n > uint64(c.e.maxMessage) || uint64(len(c.msg))+n > uint64(c.e.maxMessage)) {
		c.fail(CloseTooBig, "message too big")
		return false
	}
	c.ph, c.remain, c.mpos, c.clen = phPayload, n, 0, 0
	return true
}

// payload handles a slice of the current frame's (already unmasked) payload.
// whole is set when the slice is all of it.
func (c *Conn) payload(p []byte, whole bool) {
	if c.op >= OpClose {
		c.clen += copy(c.ctl[c.clen:], p)
		return
	}
	if c.closeSent { // we have said goodbye: what follows is not delivered
		if whole && c.fin {
			c.inMsg = false
		}
		return
	}
	if whole && c.fin && !c.inMsg { // a complete message in one piece: no copying
		if c.op == OpText && !utf8.Valid(p) {
			c.fail(CloseInvalidPayload, "invalid UTF-8 in text message")
			return
		}
		c.deliver(c.op, p)
		return
	}
	if !c.inMsg {
		c.inMsg, c.msgOp, c.vpos = true, c.op, 0
	}
	c.msg = append(c.msg, p...)
	if c.msgOp == OpText {
		n, ok := validPrefix(c.msg[c.vpos:], false)
		if !ok {
			c.fail(CloseInvalidPayload, "invalid UTF-8 in text message")
			return
		}
		c.vpos += n
	}
}

// endFrame runs when a frame's payload is complete.
func (c *Conn) endFrame() {
	c.ph = phHeader
	switch c.op {
	case OpPing:
		if !c.closeSent {
			c.queue(OpPong, c.ctl[:c.clen])
		}
	case OpPong:
		c.callPong(c.ctl[:c.clen])
	case OpClose:
		c.onClose(c.ctl[:c.clen])
	default: // a data frame; whole messages in one piece were delivered already
		if !c.fin || !c.inMsg || c.closeSent {
			if c.fin {
				c.inMsg, c.msg = false, c.msg[:0]
			}
			return
		}
		if c.msgOp == OpText {
			if _, ok := validPrefix(c.msg[c.vpos:], true); !ok {
				c.fail(CloseInvalidPayload, "invalid UTF-8 in text message")
				return
			}
		}
		op, msg := c.msgOp, c.msg
		c.inMsg = false
		c.deliver(op, msg)
		if cap(c.msg) > 64<<10 {
			c.msg = nil
		} else {
			c.msg = c.msg[:0]
		}
	}
}

// The callbacks are run behind a recover, as HTTP handlers are: a panic in one
// closes that connection with 1011 and leaves the isolate, the other connections
// and OnClose's bookkeeping intact.

func (c *Conn) deliver(op Opcode, p []byte) {
	if f := c.e.cfg.OnMessage; f != nil {
		defer c.recoverCallback()
		f(c, op, p)
	}
}

func (c *Conn) callOpen() {
	if f := c.e.cfg.OnOpen; f != nil {
		defer c.recoverCallback()
		f(c)
	}
}

func (c *Conn) callPong(p []byte) {
	if f := c.e.cfg.OnPong; f != nil {
		defer c.recoverCallback()
		f(c, p)
	}
}

func (c *Conn) callClose(code uint16) {
	if f := c.e.cfg.OnClose; f != nil {
		defer func() { recover() }() // nothing left to close
		f(c, code, c.peerReason)
	}
}

func (c *Conn) recoverCallback() {
	if recover() != nil {
		c.fail(CloseInternalError, "internal error")
	}
}

// onClose handles the peer's close frame (RFC 6455 §5.5.1, §7.1).
func (c *Conn) onClose(p []byte) {
	code, reason := CloseNoStatus, []byte(nil)
	switch {
	case len(p) == 1:
		c.fail(CloseProtocolError, "close frame with a 1-byte payload")
		return
	case len(p) >= 2:
		code, reason = binary.BigEndian.Uint16(p), p[2:]
		if !validCloseCode(code) {
			c.fail(CloseProtocolError, "invalid close code")
			return
		}
		if !utf8.Valid(reason) {
			c.fail(CloseInvalidPayload, "close reason is not UTF-8")
			return
		}
	}
	c.closeRecv, c.peerCode = true, code
	c.peerReason = append(c.peerReason[:0], reason...)
	if !c.closeSent { // answer, echoing the code; the server then ends the connection
		if code == CloseNoStatus {
			c.Close(CloseNormal, "")
		} else {
			c.Close(code, "")
		}
	}
	c.done = true
}
