// Package websocket is a WebSocket (RFC 6455) server for Gina. It is a protocol
// on top of the HTTP extensions, not a server of its own: a route handler calls
// Endpoint.Serve (or Upgrade), and the connection, once switched, belongs to a
// Conn that runs inside the same connection isolate. The same Endpoint serves
// all three ways a WebSocket can reach it:
//
//   - ws:// over HTTP/1.1 (extensions/http): the Upgrade handshake of RFC 6455 §4.
//   - wss:// over HTTP/1.1 with TLS (Config.TLS of extensions/http).
//   - over HTTP/2, with or without TLS (extensions/http2 with ExtendedConnect set):
//     the extended CONNECT of RFC 8441, one stream of a shared connection per
//     WebSocket. Browsers use this for wss:// when the server offers it.
//
// The handler cannot tell which: the Conn API is the same, and TLS is below it.
//
//	ep := websocket.New(websocket.Config{
//		OnMessage: func(c *websocket.Conn, op websocket.Opcode, data []byte) { c.Send(op, data) },
//	})
//	r := ghttp.NewRouter()
//	r.GET("/ws", ep.Serve)
//
// # Isolates and threads
//
// Callbacks run synchronously inside the connection isolate's turn, on the shard's
// thread, like any HTTP handler. data passed to OnMessage is only valid during
// the call. A Conn must not be used outside its callbacks; other isolates, on any
// shard, reach it through its Peer with Push, a message to the connection isolate
// that the isolate delivers while it waits for the peer (it parks in
// gina.WaitIOOrMessage). Pushes are limited to MaxPush bytes, the size of a Gina
// message, and are best effort: a full mailbox (raise Config.ConnMailbox of the
// HTTP server), an exhausted message pool, or a closed connection drops them.
// A hub that broadcasts keeps the Peers its OnOpen handed it and removes them
// when OnClose says so.
//
// # What is implemented
//
// All frame types, fragmented messages, ping/pong with a server-side keep-alive,
// the closing handshake with a timeout, UTF-8 validation of text messages and
// close reasons (incrementally, so a bad message is refused at the first bad
// byte), strict header checks (reserved bits, masking, control frame limits,
// opcodes) and message and queue size limits. The default Origin check is
// same-origin, which stops cross-site WebSocket hijacking by browsers.
//
// Not implemented: extensions (permessage-deflate: a client that offers it is
// answered without it, which is allowed), and sending fragmented messages (every
// message goes out as one frame).
//
// A half-duplex caveat comes from the HTTP servers: a connection reads, then
// writes what is queued, then reads again, never both at once. Writes are bounded
// by their WriteTimeout, so a peer that stops reading is dropped.
package websocket

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"sync/atomic"
	"time"

	"gina"
	ghttp "gina/extensions/http"
)

// MaxPush is the most data Push can carry: a message payload less the stream id
// and the opcode.
const MaxPush = ghttp.MaxTunnelPush - 1

// MailboxCapacity is a sensible ConnMailbox for the HTTP servers when many pushes
// arrive per connection between its turns (a broadcast hub).
const MailboxCapacity = 64

// Config configures an Endpoint. The zero value of every field takes a default.
type Config struct {
	// Callbacks. All run inside the connection isolate; see the package comment.
	OnOpen    func(c *Conn)                             // the handshake is done
	OnMessage func(c *Conn, op Opcode, data []byte)     // a complete text or binary message
	OnPong    func(c *Conn, data []byte)                // a pong arrived
	OnClose   func(c *Conn, code uint16, reason []byte) // the connection ended: code is the peer's, ours, or CloseAbnormal

	// Subprotocols the server speaks, in no particular order; the first one the
	// client offered (in the client's order) is selected. None offered, or none
	// matching, means no subprotocol.
	Subprotocols []string

	// CheckOrigin decides whether a browser page may open a connection. origin is
	// the Origin header and host the Host header (HTTP/2: :authority). It is not
	// called when there is no Origin header (not a browser). The default accepts
	// only an origin whose host is the request's host.
	CheckOrigin func(origin, host []byte) bool

	MaxMessageSize int           // largest message accepted, in bytes (default 1 MiB); more closes with 1009
	MaxQueued      int           // unsent bytes per connection before Send fails with ErrQueueFull (default 1 MiB)
	PingInterval   time.Duration // ping a peer silent this long (default 30s; negative: never)
	PongTimeout    time.Duration // close a peer that does not answer a ping in this time (default 10s)
	CloseTimeout   time.Duration // wait this long for the peer's close frame (default 5s)
}

// Endpoint accepts WebSocket connections for a route. Create it with New; one
// Endpoint is shared by all shards, so everything on it is immutable or atomic.
type Endpoint struct {
	cfg                                     Config
	maxMessage, maxQueued                   int
	pingInterval, pongTimeout, closeTimeout time.Duration
	conns                                   atomic.Int64
}

// New creates an Endpoint.
func New(cfg Config) *Endpoint {
	e := &Endpoint{cfg: cfg, maxMessage: cfg.MaxMessageSize, maxQueued: cfg.MaxQueued,
		pingInterval: cfg.PingInterval, pongTimeout: cfg.PongTimeout, closeTimeout: cfg.CloseTimeout}
	if e.maxMessage <= 0 {
		e.maxMessage = 1 << 20
	}
	if e.maxQueued <= 0 {
		e.maxQueued = 1 << 20
	}
	if e.pingInterval == 0 {
		e.pingInterval = 30 * time.Second
	}
	if e.pongTimeout <= 0 {
		e.pongTimeout = 10 * time.Second
	}
	if e.closeTimeout <= 0 {
		e.closeTimeout = 5 * time.Second
	}
	return e
}

// Conns is the number of open connections (all shards).
func (e *Endpoint) Conns() int { return int(e.conns.Load()) }

// Serve is a route handler: it upgrades the request, or answers it with the
// error status that says why not.
func (e *Endpoint) Serve(c *ghttp.Context) { e.Upgrade(c) }

const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Upgrade validates a WebSocket handshake, HTTP/1.1 or HTTP/2, and accepts it. On
// success the response is set up and the returned Conn is handed to the server
// when the handler returns: use it to set Conn.Data and nothing else, since the
// connection is not open yet (OnOpen runs when it is). On failure the Context
// holds the error response, and the result is nil.
func (e *Endpoint) Upgrade(c *ghttp.Context) (*Conn, bool) {
	r := c.Req
	h2 := r.Protocol != nil
	switch {
	case h2:
		if !equalFold(r.Protocol, "websocket") {
			reject(c, 501, "only the websocket protocol is supported\n")
			return nil, false
		}
	case r.Method != "GET" || r.Minor < 1 || !hasToken(r, "upgrade", "websocket") || !hasToken(r, "connection", "upgrade"):
		c.SetHeader("Upgrade", "websocket")
		reject(c, 426, "a WebSocket upgrade is required\n")
		return nil, false
	}
	if v := r.Header("sec-websocket-version"); string(v) != "13" {
		c.SetHeader("Sec-WebSocket-Version", "13")
		reject(c, 426, "unsupported WebSocket version\n")
		return nil, false
	}
	var accept [28]byte
	if !h2 {
		key := r.Header("sec-websocket-key")
		var raw [18]byte
		if n, err := base64.StdEncoding.Decode(raw[:], key); err != nil || n != 16 || len(key) != 24 {
			reject(c, 400, "bad Sec-WebSocket-Key\n")
			return nil, false
		}
		var buf [24 + len(guid)]byte
		copy(buf[copy(buf[:], key):], guid)
		sum := sha1.Sum(buf[:])
		base64.StdEncoding.Encode(accept[:], sum[:])
	}
	if origin := r.Header("origin"); origin != nil {
		check := e.cfg.CheckOrigin
		if check == nil {
			check = sameOrigin
		}
		if !check(origin, r.Header("host")) {
			reject(c, 403, "origin not allowed\n")
			return nil, false
		}
	}
	sub := e.selectProtocol(r)

	ws := &Conn{e: e, h2: h2, sub: sub}
	if h2 {
		c.Status(200)
	} else {
		c.Status(101)
		c.SetHeader("Upgrade", "websocket")
		c.SetHeader("Connection", "Upgrade")
		c.SetHeader("Sec-WebSocket-Accept", string(accept[:]))
	}
	if sub != "" {
		c.SetHeader("Sec-WebSocket-Protocol", sub)
	}
	c.SetTunnel(ws)
	return ws, true
}

func reject(c *ghttp.Context, code int, msg string) { c.String(code, msg) }

// selectProtocol picks the first subprotocol the client offered that we speak.
func (e *Endpoint) selectProtocol(r *ghttp.Request) string {
	if len(e.cfg.Subprotocols) == 0 {
		return ""
	}
	for _, h := range r.Headers {
		if !equalFold(h.Name, "sec-websocket-protocol") {
			continue
		}
		for v := h.Value; len(v) > 0; {
			var tok []byte
			if i := bytes.IndexByte(v, ','); i >= 0 {
				tok, v = v[:i], v[i+1:]
			} else {
				tok, v = v, nil
			}
			tok = bytes.TrimSpace(tok)
			for _, s := range e.cfg.Subprotocols {
				if string(tok) == s {
					return s
				}
			}
		}
	}
	return ""
}

// sameOrigin accepts an Origin of the form scheme://host[:port] whose authority
// is the Host the request was made to.
func sameOrigin(origin, host []byte) bool {
	i := bytes.Index(origin, []byte("://"))
	return i > 0 && len(host) > 0 && equalFold(origin[i+3:], string(host))
}

// hasToken reports whether any header called name lists token (comma separated,
// case-insensitive), as in "Connection: keep-alive, Upgrade".
func hasToken(r *ghttp.Request, name, token string) bool {
	for _, h := range r.Headers {
		if !equalFold(h.Name, name) {
			continue
		}
		for v := h.Value; len(v) > 0; {
			var tok []byte
			if i := bytes.IndexByte(v, ','); i >= 0 {
				tok, v = v[:i], v[i+1:]
			} else {
				tok, v = v, nil
			}
			if equalFold(bytes.TrimSpace(tok), token) {
				return true
			}
		}
	}
	return false
}

func equalFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		x, y := b[i], s[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

// ---- pushing from other isolates ----

// Push sends a text or binary message to the connection at to, from any isolate
// on any shard. data is at most MaxPush bytes. Delivery is best effort (see the
// package comment); the result says whether the message was handed to Gina, not
// whether it reached the peer.
func Push(g *gina.Ctx, to Peer, op Opcode, data []byte) gina.SendResult {
	if len(data) > MaxPush || (op != OpText && op != OpBinary && op != OpPing) {
		return gina.SendPayloadTooLarge
	}
	var b [ghttp.MaxTunnelPush]byte
	b[0] = byte(op)
	n := copy(b[1:], data)
	return ghttp.SendTunnel(g, to.Conn, uint32(to.Stream), b[:1+n])
}

// PushText is Push with a string.
func PushText(g *gina.Ctx, to Peer, s string) gina.SendResult {
	if len(s) > MaxPush {
		return gina.SendPayloadTooLarge
	}
	var b [ghttp.MaxTunnelPush]byte
	b[0] = byte(OpText)
	n := copy(b[1:], s)
	return ghttp.SendTunnel(g, to.Conn, uint32(to.Stream), b[:1+n])
}

// PushClose asks the connection at to to start the closing handshake. reason is
// cut to fit a message.
func PushClose(g *gina.Ctx, to Peer, code uint16, reason string) gina.SendResult {
	var b [ghttp.MaxTunnelPush]byte
	b[0], b[1], b[2] = byte(OpClose), byte(code>>8), byte(code)
	n := copy(b[3:], reason)
	return ghttp.SendTunnel(g, to.Conn, uint32(to.Stream), b[:3+n])
}
