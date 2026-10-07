package http

import (
	"time"

	"github.com/rm4n0s/gina"
)

// Tunnels. A handler can hand the connection (HTTP/1.1, after a 101 response) or
// one stream of it (HTTP/2, after a 2xx answer to an extended CONNECT, RFC 8441)
// to a Tunnel: a protocol that takes over the byte stream once HTTP is done with
// it. extensions/websocket is the one that exists; the interface is what the
// servers need from it, and is the same for both transports, so a tunnel cannot
// tell which carried it.
//
// A tunnel lives inside its connection isolate and is only ever called from that
// isolate's turns, so it needs no locking. Other isolates reach it with
// SendTunnel.

// TagTunnel is delivered to a connection isolate that carries a tunnel. The
// message's Correlation is the stream id (0 on HTTP/1.1) and its data (Ctx.Data)
// what was given to SendTunnel; the server calls Tunnel.Push with it.
const TagTunnel gina.Tag = gina.TagUserBase + 0x12

// SendTunnel pushes payload to the tunnel on stream of connection conn (stream is
// 0 on HTTP/1.1), from any isolate on any shard. The payload may be any size up
// to the system's MaxMessageBytes; it is copied. Delivery is best effort: a full
// mailbox, an exhausted message pool or a connection that is gone drops it.
func SendTunnel(g *gina.Ctx, conn gina.Handle, stream uint32, payload []byte) gina.SendResult {
	return g.SendCorr(conn, TagTunnel, stream, payload)
}

// SendTunnelBlob is SendTunnel for a payload shared between many receivers, which
// is then not copied for each (gina.Blob). A broadcast sends one Blob to every
// connection.
func SendTunnelBlob(g *gina.Ctx, conn gina.Handle, stream uint32, payload *gina.Blob) gina.SendResult {
	return g.SendBlob(conn, TagTunnel, stream, payload)
}

// Tunnel is a protocol running over an upgraded connection or stream.
type Tunnel interface {
	// Open is called once the response that switched protocols has been queued.
	// stream is the HTTP/2 stream id, 0 on HTTP/1.1. The tunnel may queue output.
	Open(g *gina.Ctx, stream uint32)
	// Receive is given bytes from the peer, in order. It must consume all of them:
	// the slice is only valid during the call and may be modified in place.
	Receive(g *gina.Ctx, b []byte)
	// Push delivers a message from SendTunnel. payload is only valid during the call.
	Push(g *gina.Ctx, payload []byte)
	// Outgoing returns bytes waiting to be sent; Sent reports that the first n of
	// them have been taken (written, or queued for the wire). A server may call
	// Outgoing again before Sent, and must not call Receive or Push while a write
	// of the returned slice is in flight.
	Outgoing() []byte
	Sent(n int)
	// Idle reports how long from now (g.Now() nanoseconds) until the tunnel wants
	// Tick; ok is false when it wants no timer. Tick is called when that time has
	// passed, and may be called early or spuriously.
	Idle(now uint64) (d time.Duration, ok bool)
	Tick(g *gina.Ctx)
	// Done reports that the tunnel has finished: once Outgoing is drained the
	// server closes the connection (HTTP/1.1) or ends the stream (HTTP/2).
	Done() bool
	// Shutdown tells the tunnel the server is stopping; it should queue whatever
	// goodbye its protocol has and become Done.
	Shutdown(g *gina.Ctx)
	// Closed is called exactly once after Open, when the connection or stream is
	// gone for whatever reason (the peer, an error, Done, shutdown).
	Closed(g *gina.Ctx)
}

// SetTunnel asks the server to give the connection (or stream) to t after this
// response, provided the response status is 101 on HTTP/1.1 or 2xx on HTTP/2
// (where only an extended CONNECT can be tunnelled). With any other status t is
// dropped without being opened.
func (c *Context) SetTunnel(t Tunnel) { c.tunnel = t }
