package websocket

import (
	"unicode/utf8"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
)

// Pushes to a connection from another isolate, on any shard. They travel as Gina
// messages (of any size, see gina.MaxPayload and SystemSpec.MaxMessageBytes) to
// the connection isolate, which writes them out in its next turn. The result of a
// push says whether Gina accepted the message, not whether it reached the peer.

// Push sends a text, binary or ping message to the connection at to. The data is
// copied.
func Push(g *gina.Ctx, to Peer, op Opcode, data []byte) gina.SendResult {
	if op != OpText && op != OpBinary && op != OpPing {
		return gina.SendPayloadTooLarge
	}
	if len(data) < gina.MaxPayload { // the opcode makes one more: stay inline, on the stack
		var b [gina.MaxPayload]byte
		b[0] = byte(op)
		n := copy(b[1:], data)
		return ghttp.SendTunnel(g, to.Conn, uint32(to.Stream), b[:1+n])
	}
	b := make([]byte, 1+len(data))
	b[0] = byte(op)
	copy(b[1:], data)
	return ghttp.SendTunnelBlob(g, to.Conn, uint32(to.Stream), gina.AdoptBlob(b))
}

// PushText is Push for a string.
func PushText(g *gina.Ctx, to Peer, s string) gina.SendResult {
	if len(s) < gina.MaxPayload {
		var b [gina.MaxPayload]byte
		b[0] = byte(OpText)
		n := copy(b[1:], s)
		return ghttp.SendTunnel(g, to.Conn, uint32(to.Stream), b[:1+n])
	}
	b := make([]byte, 1+len(s))
	b[0] = byte(OpText)
	copy(b[1:], s)
	return ghttp.SendTunnelBlob(g, to.Conn, uint32(to.Stream), gina.AdoptBlob(b))
}

// PushClose asks the connection at to to start the closing handshake. reason is
// cut to 123 bytes.
func PushClose(g *gina.Ctx, to Peer, code uint16, reason string) gina.SendResult {
	b := make([]byte, 0, 3+123)
	b = append(b, byte(OpClose), byte(code>>8), byte(code))
	b = append(b, reason[:min(len(reason), 123)]...)
	return ghttp.SendTunnel(g, to.Conn, uint32(to.Stream), b)
}

// Shared is a message built once to be pushed to many connections: the data is
// not copied for each of them, and must not (cannot) be changed. Safe to use from
// any isolate, on any shard.
type Shared struct{ blob *gina.Blob }

// NewShared builds a text or binary message for PushShared. Text must be valid
// UTF-8. data is copied.
func NewShared(op Opcode, data []byte) (*Shared, error) {
	switch {
	case op != OpText && op != OpBinary:
		return nil, ErrBadOpcode
	case op == OpText && !utf8.Valid(data):
		return nil, ErrBadMessage
	}
	b := make([]byte, 1+len(data))
	b[0] = byte(op)
	copy(b[1:], data)
	return &Shared{gina.AdoptBlob(b)}, nil
}

// PushShared sends a Shared message to the connection at to.
func PushShared(g *gina.Ctx, to Peer, m *Shared) gina.SendResult {
	return ghttp.SendTunnelBlob(g, to.Conn, uint32(to.Stream), m.blob)
}
