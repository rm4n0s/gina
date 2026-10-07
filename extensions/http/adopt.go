package http

import (
	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// Embedding: another server (extensions/http2, to serve HTTP/1.1 on its port)
// can run this package's connection logic inside an isolate of its own. It
// creates the Server with New, calls InitEmbedded instead of Install (no
// listener, no isolate types: the host owns both), and for each connection that
// speaks HTTP/1.1 calls Adopt and then forwards every message of that isolate to
// Handle until it returns a terminal effect.

// Conn is the state of one HTTP/1.1 connection driven by a host isolate.
type Conn struct{ cs connState }

// InitEmbedded prepares s to serve connections adopted on up to shards shards.
func (s *Server) InitEmbedded(shards int) {
	s.st = make([]shardState, shards)
}

// Adopt takes over a connection in the middle of its life. fd is the socket,
// owned by the calling isolate; t is its TLS session (nil for a plain
// connection), which may still be in the handshake; cbuf is the ciphertext read
// buffer that goes with t; plain holds bytes already read and decrypted. The
// returned effect is the isolate's next move (send the pending handshake flight,
// answer a request, or wait for one).
func (s *Server) Adopt(g *gina.Ctx, fd gina.FDHandle, t *gtls.Conn, cbuf, plain []byte) (*Conn, gina.Effect) {
	st := s.sh(g)
	var c *Conn
	if k := len(st.freeConns); k > 0 {
		c, st.freeConns = st.freeConns[k-1], st.freeConns[:k-1]
	} else {
		c = new(Conn)
	}
	cs := &c.cs
	*cs = connState{fd: fd, tls: t, cbuf: cbuf, owner: c}
	cs.rbuf = st.getBuf(max(s.cfg.ReadBufSize, len(plain)))
	cs.wbuf = st.getBuf(1024)[:0]
	st.conns.Add(1)
	if len(plain) > 0 {
		cs.rlen = copy(cs.rbuf, plain)
		cs.reqStart = g.Now()
	}
	if t != nil {
		return c, s.flush(cs, g) // sends the handshake flight if there is one, else carries on
	}
	return c, s.process(cs, g)
}

// Handle runs one message of an adopted connection's isolate.
func (s *Server) Handle(c *Conn, g *gina.Ctx, m *gina.Message) gina.Effect {
	return s.connHandler(&c.cs, g, m)
}
