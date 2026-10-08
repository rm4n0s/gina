package sql

// A canceller sends one CancelRequest. PostgreSQL has no way to interrupt a
// running statement on its own connection, so the protocol asks for a second
// connection that carries the backend's process id and secret key and nothing else.
// database/sql does this from the goroutine that watches a context; here it is a
// short-lived isolate with a socket of its own.
//
// The request is sent in plain text even when the main connection uses TLS, as the
// server accepts it before authentication. PostgreSQL gives no answer: it closes the
// connection, which is what the canceller waits for (briefly) before it leaves.
// A cancellation that arrives after the statement finished is ignored by the
// server, unless the connection has already started another one: that is inherent
// to the protocol.

import (
	"time"

	"github.com/rm4n0s/gina"
)

const cancelTimeout = 5 * time.Second

type canceller struct {
	fd     gina.FDHandle
	packet []byte
	sent   bool
	buf    [16]byte
}

func (d *DB) cancellerHandler(c *canceller, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagCancelGo:
		c.packet = append([]byte(nil), g.Data()...)
		fd, err := g.Dial(gina.DialSpec{IP: d.cfg.Addr.Addr(), Port: d.cfg.Addr.Port()})
		if err != nil {
			return gina.Done()
		}
		c.fd = fd
		g.IOConnect(fd, cancelTimeout)
		return gina.WaitIO()
	case gina.TagIOConnect:
		if gina.PayloadAs[gina.IOResult](m).Result < 0 {
			return c.finish(g)
		}
		g.IOSend(c.fd, c.packet, cancelTimeout)
		return gina.WaitIO()
	case gina.TagIOSend:
		if gina.PayloadAs[gina.IOResult](m).Result < 0 {
			return c.finish(g)
		}
		c.sent = true
		g.IORecv(c.fd, c.buf[:], 2*time.Second) // the server closes when it has acted
		return gina.WaitIO()
	case gina.TagIORecv:
		return c.finish(g)
	case gina.TagShutdown:
		return c.finish(g)
	}
	return gina.WaitMessage()
}

func (c *canceller) finish(g *gina.Ctx) gina.Effect {
	if c.fd != 0 {
		g.CloseFD(c.fd)
		c.fd = 0
	}
	return gina.Done()
}
