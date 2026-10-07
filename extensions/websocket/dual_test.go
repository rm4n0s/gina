package websocket_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina/extensions/http2"
	ws "github.com/rm4n0s/gina/extensions/websocket"
)

// A WebSocket over HTTP/1.1 on a port that also serves HTTP/2 (HTTP1Fallback):
// echo, pushes from another shard, and the overflow close all work inside the
// HTTP/1.1 connection that the HTTP/2 isolate adopted.
func TestWebSocketOverHTTP1OnAnHTTP2Port(t *testing.T) {
	for _, p := range []proto{dualH1, dualH1TLS} {
		t.Run(p.String(), func(t *testing.T) {
			s := startServer(t, p, echoConfig(), opts{})
			c, hs := s.dial(dialOpts{})
			if c == nil {
				t.Fatalf("handshake status %d", hs.status)
			}
			c.deadline(5 * time.Second)
			c.writeFrame(true, ws.OpText, []byte("ping me"))
			c.expectText("ping me")
		})
		t.Run(p.String()+"/hub", func(t *testing.T) {
			var n atomic.Int32
			s := startServer(t, p, hubConfig(), hubOpts(&n))
			var cs []*client
			for i := 0; i < 3; i++ {
				c, _ := s.dial(dialOpts{})
				c.deadline(10 * time.Second)
				c.expectText("welcome")
				cs = append(cs, c)
			}
			waitFor(t, "hub to know 3 peers", func() bool { return n.Load() == 3 })
			cs[0].writeFrame(true, ws.OpText, []byte("bcast hello all"))
			for i, c := range cs {
				if _, got := c.readMsg(); string(got) != "hello all" {
					t.Fatalf("client %d got %q", i, got)
				}
			}
		})
		t.Run(p.String()+"/overflow", func(t *testing.T) {
			var n atomic.Int32
			o := hubOpts(&n)
			o.h2cfg = func(c *http2.Config) { c.ConnMailbox = 4 }
			s := startServer(t, p, hubConfig(), o)
			c, _ := s.dial(dialOpts{})
			c.deadline(10 * time.Second)
			c.expectText("welcome")
			waitFor(t, "hub to know the peer", func() bool { return n.Load() == 1 })
			c.writeFrame(true, ws.OpText, []byte("flood"))
			for next := 0; ; next++ {
				op, data, err := c.tryReadMsg()
				if err != nil {
					t.Fatalf("no close frame: %v", err)
				}
				if op == ws.OpClose {
					if code := uint16(data[0])<<8 | uint16(data[1]); code != ws.CloseTryAgainLater {
						t.Fatalf("close code %d", code)
					}
					return
				}
				if string(data) != fmt.Sprintf("f%03d", next) || next == 199 {
					t.Fatalf("message %d: %q", next, data)
				}
			}
		})
	}
}
