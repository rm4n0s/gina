package websocket_test

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	ghttp "github.com/rm4n0s/gina/extensions/http"
	"github.com/rm4n0s/gina/extensions/http2"
	ws "github.com/rm4n0s/gina/extensions/websocket"
)

// A subscriber whose mailbox overflows must be told it missed messages instead
// of silently continuing with a gap: by default it is closed with 1013, and
// OnOverflow can say why. What it received before the close is a gap-free prefix.
func TestSlowSubscriberIsDisconnectedWithTryAgainLater(t *testing.T) {
	for _, custom := range []bool{false, true} {
		name := map[bool]string{false: "default", true: "OnOverflow"}[custom]
		t.Run(name, func(t *testing.T) {
			forAll(t, func(t *testing.T, p proto) {
				var n atomic.Int32
				var overflows atomic.Int32
				cfg := hubConfig()
				if custom {
					cfg.OnOverflow = func(c *ws.Conn, lost int) {
						if lost > 0 {
							overflows.Add(1)
						}
						c.Close(ws.CloseTryAgainLater, `{"reconnect":true}`)
					}
				}
				o := hubOpts(&n)
				o.h1cfg = func(c *ghttp.Config) { c.ConnMailbox = 4 }
				o.h2cfg = func(c *http2.Config) { c.ConnMailbox = 4 }
				s := startServer(t, p, cfg, o)
				c, _ := s.dial(dialOpts{})
				c.deadline(10 * time.Second)
				c.expectText("welcome")
				waitFor(t, "hub to know the peer", func() bool { return n.Load() == 1 })

				c.writeFrame(true, ws.OpText, []byte("flood"))
				next := 0
				for {
					op, data, err := c.tryReadMsg()
					if err != nil {
						t.Fatalf("connection ended without a close frame after %d messages: %v", next, err)
					}
					if op == ws.OpClose {
						code := binary.BigEndian.Uint16(data)
						if code != ws.CloseTryAgainLater {
							t.Fatalf("close code %d, want 1013", code)
						}
						if custom && string(data[2:]) != `{"reconnect":true}` {
							t.Fatalf("close reason %q", data[2:])
						}
						break
					}
					if string(data) != fmt.Sprintf("f%03d", next) {
						t.Fatalf("gap in the stream: got %q, want f%03d", data, next)
					}
					if next++; next == 200 {
						t.Fatal("all 200 messages arrived: the mailbox never overflowed")
					}
				}
				if custom && overflows.Load() != 1 {
					t.Fatalf("OnOverflow called %d times", overflows.Load())
				}
				t.Logf("%d messages arrived before the close", next)
			})
		})
	}
}
