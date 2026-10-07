//go:build linux

package websocket_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
	"github.com/rm4n0s/gina/extensions/http2"
	ws "github.com/rm4n0s/gina/extensions/websocket"
)

// forAll runs a test over every way a WebSocket can reach the server.
func forAll(t *testing.T, f func(t *testing.T, p proto)) {
	for _, p := range allProtos {
		t.Run(p.String(), func(t *testing.T) { f(t, p) })
	}
}

func echoConfig() ws.Config {
	return ws.Config{OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) { c.Send(op, data) }}
}

func TestAcceptKeyMatchesRFCSample(t *testing.T) {
	if got := wsAccept("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("test helper is wrong: %s", got)
	}
}

func TestEchoOnEveryTransport(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, echoConfig(), opts{})
		c, _ := s.dial(dialOpts{})
		c.deadline(10 * time.Second)

		c.writeFrame(true, ws.OpText, []byte("hello ✓"))
		c.expectText("hello ✓")

		bin := make([]byte, 70000)
		for i := range bin {
			bin[i] = byte(i * 31)
		}
		c.writeFrame(true, ws.OpBinary, bin)
		if op, got := c.readMsg(); op != ws.OpBinary || !bytes.Equal(got, bin) {
			t.Fatalf("binary echo: op %d, %d bytes", op, len(got))
		}

		c.writeFrame(true, ws.OpText, nil) // empty message
		c.expectText("")

		// a fragmented message with a ping in the middle
		c.writeFrame(false, ws.OpText, []byte("frag"))
		c.writeFrame(true, ws.OpPing, []byte("mid"))
		c.writeFrame(true, ws.OpContinuation, []byte("mented"))
		fin, op, pl, err := c.readFrame()
		if err != nil || !fin || op != ws.OpPong || string(pl) != "mid" {
			t.Fatalf("pong: %v %d %q %v", fin, op, pl, err)
		}
		c.expectText("fragmented")

		if n := s.ep.Conns(); n != 1 {
			t.Fatalf("Conns = %d, want 1", n)
		}
	})
}

func TestLargeMessagesOverHTTP1(t *testing.T) {
	for _, p := range []proto{h1, h1TLS} {
		t.Run(p.String(), func(t *testing.T) {
			s := startServer(t, p, ws.Config{MaxMessageSize: 4 << 20, MaxQueued: 8 << 20,
				OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) { c.Send(op, data) }}, opts{})
			c, _ := s.dial(dialOpts{})
			c.deadline(20 * time.Second)
			big := bytes.Repeat([]byte("0123456789abcdef"), 3<<16) // 3 MiB
			c.writeFrame(true, ws.OpBinary, big)
			if op, got := c.readMsg(); op != ws.OpBinary || !bytes.Equal(got, big) {
				t.Fatalf("echo of %d bytes: op %d, %d bytes back", len(big), op, len(got))
			}
		})
	}
}

func TestHandshakeRejections(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, echoConfig(), opts{})
		h2 := p == h2c || p == h2TLS

		if c, hs := s.dial(dialOpts{headers: map[string]string{"Sec-WebSocket-Version": "12"}}); c != nil || hs.status != 426 {
			t.Fatalf("version 12: status %d", hs.status)
		}
		if c, hs := s.dial(dialOpts{headers: map[string]string{"Origin": "http://evil.example"}}); c != nil || hs.status != 403 {
			t.Fatalf("foreign origin: status %d", hs.status)
		}
		// same origin and no origin are fine
		for _, origin := range []string{"http://" + s.addr(), "https://" + s.addr(), ""} {
			c, hs := s.dial(dialOpts{headers: map[string]string{"Origin": origin}})
			if c == nil {
				t.Fatalf("origin %q refused: %d", origin, hs.status)
			}
			c.writeFrame(true, ws.OpText, []byte("x"))
			c.expectText("x")
		}
		if h2 {
			return
		}
		for name, hdr := range map[string]map[string]string{
			"no key":      {"Sec-WebSocket-Key": ""},
			"short key":   {"Sec-WebSocket-Key": "abc"},
			"not base64":  {"Sec-WebSocket-Key": "!!!!!!!!!!!!!!!!!!!!!!=="},
			"no upgrade":  {"Upgrade": ""},
			"wrong proto": {"Upgrade": "h2c"},
			"no conn":     {"Connection": ""},
			"conn close":  {"Connection": "close"},
		} {
			c, hs := s.dial(dialOpts{headers: hdr})
			want := 400
			if name == "no upgrade" || name == "wrong proto" || name == "no conn" || name == "conn close" {
				want = 426
			}
			if c != nil || hs.status != want {
				t.Errorf("%s: status %d, want %d", name, hs.status, want)
			}
		}
		// Connection: keep-alive, Upgrade is a list, and tokens are case-insensitive
		if c, hs := s.dial(dialOpts{headers: map[string]string{"Connection": "keep-alive, UPGRADE", "Upgrade": "WebSocket"}}); c == nil {
			t.Fatalf("token list: status %d", hs.status)
		}
	})
}

func TestPlainRequestsStillWork(t *testing.T) {
	s := startServer(t, h1, echoConfig(), opts{})
	c := s.tcp()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(c, "GET /plain HTTP/1.1\r\nHost: x\r\n\r\nGET /ws HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	b, _ := io.ReadAll(c)
	if !strings.Contains(string(b), "200 OK") || !strings.Contains(string(b), "plain") || !strings.Contains(string(b), "426 Upgrade Required") {
		t.Fatalf("responses: %q", b)
	}
}

func TestExtendedConnectOffMeansNoWebSocketOverHTTP2(t *testing.T) {
	s := startServer(t, h2c, echoConfig(), opts{noExtended: true})
	c := s.openH2(h2opts{})
	if c.setting8 {
		t.Fatal("server advertised SETTINGS_ENABLE_CONNECT_PROTOCOL without ExtendedConnect")
	}
	st, hs := c.connect(s, 1, dialOpts{path: "/ws"})
	if st != nil || hs.status != 0 {
		t.Fatalf(":protocol was accepted (status %d)", hs.status)
	}
	// and with it on, the server says so
	s2 := startServer(t, h2c, echoConfig(), opts{})
	if !s2.openH2(h2opts{}).setting8 {
		t.Fatal("ExtendedConnect did not advertise SETTINGS_ENABLE_CONNECT_PROTOCOL")
	}
}

func TestSubprotocolAndCallbacks(t *testing.T) {
	type opened struct{ sub string }
	opens := make(chan opened, 4)
	closes := make(chan string, 4)
	cfg := ws.Config{
		Subprotocols: []string{"chat.v2", "chat.v1"},
		OnOpen:       func(c *ws.Conn) { opens <- opened{c.Subprotocol()} },
		OnClose: func(c *ws.Conn, code uint16, reason []byte) {
			closes <- fmt.Sprintf("%d %s", code, reason)
		},
		OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) { c.Send(op, data) },
	}
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, cfg, opts{})
		c, hs := s.dial(dialOpts{subproto: "foo, chat.v1, chat.v2"})
		if got := (<-opens).sub; got != "chat.v1" { // the client's first choice that we speak
			t.Fatalf("subprotocol %q", got)
		}
		if p == h1 || p == h1TLS {
			if got := hs.header.Get("Sec-WebSocket-Protocol"); got != "chat.v1" {
				t.Fatalf("header %q", got)
			}
		}
		c.deadline(5 * time.Second)
		// the closing handshake: our close is echoed, then the connection ends
		c.writeFrame(true, ws.OpClose, append([]byte{0x0b, 0xb8}, "bye"...)) // 3000
		if code, _ := c.expectClose(); code != 3000 {
			t.Fatalf("echoed code %d", code)
		}
		c.expectEOF()
		select {
		case got := <-closes:
			if got != "3000 bye" {
				t.Fatalf("OnClose: %q", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("OnClose not called")
		}
		// no subprotocol offered, none selected
		c2, _ := s.dial(dialOpts{})
		if got := (<-opens).sub; got != "" {
			t.Fatalf("subprotocol %q with none offered", got)
		}
		c2.raw.Close()
		<-closes
	})
}

func TestAbnormalCloseIsReported(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		closes := make(chan uint16, 1)
		s := startServer(t, p, ws.Config{OnClose: func(c *ws.Conn, code uint16, _ []byte) { closes <- code }}, opts{})
		c, _ := s.dial(dialOpts{})
		c.raw.Close() // vanish without a close frame
		select {
		case code := <-closes:
			if code != ws.CloseAbnormal {
				t.Fatalf("code %d, want 1006", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("OnClose not called")
		}
	})
}

func TestServerInitiatedClose(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, ws.Config{OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
			if string(data) == "quit" {
				c.Close(ws.CloseNormal, "server says bye")
				if c.SendText("after") != ws.ErrClosed {
					panic("send after Close succeeded")
				}
				return
			}
			c.Send(op, data)
		}}, opts{})
		c, _ := s.dial(dialOpts{})
		c.deadline(5 * time.Second)
		c.writeFrame(true, ws.OpText, []byte("quit"))
		if code, reason := c.expectClose(); code != 1000 || reason != "server says bye" {
			t.Fatalf("close %d %q", code, reason)
		}
		c.writeFrame(true, ws.OpClose, []byte{0x03, 0xe8})
		c.expectEOF()
	})
}

func TestProtocolViolations(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, ws.Config{MaxMessageSize: 1000, OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) { c.Send(op, data) }}, opts{})
		cases := []struct {
			name string
			send func(c *client)
			code uint16
		}{
			{"unmasked frame", func(c *client) { c.rw.Write([]byte{0x81, 0x02, 'h', 'i'}) }, ws.CloseProtocolError},
			{"invalid utf-8", func(c *client) { c.writeFrame(true, ws.OpText, []byte{0xc3, 0x28}) }, ws.CloseInvalidPayload},
			{"too big", func(c *client) { c.writeFrame(true, ws.OpBinary, make([]byte, 1001)) }, ws.CloseTooBig},
			{"reserved bit", func(c *client) { c.rw.Write([]byte{0xc1, 0x80, 0, 0, 0, 0}) }, ws.CloseProtocolError},
			{"stray continuation", func(c *client) { c.writeFrame(true, ws.OpContinuation, []byte("x")) }, ws.CloseProtocolError},
		}
		for _, tc := range cases {
			c, _ := s.dial(dialOpts{})
			c.deadline(5 * time.Second)
			tc.send(c)
			if code, _ := c.expectClose(); code != tc.code {
				t.Errorf("%s: close code %d, want %d", tc.name, code, tc.code)
			}
			c.expectEOF()
		}
	})
}

func TestPanickingCallbackClosesOnlyThatConnection(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		closes := make(chan uint16, 2)
		s := startServer(t, p, ws.Config{
			OnClose: func(c *ws.Conn, code uint16, _ []byte) { closes <- code },
			OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
				if string(data) == "boom" {
					var m map[string]int
					m["x"] = 1
				}
				c.Send(op, data)
			},
		}, opts{})
		good, _ := s.dial(dialOpts{})
		bad, _ := s.dial(dialOpts{})
		good.deadline(5 * time.Second)
		bad.deadline(5 * time.Second)
		bad.writeFrame(true, ws.OpText, []byte("boom"))
		if code, _ := bad.expectClose(); code != ws.CloseInternalError {
			t.Fatalf("close %d", code)
		}
		bad.expectEOF()
		if code := <-closes; code != ws.CloseInternalError {
			t.Fatalf("OnClose code %d", code)
		}
		good.writeFrame(true, ws.OpText, []byte("still here"))
		good.expectText("still here")
		waitFor(t, "connection count", func() bool { return s.ep.Conns() == 1 })
	})
}

func TestPingPong(t *testing.T) {
	pongs := make(chan string, 4)
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, ws.Config{
			OnPong: func(c *ws.Conn, data []byte) { pongs <- string(data) },
			OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
				if string(data) == "ping me" {
					c.Ping([]byte("from server"))
				}
			},
		}, opts{})
		c, _ := s.dial(dialOpts{})
		c.deadline(5 * time.Second)
		c.writeFrame(true, ws.OpPing, []byte("hi"))
		if _, op, pl, _ := c.readFrame(); op != ws.OpPong || string(pl) != "hi" {
			t.Fatalf("pong %d %q", op, pl)
		}
		c.writeFrame(true, ws.OpText, []byte("ping me"))
		if _, op, pl, _ := c.readFrame(); op != ws.OpPing || string(pl) != "from server" {
			t.Fatalf("ping %d %q", op, pl)
		}
		c.writeFrame(true, ws.OpPong, []byte("from server"))
		select {
		case got := <-pongs:
			if got != "from server" {
				t.Fatalf("OnPong %q", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("OnPong not called")
		}
	})
}

func TestKeepAliveDropsAPeerThatStopsAnswering(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		closes := make(chan uint16, 1)
		s := startServer(t, p, ws.Config{PingInterval: 60 * time.Millisecond, PongTimeout: 120 * time.Millisecond,
			OnClose: func(c *ws.Conn, code uint16, _ []byte) { closes <- code }}, opts{})
		c, _ := s.dial(dialOpts{})
		c.deadline(5 * time.Second)
		start := time.Now()
		if _, op, _, err := c.readFrame(); err != nil || op != ws.OpPing {
			t.Fatalf("expected a ping, got op %d err %v", op, err)
		}
		if d := time.Since(start); d < 40*time.Millisecond {
			t.Fatalf("ping after only %v", d)
		}
		// we never pong: the server gives up with 1001
		if code, reason := c.expectClose(); code != ws.CloseGoingAway || reason != "ping timeout" {
			t.Fatalf("close %d %q", code, reason)
		}
		c.expectEOF()
		if code := <-closes; code != ws.CloseGoingAway {
			t.Fatalf("OnClose code %d", code)
		}
	})
	t.Run("answering keeps it alive", func(t *testing.T) {
		s := startServer(t, h1, ws.Config{PingInterval: 40 * time.Millisecond, PongTimeout: 100 * time.Millisecond}, opts{})
		c, _ := s.dial(dialOpts{})
		c.deadline(5 * time.Second)
		for i := 0; i < 5; i++ { // five pings, all answered, well past PongTimeout in total
			_, op, pl, err := c.readFrame()
			if err != nil || op != ws.OpPing {
				t.Fatalf("round %d: op %d err %v", i, op, err)
			}
			c.writeFrame(true, ws.OpPong, pl)
		}
	})
}

func TestShutdownSaysGoingAway(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		closes := make(chan uint16, 1)
		s := startServer(t, p, ws.Config{OnClose: func(c *ws.Conn, code uint16, _ []byte) { closes <- code }}, opts{})
		c, _ := s.dial(dialOpts{})
		c.deadline(5 * time.Second)
		go s.sys.Stop()
		if code, reason := c.expectClose(); code != ws.CloseGoingAway || reason != "server shutting down" {
			t.Fatalf("close %d %q", code, reason)
		}
		c.expectEOF()
		if code := <-closes; code != ws.CloseGoingAway {
			t.Fatalf("OnClose code %d", code)
		}
	})
}

func TestFirstFrameSentWithTheHandshake(t *testing.T) {
	for _, p := range []proto{h1, h1TLS} {
		t.Run(p.String(), func(t *testing.T) {
			s := startServer(t, p, echoConfig(), opts{})
			raw := s.tcp()
			raw.SetDeadline(time.Now().Add(5 * time.Second))
			// one write: the upgrade request and a masked text frame "early"
			req := "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
				"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
			frame := []byte{0x81, 0x85, 1, 2, 3, 4, 'e' ^ 1, 'a' ^ 2, 'r' ^ 3, 'l' ^ 4, 'y' ^ 1}
			raw.Write(append([]byte(req), frame...))
			var got []byte
			buf := make([]byte, 512)
			for !bytes.Contains(got, []byte("\x81\x05early")) {
				n, err := raw.Read(buf)
				if err != nil {
					t.Fatalf("read: %v (got %q)", err, got)
				}
				got = append(got, buf[:n]...)
			}
			if !bytes.Contains(got, []byte("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")) {
				t.Fatalf("no accept header in %q", got)
			}
		})
	}
}

// ---- pushing from another isolate ----

const (
	typeHub   = gina.TypeID(77)
	tagJoin   = gina.TagUserBase + 0x70
	tagLeave  = gina.TagUserBase + 0x71
	tagBcast  = gina.TagUserBase + 0x72
	tagShare  = gina.TagUserBase + 0x73
	tagFlood  = gina.TagUserBase + 0x74
	hubShard  = 1
	typeCount = 1
)

var hubH = gina.MakeHandle(hubShard, typeHub, 0, 1)

type hub struct {
	peers []ws.Peer
	n     *atomic.Int32
}

func hubInit(n *atomic.Int32) gina.InitHandler[hub] {
	return func(h *hub, g *gina.Ctx, _ []byte) gina.Effect { h.n = n; return gina.WaitMessage() }
}

func hubHandler(h *hub, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagJoin:
		p := *gina.PayloadAs[ws.Peer](m)
		h.peers = append(h.peers, p)
		h.n.Store(int32(len(h.peers)))
		ws.PushText(g, p, "welcome")
	case tagLeave:
		p := *gina.PayloadAs[ws.Peer](m)
		for i, q := range h.peers {
			if q == p {
				h.peers = append(h.peers[:i], h.peers[i+1:]...)
				break
			}
		}
		h.n.Store(int32(len(h.peers)))
	case tagBcast: // one copy per peer
		for _, p := range h.peers {
			ws.PushText(g, p, string(g.Data()))
		}
	case tagFlood: // 200 pushes to each peer within one turn: more than a small mailbox holds
		for _, p := range h.peers {
			for i := 0; i < 200; i++ {
				ws.PushText(g, p, fmt.Sprintf("f%03d", i))
			}
		}
	case tagShare: // one copy for all
		sh, _ := ws.NewShared(ws.OpText, g.Data())
		for _, p := range h.peers {
			ws.PushShared(g, p, sh)
		}
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// hubOpts runs the HTTP server on shard 0 and the hub alone on shard 1.
func hubOpts(n *atomic.Int32) opts {
	return opts{post: func(spec *gina.SystemSpec) {
		spec.Types = append(spec.Types, gina.RegisterType(typeHub, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 256}, hubInit(n), hubHandler))
		spec.PoolSlots = max(spec.PoolSlots, 8192)
		spec.Shards = append(spec.Shards, gina.ShardSpec{Boot: []gina.SpawnSpec{{Type: typeHub, Group: gina.GroupRoot}}})
	}}
}

func hubConfig() ws.Config {
	return ws.Config{
		OnOpen: func(c *ws.Conn) { p := c.Peer(); gina.Send(c.Gina(), hubH, tagJoin, &p) },
		OnClose: func(c *ws.Conn, _ uint16, _ []byte) {
			p := c.Peer()
			gina.Send(c.Gina(), hubH, tagLeave, &p)
		},
		OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
			if rest, ok := bytes.CutPrefix(data, []byte("bcast ")); ok {
				c.Gina().SendRaw(hubH, tagBcast, rest)
				return
			}
			if bytes.Equal(data, []byte("flood")) {
				c.Gina().SendRaw(hubH, tagFlood, nil)
				return
			}
			if rest, ok := bytes.CutPrefix(data, []byte("share ")); ok {
				c.Gina().SendRaw(hubH, tagShare, rest)
				return
			}
			c.Send(op, data)
		},
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPushFromAnotherShardAndBroadcast(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		var n atomic.Int32
		s := startServer(t, p, hubConfig(), hubOpts(&n))
		var cs []*client
		for i := 0; i < 3; i++ {
			c, _ := s.dial(dialOpts{})
			c.deadline(10 * time.Second)
			c.expectText("welcome") // pushed by the hub on shard 1 while the connection waits for the peer
			cs = append(cs, c)
		}
		waitFor(t, "hub to know 3 peers", func() bool { return n.Load() == 3 })

		cs[0].writeFrame(true, ws.OpText, []byte("bcast hello all"))
		for i, c := range cs {
			if op, got := c.readMsg(); op != ws.OpText || string(got) != "hello all" {
				t.Fatalf("client %d got %d %q", i, op, got)
			}
		}
		// a burst arrives complete and in order
		for i := 0; i < 50; i++ {
			cs[1].writeFrame(true, ws.OpText, []byte(fmt.Sprintf("bcast m%02d", i)))
		}
		for i, c := range cs {
			for j := 0; j < 50; j++ {
				if _, got := c.readMsg(); string(got) != fmt.Sprintf("m%02d", j) {
					t.Fatalf("client %d message %d: %q", i, j, got)
				}
			}
		}
		// a client that leaves is dropped from the hub
		cs[2].writeFrame(true, ws.OpClose, []byte{0x03, 0xe8})
		cs[2].expectClose()
		waitFor(t, "hub to drop the closed peer", func() bool { return n.Load() == 2 })
		cs[0].writeFrame(true, ws.OpText, []byte("bcast still here"))
		cs[1].expectText("still here")
	})
}

func TestLongPushesAndSharedBroadcast(t *testing.T) {
	forAll(t, func(t *testing.T, p proto) {
		var n atomic.Int32
		s := startServer(t, p, ws.Config{MaxMessageSize: 1 << 20, OnOpen: hubConfig().OnOpen, OnClose: hubConfig().OnClose,
			OnMessage: hubConfig().OnMessage}, hubOpts(&n))
		var cs []*client
		for i := 0; i < 3; i++ {
			c, _ := s.dial(dialOpts{})
			c.deadline(10 * time.Second)
			c.expectText("welcome")
			cs = append(cs, c)
		}
		waitFor(t, "hub to know 3 peers", func() bool { return n.Load() == 3 })

		// every length around the inline limit, through both ways of pushing
		for _, size := range []int{94, 95, 96, 97, 5000, 200_000} {
			text := strings.Repeat("é", size/2) + strings.Repeat("x", size%2) // valid UTF-8, size bytes
			for _, verb := range []string{"bcast ", "share "} {
				cs[0].writeFrame(true, ws.OpText, []byte(verb+text))
				for i, c := range cs {
					if op, got := c.readMsg(); op != ws.OpText || string(got) != text {
						t.Fatalf("%s%d bytes, client %d: op %d, %d bytes back", verb, size, i, op, len(got))
					}
				}
			}
		}
		// the message that was too long to push before still arrives in order with small ones
		for i := 0; i < 20; i++ {
			text := fmt.Sprintf("n%02d", i)
			if i%4 == 0 {
				text += strings.Repeat("-", 30000)
			}
			cs[1].writeFrame(true, ws.OpText, []byte("share "+text))
		}
		for j := 0; j < 20; j++ {
			_, got := cs[2].readMsg()
			if !strings.HasPrefix(string(got), fmt.Sprintf("n%02d", j)) {
				t.Fatalf("message %d: %.8q", j, got)
			}
		}
	})
}

func TestSharedMessageChecks(t *testing.T) {
	if _, err := ws.NewShared(ws.OpText, []byte{0xff}); err != ws.ErrBadMessage {
		t.Fatalf("bad UTF-8: %v", err)
	}
	if _, err := ws.NewShared(ws.OpPing, nil); err != ws.ErrBadOpcode {
		t.Fatalf("control opcode: %v", err)
	}
}

// ---- HTTP/2 specifics ----

func TestManyStreamsShareOneHTTP2Connection(t *testing.T) {
	for _, p := range []proto{h2c, h2TLS} {
		t.Run(p.String(), func(t *testing.T) {
			var n atomic.Int32
			s := startServer(t, p, hubConfig(), hubOpts(&n))
			c := s.openH2(h2opts{})
			var cs []*client
			for _, id := range []uint32{1, 3, 5} {
				st, _ := c.connect(s, id, dialOpts{})
				if st == nil {
					t.Fatalf("stream %d refused", id)
				}
				cl := &client{t: t, rw: st, raw: c.raw, br: bufioReader(st)}
				cs = append(cs, cl)
			}
			c.raw.SetDeadline(time.Now().Add(10 * time.Second))
			for _, cl := range cs {
				cl.expectText("welcome")
			}
			// independent echo, interleaved
			for i, cl := range cs {
				cl.writeFrame(true, ws.OpText, []byte(fmt.Sprintf("stream %d", i)))
			}
			for i, cl := range cs {
				cl.expectText(fmt.Sprintf("stream %d", i))
			}
			// one stream closes; the others carry on, and a broadcast reaches only the living
			cs[1].writeFrame(true, ws.OpClose, []byte{0x03, 0xe8})
			cs[1].expectClose()
			cs[1].expectEOF()
			waitFor(t, "hub to drop the closed stream", func() bool { return n.Load() == 2 })
			cs[0].writeFrame(true, ws.OpText, []byte("bcast after close"))
			cs[0].expectText("after close")
			cs[2].expectText("after close")
		})
	}
}

func TestHTTP2FlowControlHoldsBackWebSocketData(t *testing.T) {
	s := startServer(t, h2c, echoConfig(), opts{})
	c := s.openH2(h2opts{initWin: 1000, manual: true})
	st, _ := c.connect(s, 1, dialOpts{})
	if st == nil {
		t.Fatal("refused")
	}
	cl := &client{t: t, rw: st, raw: c.raw, br: bufioReader(st)}
	payload := bytes.Repeat([]byte("flow"), 2500) // 10000 bytes: ten windows of ours
	cl.writeFrame(true, ws.OpBinary, payload)

	// Only our 1000-byte window may be used; the rest waits for WINDOW_UPDATE.
	read := func(d time.Duration) {
		c.raw.SetReadDeadline(time.Now().Add(d))
		for {
			if err := c.pump(); err != nil {
				break
			}
		}
		c.raw.SetReadDeadline(time.Time{})
	}
	read(300 * time.Millisecond)
	if len(st.buf) != 1000 {
		t.Fatalf("server sent %d bytes into a 1000-byte window", len(st.buf))
	}
	const frameLen = 4 + 10000 // 0x82 0x7e <len16>, then the payload
	for i := 0; len(st.buf) < frameLen && i < 20; i++ {
		prev := len(st.buf)
		st.grant(2500)
		read(100 * time.Millisecond)
		if got := len(st.buf) - prev; got > 2500 {
			t.Fatalf("server sent %d bytes against a 2500-byte grant", got)
		}
	}
	c.raw.SetDeadline(time.Now().Add(5 * time.Second))
	if op, got := cl.readMsg(); op != ws.OpBinary || !bytes.Equal(got, payload) {
		t.Fatalf("echo through a small window: op %d, %d bytes", op, len(got))
	}
}

func TestHTTP2WebSocketAndOrdinaryRequestsMix(t *testing.T) {
	s := startServer(t, h2c, echoConfig(), opts{})
	c := s.openH2(h2opts{})
	st, _ := c.connect(s, 1, dialOpts{})
	cl := &client{t: t, rw: st, raw: c.raw, br: bufioReader(st)}
	// an ordinary GET /plain on stream 3 while the WebSocket is open
	blk := append(append(append(lit(":method", "GET"), lit(":scheme", "http")...), lit(":authority", s.addr())...), lit(":path", "/plain")...)
	c.writeFrame(1, 4|1, 3, blk)
	plain := &h2stream{c: c, id: 3}
	c.streams[3] = plain
	c.raw.SetDeadline(time.Now().Add(5 * time.Second))
	cl.writeFrame(true, ws.OpText, []byte("ws still ok"))
	cl.expectText("ws still ok")
	for !plain.eof {
		if err := c.pump(); err != nil {
			t.Fatal(err)
		}
	}
	if plain.status != 200 || string(plain.buf) != "plain" {
		t.Fatalf("plain: %d %q", plain.status, plain.buf)
	}
}

func TestHTTP2StreamResetEndsTheWebSocket(t *testing.T) {
	closes := make(chan uint16, 1)
	s := startServer(t, h2c, ws.Config{OnClose: func(c *ws.Conn, code uint16, _ []byte) { closes <- code }}, opts{})
	c := s.openH2(h2opts{})
	st, _ := c.connect(s, 1, dialOpts{})
	if st == nil {
		t.Fatal("refused")
	}
	c.writeFrame(3, 0, 1, []byte{0, 0, 0, 8}) // RST_STREAM(CANCEL)
	select {
	case code := <-closes:
		if code != ws.CloseAbnormal {
			t.Fatalf("code %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnClose not called after RST_STREAM")
	}
}

// ---- many connections, several shards ----

func TestManyConnectionsAcrossShards(t *testing.T) {
	for _, p := range []proto{h1, h1TLS, h2c} {
		t.Run(p.String(), func(t *testing.T) {
			s := startServer(t, p, echoConfig(), opts{shards: 3})
			var wg sync.WaitGroup
			var fails atomic.Int32
			for w := 0; w < 24; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					c, _ := s.dial(dialOpts{})
					if c == nil {
						fails.Add(1)
						return
					}
					c.deadline(15 * time.Second)
					for i := 0; i < 40; i++ {
						want := fmt.Sprintf("w%d m%d %s", w, i, strings.Repeat("z", i*37))
						if err := c.tryWriteFrame(true, ws.OpText, []byte(want)); err != nil {
							fails.Add(1)
							return
						}
						if op, got, err := c.tryReadMsg(); err != nil || op != ws.OpText || string(got) != want {
							fails.Add(1)
							return
						}
					}
				}(w)
			}
			wg.Wait()
			if n := fails.Load(); n != 0 {
				t.Fatalf("%d of 24 clients failed", n)
			}
		})
	}
}

// ---- Upgrade as a building block ----

func TestUpgradeLetsTheHandlerDecide(t *testing.T) {
	ep := ws.New(ws.Config{OnMessage: func(c *ws.Conn, op ws.Opcode, data []byte) {
		c.SendText(fmt.Sprintf("%v:%s", c.Data, data))
	}})
	forAll(t, func(t *testing.T, p proto) {
		s := startServer(t, p, ws.Config{}, opts{routes: func(r *ghttp.Router, _ *ws.Endpoint) {
			r.GET("/room/:name", func(c *ghttp.Context) {
				if c.Param("name") == "closed" {
					c.String(403, "room closed\n")
					return
				}
				if conn, ok := ep.Upgrade(c); ok {
					conn.Data = c.Param("name") // visible to every later callback
				}
			})
		}})
		if c, hs := s.dial(dialOpts{path: "/room/closed"}); c != nil || hs.status != 403 {
			t.Fatalf("closed room: %d", hs.status)
		}
		c, _ := s.dial(dialOpts{path: "/room/lobby"})
		c.deadline(5 * time.Second)
		c.writeFrame(true, ws.OpText, []byte("hi"))
		c.expectText("lobby:hi")
	})
}

// Compile-time checks that the pieces fit: Conn is a Tunnel, and the h2 server
// takes the option.
var _ ghttp.Tunnel = (*ws.Conn)(nil)
var _ = http2.Config{ExtendedConnect: true}
var _ net.Conn
