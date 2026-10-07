package websocket

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

// ---- helpers: a client-side encoder and a server-frame decoder ----

func maskedFrame(fin bool, op Opcode, payload []byte, key [4]byte) []byte {
	b0 := byte(op)
	if fin {
		b0 |= 0x80
	}
	var f []byte
	switch n := len(payload); {
	case n < 126:
		f = append(f, b0, 0x80|byte(n))
	case n <= 0xffff:
		f = append(f, b0, 0x80|126, byte(n>>8), byte(n))
	default:
		f = append(f, b0, 0x80|127)
		f = binary.BigEndian.AppendUint64(f, uint64(n))
	}
	f = append(f, key[:]...)
	for i, c := range payload {
		f = append(f, c^key[i&3])
	}
	return f
}

type frame struct {
	op      Opcode
	payload []byte
}

// decodeFrames splits server output (unmasked frames) into frames.
func decodeFrames(t *testing.T, b []byte) []frame {
	t.Helper()
	var out []frame
	for len(b) > 0 {
		op, n, hl := Opcode(b[0]&0x0f), uint64(b[1]&0x7f), 2
		if b[0]&0x80 == 0 {
			t.Fatal("server sent a fragment")
		}
		switch n {
		case 126:
			n, hl = uint64(binary.BigEndian.Uint16(b[2:])), 4
		case 127:
			n, hl = binary.BigEndian.Uint64(b[2:]), 10
		}
		out = append(out, frame{op, append([]byte(nil), b[hl:hl+int(n)]...)})
		b = b[hl+int(n):]
	}
	return out
}

type msg struct {
	op   Opcode
	data string
}

// newTestConn returns a Conn wired to record messages, with no gina behind it.
func newTestConn(cfg Config) (*Conn, *[]msg) {
	var got []msg
	cfg.OnMessage = func(c *Conn, op Opcode, data []byte) { got = append(got, msg{op, string(data)}) }
	c := &Conn{e: New(cfg), now: 1_000}
	c.opened = true
	return c, &got
}

func feed(c *Conn, b []byte, rng *rand.Rand) {
	for len(b) > 0 {
		n := len(b)
		if rng != nil {
			n = 1 + rng.Intn(min(len(b), 40))
		}
		c.receive(b[:n:n])
		b = b[n:]
	}
}

func TestUnmaskAllOffsets(t *testing.T) {
	key := [4]byte{0x12, 0x34, 0x56, 0x78}
	plain := make([]byte, 100)
	for i := range plain {
		plain[i] = byte(i * 7)
	}
	for pos := 0; pos < 8; pos++ {
		for n := 0; n <= 70; n++ {
			b := append([]byte(nil), plain[:n]...)
			for i := range b {
				b[i] ^= key[(pos+i)&3]
			}
			unmask(b, key, pos)
			if !bytes.Equal(b, plain[:n]) {
				t.Fatalf("pos %d len %d", pos, n)
			}
		}
	}
}

func TestMessagesSurviveAnySplit(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 200; round++ {
		c, got := newTestConn(Config{MaxMessageSize: 1 << 20})
		var want []msg
		var wire, wantPongs []byte
		for i, k := 0, 1+rng.Intn(8); i < k; i++ {
			size := []int{0, 1, 5, 125, 126, 127, 1000, 65535, 65536, 70000}[rng.Intn(10)]
			op := OpBinary
			data := make([]byte, size)
			rng.Read(data)
			if rng.Intn(2) == 0 {
				op = OpText
				data = bytes.Repeat([]byte("héllo €𝄞 "), size/12+1)[:0]
				for len(data) < size {
					data = append(data, "héllo €𝄞 "...)
				}
			}
			key := [4]byte{byte(rng.Int()), byte(rng.Int()), byte(rng.Int()), byte(rng.Int())}
			want = append(want, msg{op, string(data)})
			if rng.Intn(2) == 0 || len(data) < 3 { // one frame
				wire = append(wire, maskedFrame(true, op, data, key)...)
				continue
			}
			// fragments, with a ping between them
			cut := 1 + rng.Intn(len(data)-1) // anywhere, even inside a rune
			wire = append(wire, maskedFrame(false, op, data[:cut], key)...)
			ping := []byte{byte(i), 9}
			wire = append(wire, maskedFrame(true, OpPing, ping, key)...)
			wantPongs = append(wantPongs, 0x8a, 2, byte(i), 9)
			wire = append(wire, maskedFrame(true, OpContinuation, data[cut:], key)...)
		}
		feed(c, wire, rng)
		if c.done {
			t.Fatalf("round %d: connection failed: %v", round, decodeFrames(t, c.out))
		}
		if len(*got) != len(want) {
			t.Fatalf("round %d: %d messages, want %d", round, len(*got), len(want))
		}
		for i := range want {
			if (*got)[i] != want[i] {
				t.Fatalf("round %d message %d differs (op %d/%d, len %d/%d)", round, i, (*got)[i].op, want[i].op, len((*got)[i].data), len(want[i].data))
			}
		}
		if !bytes.Equal(c.out, wantPongs) {
			t.Fatalf("round %d: pongs % x, want % x", round, c.out, wantPongs)
		}
	}
}

func closeCode(t *testing.T, c *Conn) (uint16, string) {
	t.Helper()
	fr := decodeFrames(t, c.out)
	if len(fr) == 0 || fr[len(fr)-1].op != OpClose {
		t.Fatalf("no close frame in %v", fr)
	}
	p := fr[len(fr)-1].payload
	if len(p) < 2 {
		t.Fatalf("close frame without code: %v", p)
	}
	return binary.BigEndian.Uint16(p), string(p[2:])
}

func TestProtocolErrors(t *testing.T) {
	key := [4]byte{1, 2, 3, 4}
	long := bytes.Repeat([]byte{'x'}, 126)
	cases := []struct {
		name string
		wire []byte
		code uint16
		cfg  Config
	}{
		{"unmasked", []byte{0x81, 0x01, 'x'}, CloseProtocolError, Config{}},
		{"reserved bit", append([]byte{0x91, 0x80}, key[:]...), CloseProtocolError, Config{}},
		{"unknown opcode", maskedFrame(true, 3, nil, key), CloseProtocolError, Config{}},
		{"fragmented ping", maskedFrame(false, OpPing, nil, key), CloseProtocolError, Config{}},
		{"long ping", maskedFrame(true, OpPing, long, key), CloseProtocolError, Config{}},
		{"stray continuation", maskedFrame(true, OpContinuation, []byte("x"), key), CloseProtocolError, Config{}},
		{"text inside fragments", append(maskedFrame(false, OpText, []byte("a"), key), maskedFrame(true, OpText, []byte("b"), key)...), CloseProtocolError, Config{}},
		{"length top bit", append([]byte{0x82, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0}, key[:]...), CloseProtocolError, Config{}},
		{"bad utf8", maskedFrame(true, OpText, []byte{0xff, 0xfe}, key), CloseInvalidPayload, Config{}},
		{"bad utf8 in first fragment", maskedFrame(false, OpText, []byte{'a', 0xc0, 0x80}, key), CloseInvalidPayload, Config{}},
		{"truncated rune at the end", append(maskedFrame(false, OpText, []byte{0xe2, 0x82}, key), maskedFrame(true, OpContinuation, []byte("x"), key)...), CloseInvalidPayload, Config{}},
		{"too big", maskedFrame(true, OpBinary, make([]byte, 300), key), CloseTooBig, Config{MaxMessageSize: 256}},
		{"too big in pieces", append(maskedFrame(false, OpBinary, make([]byte, 200), key), maskedFrame(true, OpContinuation, make([]byte, 100), key)...), CloseTooBig, Config{MaxMessageSize: 256}},
		{"close with one byte", maskedFrame(true, OpClose, []byte{3}, key), CloseProtocolError, Config{}},
		{"close with reserved code", maskedFrame(true, OpClose, []byte{0x03, 0xed}, key), CloseProtocolError, Config{}}, // 1005 must not be sent
		{"close with bad reason", maskedFrame(true, OpClose, []byte{0x03, 0xe8, 0xff}, key), CloseInvalidPayload, Config{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, got := newTestConn(tc.cfg)
			feed(c, tc.wire, nil)
			if !c.done {
				t.Fatal("connection not finished")
			}
			if code, _ := closeCode(t, c); code != tc.code {
				t.Fatalf("close code %d, want %d", code, tc.code)
			}
			if len(*got) != 0 {
				t.Fatalf("delivered %v", *got)
			}
			// nothing after the failure is read
			n := len(c.out)
			feed(c, maskedFrame(true, OpPing, nil, key), nil)
			if len(c.out) != n {
				t.Fatal("input after a failure was answered")
			}
		})
	}
}

func TestMidFragmentUTF8IsRefusedAtTheBadByte(t *testing.T) {
	// A bad byte in the first fragment must fail at once, not when the message ends.
	c, _ := newTestConn(Config{})
	feed(c, maskedFrame(false, OpText, []byte("ok \xff"), [4]byte{9, 9, 9, 9}), nil)
	if !c.done {
		t.Fatal("not refused before the final fragment")
	}
}

func TestEmptyMessagesAndSplitRunes(t *testing.T) {
	c, got := newTestConn(Config{})
	k := [4]byte{5, 6, 7, 8}
	euro := []byte("€") // e2 82 ac
	var wire []byte
	wire = append(wire, maskedFrame(true, OpText, nil, k)...)
	wire = append(wire, maskedFrame(true, OpBinary, nil, k)...)
	wire = append(wire, maskedFrame(false, OpText, euro[:1], k)...)
	wire = append(wire, maskedFrame(false, OpContinuation, euro[1:2], k)...)
	wire = append(wire, maskedFrame(true, OpContinuation, euro[2:], k)...)
	feed(c, wire, nil)
	want := []msg{{OpText, ""}, {OpBinary, ""}, {OpText, "€"}}
	if c.done || len(*got) != 3 || (*got)[0] != want[0] || (*got)[1] != want[1] || (*got)[2] != want[2] {
		t.Fatalf("got %v done=%v", *got, c.done)
	}
}

func TestCloseHandshake(t *testing.T) {
	k := [4]byte{1, 1, 1, 1}
	// the peer closes: we echo its code and finish
	c, _ := newTestConn(Config{})
	feed(c, maskedFrame(true, OpClose, []byte{0x0b, 0xb8, 'b', 'y', 'e'}, k), nil) // 3000 "bye"
	if code, _ := closeCode(t, c); code != 3000 || !c.done || !c.closeRecv {
		t.Fatalf("code %d done %v", code, c.done)
	}
	// a close without a code is answered 1000
	c, _ = newTestConn(Config{})
	feed(c, maskedFrame(true, OpClose, nil, k), nil)
	if code, _ := closeCode(t, c); code != CloseNormal || c.peerCode != CloseNoStatus {
		t.Fatalf("code %d peer %d", code, c.peerCode)
	}
	// we close first: wait for the answer, then finish; data in between is dropped
	c, got := newTestConn(Config{})
	c.Close(CloseNormal, "done")
	if c.done || c.SendText("x") != ErrClosed {
		t.Fatal("close should be pending and block sends")
	}
	feed(c, maskedFrame(true, OpText, []byte("late"), k), nil)
	if len(*got) != 0 || c.done {
		t.Fatal("data after our close was delivered")
	}
	feed(c, maskedFrame(true, OpClose, []byte{0x03, 0xe8}, k), nil)
	if !c.done {
		t.Fatal("not finished after the peer's close")
	}
}

func TestKeepAliveTimers(t *testing.T) {
	c, _ := newTestConn(Config{PingInterval: 10 * time.Second, PongTimeout: 5 * time.Second})
	c.lastRecv = c.now
	d, ok := c.Idle(c.now)
	if !ok || d != c.e.pingInterval {
		t.Fatalf("idle %v %v", d, ok)
	}
	// time passes with no traffic: Tick pings and starts the pong timer
	c.now += uint64(c.e.pingInterval)
	c.tick()
	fr := decodeFrames(t, c.out)
	if len(fr) != 1 || fr[0].op != OpPing || !c.pingOut {
		t.Fatalf("no ping: %v", fr)
	}
	if d, ok := c.Idle(c.now); !ok || d != c.e.pongTimeout {
		t.Fatalf("pong deadline %v %v", d, ok)
	}
	// any traffic cancels it
	feed(c, maskedFrame(true, OpPong, nil, [4]byte{}), nil)
	if c.pingOut {
		t.Fatal("pong did not clear the ping")
	}
	// silence again: ping, then give up
	c.out = c.out[:0]
	c.now += uint64(c.e.pingInterval)
	c.tick()
	c.now += uint64(c.e.pongTimeout)
	c.tick()
	if !c.done {
		t.Fatal("peer that never answered was not dropped")
	}
	if code, _ := closeCode(t, c); code != CloseGoingAway {
		t.Fatalf("code %d", code)
	}
}

func TestSendLimitsAndFraming(t *testing.T) {
	c, _ := newTestConn(Config{MaxQueued: 1000})
	for _, n := range []int{0, 125, 126, 65535, 65536} {
		c2, _ := newTestConn(Config{MaxQueued: 1 << 20})
		if err := c2.Send(OpBinary, make([]byte, n)); err != nil {
			t.Fatal(err)
		}
		fr := decodeFrames(t, c2.out)
		if len(fr) != 1 || len(fr[0].payload) != n {
			t.Fatalf("n=%d: %d frames", n, len(fr))
		}
	}
	if err := c.SendBinary(make([]byte, 900)); err != nil {
		t.Fatal(err)
	}
	if err := c.SendBinary(make([]byte, 900)); err != ErrQueueFull {
		t.Fatalf("second send: %v", err)
	}
	c.Sent(len(c.Outgoing())) // the first went out: room again
	if err := c.SendBinary(make([]byte, 900)); err != nil {
		t.Fatal(err)
	}
	if c.SendText("\xff") != ErrBadMessage || c.Send(OpPing, nil) != ErrBadOpcode || c.Ping(make([]byte, 126)) != ErrTooLong {
		t.Fatal("argument checks")
	}
}
