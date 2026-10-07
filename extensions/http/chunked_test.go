package http

import (
	"bytes"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func chunkedBody(parts ...string) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(strconv.FormatInt(int64(len(p)), 16) + "\r\n" + p + "\r\n")
	}
	b.WriteString("0\r\n\r\n")
	return b.String()
}

// feed gives the decoder wire in pieces, the way reads arrive, keeping the
// buffer exactly as the server does (decoded prefix + undecoded rest).
func feed(t *testing.T, wire string, split func(rest int) int, max int) (body string, rest string, status int, done bool) {
	t.Helper()
	var ck chunkState
	var buf []byte
	in := []byte(wire)
	for !done {
		n := 0
		if len(in) > 0 {
			n = min(split(len(in)), len(in))
			buf = append(buf, in[:n]...)
			in = in[n:]
		}
		start := ck.body
		raw := buf[start:]
		i, w, d, st := ck.decode(raw, max, 1024)
		if st != 0 {
			return "", "", st, false
		}
		buf = buf[:start+w+(len(raw)-i)]
		done = d
		if n == 0 && !done {
			return string(buf[:ck.body]), string(buf[ck.body:]), 0, false // starved
		}
	}
	return string(buf[:ck.body]), string(buf[ck.body:]), 0, true
}

func TestChunkedDecodeAnySplit(t *testing.T) {
	body := chunkedBody("hello ", "chunked ", strings.Repeat("x", 3000), "!")
	want := "hello chunked " + strings.Repeat("x", 3000) + "!"
	rng := rand.New(rand.NewSource(1))
	for name, split := range map[string]func(int) int{
		"whole":   func(r int) int { return r },
		"1 byte":  func(int) int { return 1 },
		"random":  func(int) int { return 1 + rng.Intn(40) },
		"2 bytes": func(int) int { return 2 },
		"1000":    func(int) int { return 1000 },
	} {
		got, rest, st, done := feed(t, body+"GET /next", split, 1<<20)
		if st != 0 || !done || got != want || !strings.HasPrefix("GET /next", rest) { // rest: whatever pipelined bytes had been fed by the end
			t.Errorf("%s: status=%d done=%v body=%d bytes (want %d) rest=%q", name, st, done, len(got), len(want), rest)
		}
	}
}

func TestChunkedExtensionsAndTrailers(t *testing.T) {
	wire := "5;name=value;x\r\nhello\r\n6\r\n world\r\n0;last\r\nX-Sum: 1\r\nX-Other: 2\r\n\r\n"
	got, rest, st, done := feed(t, wire, func(int) int { return 3 }, 100)
	if st != 0 || !done || got != "hello world" || rest != "" {
		t.Fatalf("status=%d done=%v body=%q rest=%q", st, done, got, rest)
	}
	// uppercase hex
	got, _, st, done = feed(t, "A\r\n0123456789\r\n0\r\n\r\n", func(r int) int { return r }, 100)
	if st != 0 || !done || got != "0123456789" {
		t.Fatalf("hex: %d %v %q", st, done, got)
	}
}

func TestChunkedRejects(t *testing.T) {
	for _, c := range []struct {
		name, wire string
		status     int
	}{
		{"not hex", "zz\r\nab\r\n0\r\n\r\n", 400},
		{"empty size", "\r\nab\r\n", 400},
		{"space before ext", "2 ;x\r\nab\r\n0\r\n\r\n", 400},
		{"0x prefix", "0x2\r\nab\r\n0\r\n\r\n", 400},
		{"negative", "-1\r\nab\r\n0\r\n\r\n", 400},
		{"data not followed by CRLF", "2\r\nabXX0\r\n\r\n", 400},
		{"bare LF after data", "2\r\nab\n0\r\n\r\n", 400},
		{"bad trailer", "0\r\nnot a header\r\n\r\n", 400},
		{"NUL in extension", "2;a\x00b\r\nab\r\n0\r\n\r\n", 400},
		{"huge size", "fffffffff\r\n", 413},
		{"over the limit", "65\r\n" + strings.Repeat("a", 0x65) + "\r\n0\r\n\r\n", 413},
		{"two chunks over the limit", "40\r\n" + strings.Repeat("a", 0x40) + "\r\n40\r\n" + strings.Repeat("a", 0x40) + "\r\n0\r\n\r\n", 413},
		{"trailers too long", "0\r\n" + strings.Repeat("X: "+strings.Repeat("v", 100)+"\r\n", 12) + "\r\n", 431},
		{"endless size line", strings.Repeat("1", 2000), 400},
	} {
		for _, step := range []int{1, 7, 100000} {
			_, _, st, _ := feed(t, c.wire, func(int) int { return step }, 100)
			if st != c.status {
				t.Errorf("%s (step %d): status %d, want %d", c.name, step, st, c.status)
			}
		}
	}
}

func TestChunkedIncompleteWaits(t *testing.T) {
	for _, wire := range []string{"5\r\nhel", "5\r\nhello", "5\r\nhello\r", "5\r\nhello\r\n", "5\r\nhello\r\n0\r\n", "5\r\nhello\r\n0\r\nX: y\r\n"} {
		_, _, st, done := feed(t, wire, func(r int) int { return r }, 100)
		if st != 0 || done {
			t.Errorf("%q: status=%d done=%v, should wait for more", wire, st, done)
		}
	}
}

func TestChunkedDecodeIsLinear(t *testing.T) {
	// 100k one-byte chunks fed one byte at a time must not be quadratic.
	var b strings.Builder
	for i := 0; i < 100000; i++ {
		b.WriteString("1\r\nx\r\n")
	}
	b.WriteString("0\r\n\r\n")
	got, _, st, done := feed(t, b.String(), func(int) int { return 1 }, 1<<20)
	if st != 0 || !done || len(got) != 100000 || !bytes.Equal([]byte(got[:3]), []byte("xxx")) {
		t.Fatalf("status=%d done=%v len=%d", st, done, len(got))
	}
}

func TestParseChunkedRequestHead(t *testing.T) {
	st, consumed, _, _, req := parse("POST /up?a=b HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: Chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n")
	if st != psChunked || consumed != len("POST /up?a=b HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: Chunked\r\n\r\n") {
		t.Fatalf("st=%v consumed=%d", st, consumed)
	}
	if req.Method != "POST" || string(req.Path) != "/up" || string(req.Query) != "a=b" || req.ContentLength != -1 {
		t.Fatalf("%+v", req)
	}
}
