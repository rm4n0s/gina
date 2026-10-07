//go:build linux

package http_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	ghttp "github.com/rm4n0s/gina/extensions/http"
)

func chunked(parts ...string) string {
	var b strings.Builder
	for _, p := range parts {
		fmt.Fprintf(&b, "%x\r\n%s\r\n", len(p), p)
	}
	return b.String() + "0\r\n\r\n"
}

func postChunked(path string, parts ...string) string {
	return "POST " + path + " HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n" + chunked(parts...)
}

func TestChunkedRequestBodies(t *testing.T) {
	h := start(t, ghttp.Config{ReadBufSize: 64}, 1)
	c := h.dial(0)
	if r := h.roundTrip(c, postChunked("/echo", "hello ", "chunked ", "world")); r.status != 200 || r.body != "hello chunked world" {
		t.Fatalf("%d %q", r.status, r.body)
	}
	// pipelined: a chunked POST followed by a GET in the same write
	h.write(c, postChunked("/echo", "one", "two")+get("/")+postChunked("/echo", "x"))
	b, _ := h.read(c, nil, hasResponses(3), 3*time.Second)
	var got []string
	for i := 0; i < 3; i++ {
		r, rest, ok := splitResponse(b)
		if !ok {
			t.Fatalf("response %d missing in %q", i, b)
		}
		got, b = append(got, r.body), rest
	}
	if got[0] != "onetwo" || got[1] != "hello\n" || got[2] != "x" {
		t.Fatalf("pipelined: %q", got)
	}
	// a body larger than the read buffer, written in small pieces
	big := strings.Repeat("0123456789", 5000)
	req := "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n" + chunked(big[:20000], big[20000:])
	for i := 0; i < len(req); i += 1500 {
		h.write(c, req[i:min(i+1500, len(req))])
	}
	b, _ = h.read(c, nil, hasResponses(1), 3*time.Second)
	if r, _, ok := splitResponse(b); !ok || r.status != 200 || r.body != big {
		t.Fatalf("large chunked body: ok=%v status=%d len=%d", ok, r.status, len(r.body))
	}
}

func TestChunkedRequestErrors(t *testing.T) {
	h := start(t, ghttp.Config{MaxBodyBytes: 1000}, 1)
	for _, tc := range []struct {
		name, req string
		status    int
	}{
		{"bad size", "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nab\r\n0\r\n\r\n", 400},
		{"too large", postChunked("/echo", strings.Repeat("a", 600), strings.Repeat("b", 600)), 413},
		{"TE+CL", "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n5\r\nhello\r\n0\r\n\r\n", 400},
	} {
		c := h.dial(0)
		r := h.roundTrip(c, tc.req)
		if r.status != tc.status {
			t.Errorf("%s: %d, want %d", tc.name, r.status, tc.status)
		}
	}
	if r := h.roundTrip(h.dial(0), get("/")); r.status != 200 {
		t.Fatal("server unhealthy")
	}
}

func TestPerRouteBodyLimits(t *testing.T) {
	h := start(t, ghttp.Config{MaxBodyBytes: 1000, ReadBufSize: 128}, 1)
	post := func(path string, n int) resp {
		c := h.dial(0)
		return h.roundTrip(c, fmt.Sprintf("POST %s HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n%s", path, n, strings.Repeat("a", n)))
	}
	for _, tc := range []struct {
		path   string
		n      int
		status int
	}{
		{"/echo", 1000, 200},   // the server default
		{"/echo", 1001, 413},   // just over it
		{"/big", 250_000, 200}, // raised for one route
		{"/big", 300_001, 413},
		{"/small", 10, 200}, // lowered for another
		{"/small", 11, 413},
	} {
		if r := post(tc.path, tc.n); r.status != tc.status {
			t.Errorf("POST %s with %d bytes: %d, want %d", tc.path, tc.n, r.status, tc.status)
		}
	}
	// the same limits apply to chunked bodies
	if r := h.roundTrip(h.dial(0), postChunked("/small", "1234567890", "1")); r.status != 413 {
		t.Errorf("chunked over /small: %d", r.status)
	}
	if r := h.roundTrip(h.dial(0), postChunked("/big", strings.Repeat("z", 100_000), strings.Repeat("z", 100_000))); r.status != 200 || len(r.body) != 200_000 {
		t.Errorf("chunked over /big: %d len %d", r.status, len(r.body))
	}
	// an oversized announcement is refused before any body is buffered: the 413
	// comes with the headers alone
	c := h.dial(0)
	h.write(c, "POST /small HTTP/1.1\r\nHost: t\r\nContent-Length: 100000000\r\n\r\n")
	b, _ := h.read(c, nil, hasResponses(1), 2*time.Second)
	if r, _, ok := splitResponse(b); !ok || r.status != 413 {
		t.Errorf("early refusal: %q", b)
	}
}

func TestPerRouteReadTimeout(t *testing.T) {
	// the server allows 10s (default); /slow only 150ms
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	h.write(c, "POST /slow HTTP/1.1\r\nHost: t\r\nContent-Length: 10\r\n\r\nabc")
	b, _ := h.read(c, nil, hasResponses(1), 2*time.Second)
	if r, _, ok := splitResponse(b); !ok || r.status != 408 {
		t.Fatalf("slow body on /slow: %q", b)
	}
	// the same drip on another route is still waiting
	c = h.dial(0)
	h.write(c, "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 10\r\n\r\nabc")
	if b, _ := h.read(c, nil, hasResponses(1), 400*time.Millisecond); len(b) != 0 {
		t.Fatalf("/echo should still be waiting for its body, got %q", b)
	}
	h.write(c, "defghij")
	if r := func() resp {
		b, _ := h.read(c, nil, hasResponses(1), 2*time.Second)
		r, _, _ := splitResponse(b)
		return r
	}(); r.body != "abcdefghij" {
		t.Fatalf("%q", r.body)
	}
}

func TestChunkedOverTLS(t *testing.T) {
	h := startTLS(t, ghttp.Config{ReadBufSize: 64}, 1)
	c, err := h.client(nil)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("abcdefgh", 6000)
	c.Write([]byte(postChunked("/echo", "hi ", big, " end") + get("/")))
	rs := readResponses(t, c, 2)
	if rs[0].body != "hi "+big+" end" || rs[1].body != "hello\n" {
		t.Fatalf("lens %d %d", len(rs[0].body), len(rs[1].body))
	}
}
