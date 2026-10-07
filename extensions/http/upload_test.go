//go:build linux

package http_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ghttp "github.com/rm4n0s/gina/extensions/http"
)

type uploadStats struct {
	pieces, bytes atomic.Int64
	biggest       atomic.Int64
	aborted       atomic.Int32
	lastSeen      atomic.Int32
}

func uploadRoutes(st *uploadStats) *ghttp.Router {
	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.String(200, "hello\n") })
	hashing := func(c *ghttp.Context) {
		h := sha256.New()
		n := 0
		c.OnBody(func(c *ghttp.Context, chunk []byte, last bool) {
			st.pieces.Add(1)
			if int64(len(chunk)) > st.biggest.Load() {
				st.biggest.Store(int64(len(chunk)))
			}
			h.Write(chunk)
			n += len(chunk)
			if c.BodyAborted() {
				st.aborted.Add(1)
				return
			}
			if last {
				st.lastSeen.Add(1)
				c.String(200, fmt.Sprintf("%d %x", n, h.Sum(nil)[:6]))
			}
		})
	}
	r.POST("/upload", hashing).StreamBody()
	r.PUT("/upload", hashing).StreamBody()
	r.POST("/limited", hashing).StreamBody().MaxBody(100_000)
	r.POST("/refuse", func(c *ghttp.Context) { c.StopBody(403, "no uploads for you\n") }).StreamBody()
	r.POST("/refuse-late", func(c *ghttp.Context) {
		got := 0
		c.OnBody(func(c *ghttp.Context, chunk []byte, last bool) {
			if got += len(chunk); got > 5000 {
				c.StopBody(413, "too big for me\n")
			}
		})
	}).StreamBody()
	r.POST("/panics", func(c *ghttp.Context) {
		c.OnBody(func(c *ghttp.Context, chunk []byte, last bool) { panic("callback") })
	}).StreamBody()
	r.POST("/buffered", hashing) // not a stream route: the buffered body is delivered in one call
	r.POST("/slow", hashing).StreamBody().ReadTimeout(200 * time.Millisecond)
	return r
}

func sum6(b []byte) string { s := sha256.Sum256(b); return fmt.Sprintf("%x", s[:6]) }

func TestStreamedUploadContentLength(t *testing.T) {
	st := &uploadStats{}
	h := startWith(t, ghttp.Config{ReadBufSize: 512, MaxBodyBytes: 1000}, uploadRoutes(st), 1) // default limit far below the upload
	c := h.dial(0)
	body := pattern(3 << 20)
	req := fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n", len(body))
	h.write(c, req+string(body[:100]))
	for i := 100; i < len(body); i += 40_000 {
		h.write(c, string(body[i:min(i+40_000, len(body))]))
	}
	b, _ := h.read(c, nil, hasResponses(1), 10*time.Second)
	r, _, ok := splitResponse(b)
	if !ok || r.status != 200 || r.body != fmt.Sprintf("%d %s", len(body), sum6(body)) {
		t.Fatalf("response %q", b)
	}
	if st.biggest.Load() > 64<<10 || st.pieces.Load() < 40 {
		t.Fatalf("body was not streamed: %d pieces, biggest %d bytes", st.pieces.Load(), st.biggest.Load())
	}
	// the connection is reusable, and a pipelined request right behind the body is not lost
	h.write(c, fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n\r\nhello%s", get("/")))
	b, _ = h.read(c, nil, hasResponses(2), 5*time.Second)
	r1, rest, _ := splitResponse(b)
	r2, _, _ := splitResponse(rest)
	if r1.body != "5 "+sum6([]byte("hello")) || r2.body != "hello\n" {
		t.Fatalf("pipelined: %q / %q", r1.body, r2.body)
	}
}

func TestStreamedUploadChunked(t *testing.T) {
	st := &uploadStats{}
	h := startWith(t, ghttp.Config{ReadBufSize: 512}, uploadRoutes(st), 1)
	c := h.dial(0)
	body := pattern(1_000_000)
	var wire strings.Builder
	wire.WriteString("PUT /upload HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n")
	for i := 0; i < len(body); i += 7_001 {
		p := body[i:min(i+7_001, len(body))]
		fmt.Fprintf(&wire, "%x\r\n%s\r\n", len(p), p)
	}
	wire.WriteString("0\r\nX-Trailer: yes\r\n\r\n")
	w := wire.String()
	for i := 0; i < len(w); i += 30_000 {
		h.write(c, w[i:min(i+30_000, len(w))])
	}
	b, _ := h.read(c, nil, hasResponses(1), 10*time.Second)
	if r, _, ok := splitResponse(b); !ok || r.status != 200 || r.body != fmt.Sprintf("%d %s", len(body), sum6(body)) {
		t.Fatalf("response %q", b)
	}
	// and the connection carries on
	if r := h.roundTrip(c, get("/")); r.body != "hello\n" {
		t.Fatalf("after chunked upload: %q", r.body)
	}
}

func TestStreamedUploadLimitsAndEarlyAnswers(t *testing.T) {
	st := &uploadStats{}
	h := startWith(t, ghttp.Config{}, uploadRoutes(st), 1)
	post := func(path string, n int) resp {
		return h.roundTrip(h.dial(0), fmt.Sprintf("POST %s HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n%s", path, n, strings.Repeat("a", n)))
	}
	// the route's own limit applies to a streamed body, from the headers on
	if r := post("/limited", 100_000); r.status != 200 {
		t.Fatalf("at the limit: %d", r.status)
	}
	if r := post("/limited", 100_001); r.status != 413 {
		t.Fatalf("over the limit: %d", r.status)
	}
	// chunked: refused as soon as the total crosses it
	big := strings.Repeat("b", 60_000)
	req := fmt.Sprintf("POST /limited HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n%x\r\n%s\r\n0\r\n\r\n", len(big), big, len(big), big)
	if r := h.roundTrip(h.dial(0), req); r.status != 413 {
		t.Fatalf("chunked over the limit: %d", r.status)
	}
	// the handler refuses before reading anything: the answer comes with the headers
	c := h.dial(0)
	h.write(c, "POST /refuse HTTP/1.1\r\nHost: t\r\nContent-Length: 50000000\r\n\r\n")
	b, closed := h.read(c, nil, hasResponses(1), 2*time.Second)
	if r, _, ok := splitResponse(b); !ok || r.status != 403 || r.body != "no uploads for you\n" || !strings.Contains(r.headers, "Connection: close") {
		t.Fatalf("early refusal: %q", b)
	}
	if !closed {
		if _, closed = h.read(c, nil, nil, time.Second); !closed {
			t.Fatal("connection left open after an unread body")
		}
	}
	// refusal part-way through
	if r := post("/refuse-late", 20_000); r.status != 413 || r.body != "too big for me\n" {
		t.Fatalf("late refusal: %d %q", r.status, r.body)
	}
	// a panicking callback is a 500, not a dead shard
	if r := post("/panics", 10); r.status != 500 {
		t.Fatalf("panic: %d", r.status)
	}
	// an ordinary route's handler can use OnBody too: one call with the whole body
	before := st.pieces.Load()
	if r := post("/buffered", 1234); r.status != 200 || r.body != "1234 "+sum6(bytes.Repeat([]byte("a"), 1234)) || st.pieces.Load() != before+1 {
		t.Fatalf("buffered route: %d %q pieces %d", r.status, r.body, st.pieces.Load()-before)
	}
	// a streaming route also serves requests without a body
	r := h.roundTrip(h.dial(0), "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n")
	if r.status != 200 || r.body != "0 "+sum6(nil) {
		t.Fatalf("empty body: %d %q", r.status, r.body)
	}
	if r := h.roundTrip(h.dial(0), get("/")); r.status != 200 {
		t.Fatal("server unhealthy")
	}
}

func TestStreamedUploadAbortedAndTimeouts(t *testing.T) {
	st := &uploadStats{}
	h := startWith(t, ghttp.Config{}, uploadRoutes(st), 1)
	// the client hangs up in the middle: the handler is told
	c := h.dial(0)
	h.write(c, "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: 100000\r\n\r\n"+strings.Repeat("x", 5000))
	h.read(c, nil, nil, 100*time.Millisecond)
	closeFD(c)
	for i := 0; i < 200 && st.aborted.Load() == 0; i++ {
		h.step()
		time.Sleep(5 * time.Millisecond)
	}
	if st.aborted.Load() != 1 || st.lastSeen.Load() != 0 {
		t.Fatalf("aborted=%d completed=%d", st.aborted.Load(), st.lastSeen.Load())
	}
	// a stalled upload hits the route's inactivity timeout (and a slow but steady one does not)
	c = h.dial(0)
	h.write(c, "POST /slow HTTP/1.1\r\nHost: t\r\nContent-Length: 12\r\n\r\n")
	var got []byte
	for _, piece := range []string{"abcd", "efgh", "ijkl"} { // 3 x 120ms: longer than the timeout in total, never idle for 200ms
		time.Sleep(120 * time.Millisecond)
		h.write(c, piece)
		var closed bool
		if got, closed = h.read(c, got, nil, 5*time.Millisecond); closed {
			t.Fatalf("steady upload was cut off: %q", got)
		}
	}
	got, _ = h.read(c, got, hasResponses(1), 2*time.Second)
	if r, _, ok := splitResponse(got); !ok || r.status != 200 || r.body != "12 "+sum6([]byte("abcdefghijkl")) {
		t.Fatalf("steady slow upload: %q", got)
	}
	c = h.dial(0)
	h.write(c, "POST /slow HTTP/1.1\r\nHost: t\r\nContent-Length: 12\r\n\r\nabcd")
	b, _ := h.read(c, nil, hasResponses(1), 2*time.Second)
	if r, _, ok := splitResponse(b); !ok || r.status != 408 {
		t.Fatalf("stalled upload: %q", b)
	}
}

func TestStreamedUploadOverTLS(t *testing.T) {
	st := &uploadStats{}
	h := startTLSWith(t, ghttp.Config{ReadBufSize: 256}, uploadRoutes(st))
	c, err := h.client(nil)
	if err != nil {
		t.Fatal(err)
	}
	body := pattern(900_000)
	c.Write([]byte(fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n", len(body))))
	c.Write(body)
	if r := readResponses(t, c, 1)[0]; r.status != 200 || r.body != fmt.Sprintf("%d %s", len(body), sum6(body)) {
		t.Fatalf("%d %q", r.status, r.body)
	}
	var wire strings.Builder
	wire.WriteString("POST /upload HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n")
	fmt.Fprintf(&wire, "%x\r\n%s\r\n0\r\n\r\n", len(body), body)
	c.Write([]byte(wire.String() + get("/")))
	rs := readResponses(t, c, 2)
	if rs[0].body != fmt.Sprintf("%d %s", len(body), sum6(body)) || rs[1].body != "hello\n" {
		t.Fatalf("chunked over TLS: %q / %q", rs[0].body, rs[1].body)
	}
}

func closeFD(fd int) { syscall.Close(fd) }
