//go:build linux

package http_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	ghttp "github.com/rm4n0s/gina/extensions/http"
)

// pattern is a deterministic body of any length.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i>>8)
	}
	return b
}

type countingReader struct {
	r      io.Reader
	closed *atomic.Int32
	reads  *atomic.Int32
}

func (c countingReader) Read(p []byte) (int, error) { c.reads.Add(1); return c.r.Read(p) }
func (c countingReader) Close() error               { c.closed.Add(1); return nil }

func streamRoutes(r *ghttp.Router, closed, reads *atomic.Int32, dir string) {
	src := func(n int) io.Reader {
		return countingReader{bytes.NewReader(pattern(n)), closed, reads}
	}
	r.GET("/stream/:n", func(c *ghttp.Context) {
		n := 0
		fmt.Sscan(c.Param("n"), &n)
		c.SendReader(200, "application/octet-stream", int64(n), src(n))
	})
	r.GET("/unknown/:n", func(c *ghttp.Context) { // length not declared
		n := 0
		fmt.Sscan(c.Param("n"), &n)
		c.SendReader(200, "application/octet-stream", -1, countingReader{iotest.OneByteReader(bytes.NewReader(pattern(n))), closed, reads})
	})
	r.GET("/unknown-big/:n", func(c *ghttp.Context) {
		n := 0
		fmt.Sscan(c.Param("n"), &n)
		c.SendReader(200, "application/octet-stream", -1, src(n))
	})
	r.GET("/short", func(c *ghttp.Context) { // promises 1000, delivers 10
		c.SendReader(200, "text/plain", 1000, countingReader{strings.NewReader("0123456789"), closed, reads})
	})
	r.GET("/failing", func(c *ghttp.Context) {
		c.SendReader(200, "text/plain", 100000, countingReader{io.MultiReader(bytes.NewReader(pattern(40000)), iotest.ErrReader(errors.New("disk on fire"))), closed, reads})
	})
	r.GET("/panic-after-reader", func(c *ghttp.Context) {
		c.SendReader(200, "text/plain", 5, src(5))
		panic("boom")
	})
	r.GET("/file/*", func(c *ghttp.Context) { c.ServeFS(os.DirFS(dir), string(c.ParamBytes("*"))) })
	r.GET("/nocontent", func(c *ghttp.Context) { c.SendReader(204, "", 5, src(5)) })
}

func startStream(t *testing.T, cfg ghttp.Config) (*harness, *atomic.Int32, *atomic.Int32, string) {
	var closed, reads atomic.Int32
	dir := t.TempDir()
	r := ghttp.NewRouter()
	streamRoutes(r, &closed, &reads, dir)
	r.GET("/", func(c *ghttp.Context) { c.String(200, "hello\n") })
	r.GET("/ip", func(c *ghttp.Context) { c.String(200, "x") })
	h := startWith(t, cfg, r, 1)
	return h, &closed, &reads, dir
}

// readAll reads one HTTP response of unknown framing from c until the connection
// closes or the response is complete.
func (h *harness) rawResponse(fd int, req string, within time.Duration) (head string, body []byte, closed bool) {
	h.write(fd, req)
	var acc []byte
	acc, closed = h.read(fd, nil, func(b []byte) bool {
		i := bytes.Index(b, []byte("\r\n\r\n"))
		if i < 0 {
			return false
		}
		hd := string(b[:i])
		if cl := headerValue(hd, "Content-Length"); cl != "" {
			var n int
			fmt.Sscan(cl, &n)
			return len(b) >= i+4+n
		}
		if strings.Contains(strings.ToLower(hd), "transfer-encoding: chunked") {
			return bytes.HasSuffix(b, []byte("0\r\n\r\n"))
		}
		return false
	}, within)
	i := bytes.Index(acc, []byte("\r\n\r\n"))
	if i < 0 {
		return string(acc), nil, closed
	}
	return string(acc[:i]), acc[i+4:], closed
}

func dechunk(t *testing.T, b []byte) []byte {
	t.Helper()
	var out []byte
	for {
		i := bytes.Index(b, []byte("\r\n"))
		if i < 0 {
			t.Fatalf("truncated chunked body")
		}
		var n int
		if _, err := fmt.Sscanf(string(b[:i]), "%x", &n); err != nil {
			t.Fatalf("bad chunk size %q", b[:i])
		}
		b = b[i+2:]
		if n == 0 {
			if string(b) != "\r\n" {
				t.Fatalf("garbage after the last chunk: %q", b)
			}
			return out
		}
		if len(b) < n+2 || string(b[n:n+2]) != "\r\n" {
			t.Fatalf("bad chunk framing")
		}
		out = append(out, b[:n]...)
		b = b[n+2:]
	}
}

func TestStreamedResponseKnownLength(t *testing.T) {
	h, closed, _, _ := startStream(t, ghttp.Config{})
	c := h.dial(0)
	for _, n := range []int{1, 100, 32 << 10, 32<<10 + 1, 1 << 20, 3<<20 + 17} {
		head, body, _ := h.rawResponse(c, get(fmt.Sprintf("/stream/%d", n)), 10*time.Second)
		if !strings.HasPrefix(head, "HTTP/1.1 200") || headerValue(head, "Content-Length") != fmt.Sprint(n) {
			t.Fatalf("n=%d: head %q", n, head)
		}
		if sha256.Sum256(body) != sha256.Sum256(pattern(n)) {
			t.Fatalf("n=%d: body differs (%d bytes)", n, len(body))
		}
	}
	// the same connection still serves ordinary requests (keep-alive survived)
	if r := h.roundTrip(c, get("/")); r.body != "hello\n" {
		t.Fatalf("after streaming: %q", r.body)
	}
	// zero length is an ordinary empty body
	if r := h.roundTrip(c, get("/stream/0")); r.status != 200 || r.body != "" {
		t.Fatalf("zero length: %d %q", r.status, r.body)
	}
	if got := closed.Load(); got != 7 { // every source closed exactly once (6 sizes + the empty one)
		t.Fatalf("sources closed: %d", got)
	}
}

func TestStreamedResponsePipelinedAndHEAD(t *testing.T) {
	h, closed, reads, _ := startStream(t, ghttp.Config{})
	c := h.dial(0)
	// a HEAD announces the length but reads and sends nothing
	h.write(c, "HEAD /stream/5000 HTTP/1.1\r\nHost: t\r\n\r\n")
	b, _ := h.read(c, nil, func(b []byte) bool { return bytes.Contains(b, []byte("\r\n\r\n")) }, time.Second)
	if !strings.Contains(string(b), "Content-Length: 5000") || reads.Load() != 0 || closed.Load() != 1 {
		t.Fatalf("HEAD: %q reads=%d closed=%d", b, reads.Load(), closed.Load())
	}
	// pipelined: a streamed response, then another request, in one write
	h.write(c, get("/stream/70000")+get("/"))
	got, _ := h.read(c, nil, func(b []byte) bool { return bytes.HasSuffix(b, []byte("hello\n")) }, 5*time.Second)
	i := bytes.Index(got, []byte("\r\n\r\n"))
	if i < 0 || len(got) != i+4+70000+len("HTTP/1.1 200 OK\r\nDate: Mon, 02 Jan 2006 15:04:05 GMT\r\nServer: gina\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 6\r\n\r\nhello\n") {
		// lengths of the Date header are fixed, so the total is exact
		t.Fatalf("pipelined response is %d bytes", len(got))
	}
	// 204 and friends cannot carry a body: the source is dropped, not read
	if r := h.roundTrip(c, get("/nocontent")); r.status != 204 || r.body != "" {
		t.Fatalf("204: %d %q", r.status, r.body)
	}
}

func TestStreamedResponseUnknownLengthIsChunked(t *testing.T) {
	h, closed, _, _ := startStream(t, ghttp.Config{})
	c := h.dial(0)
	for _, route := range []string{"/unknown/3000", "/unknown-big/100000", "/unknown-big/32000", "/unknown-big/1"} {
		head, body, _ := h.rawResponse(c, get(route), 10*time.Second)
		if !strings.Contains(head, "Transfer-Encoding: chunked") || headerValue(head, "Content-Length") != "" {
			t.Fatalf("%s: head %q", route, head)
		}
		var n int
		fmt.Sscan(route[strings.LastIndex(route, "/")+1:], &n)
		if got := dechunk(t, body); !bytes.Equal(got, pattern(n)) {
			t.Fatalf("%s: decoded %d bytes, want %d", route, len(got), n)
		}
	}
	if closed.Load() != 4 {
		t.Fatalf("closed %d", closed.Load())
	}
	// HTTP/1.0 cannot be chunked: the body ends with the connection
	c = h.dial(0)
	head, body, isClosed := h.rawResponse(c, "GET /unknown-big/50000 HTTP/1.0\r\n\r\n", 5*time.Second)
	if strings.Contains(head, "chunked") || !strings.Contains(head, "Connection: close") || !bytes.Equal(body, pattern(50000)) || !isClosed {
		t.Fatalf("HTTP/1.0: head %q body %d closed=%v", head, len(body), isClosed)
	}
}

func TestStreamedResponseFailuresEndTheConnection(t *testing.T) {
	h, closed, _, _ := startStream(t, ghttp.Config{})
	for _, route := range []string{"/short", "/failing"} {
		c := h.dial(0)
		h.write(c, get(route))
		_, isClosed := h.read(c, nil, nil, 3*time.Second)
		if !isClosed {
			t.Errorf("%s: a source that fails must end the connection, not leave the client waiting", route)
		}
	}
	// and the server is fine
	if r := h.roundTrip(h.dial(0), get("/")); r.body != "hello\n" {
		t.Fatal("server unhealthy")
	}
	// the source of a handler that panicked is closed too
	h.roundTrip(h.dial(0), get("/panic-after-reader"))
	if closed.Load() != 3 {
		t.Fatalf("closed %d, want 3 (short, failing, panicking)", closed.Load())
	}
}

func TestStreamedResponseClientHangsUpMidway(t *testing.T) {
	h, closed, _, _ := startStream(t, ghttp.Config{WriteTimeout: 300 * time.Millisecond})
	c := h.dial(0)
	h.write(c, get("/stream/50000000")) // 50 MB that the client never reads
	// the client never reads: the kernel buffers fill, the send blocks, and the write timeout ends the connection
	for i := 0; i < 300 && closed.Load() == 0; i++ {
		h.step()
		time.Sleep(10 * time.Millisecond)
	}
	if closed.Load() != 1 {
		t.Fatalf("abandoned source closed %d times", closed.Load())
	}
}

func TestServeFileRangesAndStreaming(t *testing.T) {
	h, _, _, dir := startStream(t, ghttp.Config{})
	data := pattern(300_000)
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "page.html"), []byte("<p>hi</p>"), 0o644)
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	c := h.dial(0)

	head, body, _ := h.rawResponse(c, get("/file/data.bin"), 5*time.Second)
	if !strings.HasPrefix(head, "HTTP/1.1 200") || !bytes.Equal(body, data) || headerValue(head, "Accept-Ranges") != "bytes" ||
		headerValue(head, "Content-Type") != "application/octet-stream" || headerValue(head, "Last-Modified") == "" {
		t.Fatalf("whole file: %q (%d bytes)", head, len(body))
	}
	lm := headerValue(head, "Last-Modified")

	for _, tc := range []struct {
		hdr        string
		start, end int // inclusive; the expected slice
		status     string
	}{
		{"bytes=0-99", 0, 99, "206"},
		{"bytes=100000-199999", 100000, 199999, "206"},
		{"bytes=290000-", 290000, 299999, "206"},
		{"bytes=-1234", 300000 - 1234, 299999, "206"},
		{"bytes=0-9999999", 0, 299999, "206"},
	} {
		head, body, _ := h.rawResponse(c, "GET /file/data.bin HTTP/1.1\r\nHost: t\r\nRange: "+tc.hdr+"\r\n\r\n", 5*time.Second)
		want := fmt.Sprintf("bytes %d-%d/300000", tc.start, tc.end)
		if !strings.HasPrefix(head, "HTTP/1.1 "+tc.status) || headerValue(head, "Content-Range") != want || !bytes.Equal(body, data[tc.start:tc.end+1]) {
			t.Fatalf("%s: head %q body %d bytes", tc.hdr, head, len(body))
		}
	}
	head, _, _ = h.rawResponse(c, "GET /file/data.bin HTTP/1.1\r\nHost: t\r\nRange: bytes=400000-\r\n\r\n", 5*time.Second)
	if !strings.HasPrefix(head, "HTTP/1.1 416") || headerValue(head, "Content-Range") != "bytes */300000" {
		t.Fatalf("unsatisfiable: %q", head)
	}
	// If-Range with the Last-Modified we gave out: honoured; with another date: whole file
	head, body, _ = h.rawResponse(c, "GET /file/data.bin HTTP/1.1\r\nHost: t\r\nRange: bytes=0-9\r\nIf-Range: "+lm+"\r\n\r\n", 5*time.Second)
	if !strings.HasPrefix(head, "HTTP/1.1 206") || len(body) != 10 {
		t.Fatalf("If-Range match: %q", head)
	}
	head, body, _ = h.rawResponse(c, "GET /file/data.bin HTTP/1.1\r\nHost: t\r\nRange: bytes=0-9\r\nIf-Range: Mon, 01 Jan 2001 00:00:00 GMT\r\n\r\n", 5*time.Second)
	if !strings.HasPrefix(head, "HTTP/1.1 200") || len(body) != 300000 {
		t.Fatalf("If-Range mismatch: %q", head)
	}
	// content type by extension; missing files and directories
	head, _, _ = h.rawResponse(c, get("/file/page.html"), 5*time.Second)
	if !strings.Contains(headerValue(head, "Content-Type"), "text/html") {
		t.Fatalf("content type %q", headerValue(head, "Content-Type"))
	}
	for _, p := range []string{"/file/nope.txt", "/file/sub", "/file/../etc/passwd"} {
		if r := h.roundTrip(c, get(p)); r.status != 404 {
			t.Fatalf("%s: %d", p, r.status)
		}
	}
}

func TestStreamedResponseOverTLS(t *testing.T) {
	var closed, reads atomic.Int32
	r := ghttp.NewRouter()
	streamRoutes(r, &closed, &reads, t.TempDir())
	h := startTLSWith(t, ghttp.Config{}, r)
	c, err := h.client(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1000, 500_000} {
		c.Write([]byte(get(fmt.Sprintf("/stream/%d", n))))
		rs := readResponses(t, c, 1)
		if rs[0].status != 200 || !bytes.Equal([]byte(rs[0].body), pattern(n)) {
			t.Fatalf("n=%d: %d, %d bytes", n, rs[0].status, len(rs[0].body))
		}
	}
	// chunked, and a connection that closes after the response (close_notify at the very end)
	c.Write([]byte("GET /unknown-big/90000 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"))
	var acc []byte
	buf := make([]byte, 32<<10)
	for {
		n, err := c.Read(buf)
		acc = append(acc, buf[:n]...)
		if err != nil {
			break
		}
	}
	i := bytes.Index(acc, []byte("\r\n\r\n"))
	if i < 0 || !bytes.Equal(dechunk(t, acc[i+4:]), pattern(90000)) {
		t.Fatalf("chunked over TLS: %d bytes", len(acc))
	}
}
