//go:build linux

package http2_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	ghttp "github.com/rm4n0s/gina/extensions/http"
)

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i>>8)
	}
	return b
}

type trackedReader struct {
	io.Reader
	closed *atomic.Int32
}

func (t trackedReader) Close() error { t.closed.Add(1); return nil }

func streamRouter(closed *atomic.Int32, dir string) *ghttp.Router {
	r := ghttp.NewRouter()
	src := func(n int) io.Reader { return trackedReader{bytes.NewReader(pattern(n)), closed} }
	size := func(c *ghttp.Context) int { var n int; fmt.Sscan(c.Param("n"), &n); return n }
	r.GET("/", func(c *ghttp.Context) { c.String(200, "hello\n") })
	r.GET("/stream/:n", func(c *ghttp.Context) { n := size(c); c.SendReader(200, "application/octet-stream", int64(n), src(n)) })
	r.GET("/unknown/:n", func(c *ghttp.Context) {
		n := size(c)
		c.SendReader(200, "application/octet-stream", -1, trackedReader{iotest.OneByteReader(bytes.NewReader(pattern(n))), closed})
	})
	r.GET("/unknown-big/:n", func(c *ghttp.Context) { n := size(c); c.SendReader(200, "application/octet-stream", -1, src(n)) })
	r.GET("/short", func(c *ghttp.Context) {
		c.SendReader(200, "text/plain", 1000, trackedReader{strings.NewReader("0123456789"), closed})
	})
	r.GET("/failing", func(c *ghttp.Context) {
		c.SendReader(200, "text/plain", 100000, trackedReader{io.MultiReader(bytes.NewReader(pattern(40000)), iotest.ErrReader(errors.New("disk on fire"))), closed})
	})
	r.GET("/file/*", func(c *ghttp.Context) { c.ServeFS(os.DirFS(dir), string(c.ParamBytes("*"))) })
	return r
}

func TestStreamedResponses(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		var closed atomic.Int32
		h := startWith(t, streamRouter(&closed, t.TempDir()), 1, useTLS)
		get := func(path string) (*http.Response, []byte) { return h.do("GET", path, nil) }

		for _, n := range []int{1, 100, 32 << 10, 32<<10 + 1, 1 << 20, 3<<20 + 17} {
			resp, body := get(fmt.Sprintf("/stream/%d", n))
			if resp.StatusCode != 200 || resp.ContentLength != int64(n) || !bytes.Equal(body, pattern(n)) {
				t.Fatalf("n=%d: status %d, content-length %d, %d bytes", n, resp.StatusCode, resp.ContentLength, len(body))
			}
		}
		// unknown length: the stream's END_STREAM ends the body
		for _, route := range []string{"/unknown/2000", "/unknown-big/100000", "/unknown-big/32768", "/unknown-big/1"} {
			resp, body := get(route)
			var n int
			fmt.Sscan(route[strings.LastIndex(route, "/")+1:], &n)
			if resp.StatusCode != 200 || resp.ContentLength != -1 || !bytes.Equal(body, pattern(n)) {
				t.Fatalf("%s: status %d, content-length %d, %d bytes", route, resp.StatusCode, resp.ContentLength, len(body))
			}
		}
		if resp, body := get("/stream/0"); resp.StatusCode != 200 || len(body) != 0 {
			t.Fatalf("zero length: %d %d", resp.StatusCode, len(body))
		}
		// HEAD: headers only, the source is closed without being read
		before := closed.Load()
		resp, _ := h.do("HEAD", "/stream/99999", nil)
		if resp.ContentLength != 99999 {
			t.Fatalf("HEAD content-length %d", resp.ContentLength)
		}
		waitClosed(t, &closed, before+1)

		// a source that fails or ends early resets the stream; the connection survives
		for _, route := range []string{"/short", "/failing"} {
			before := closed.Load()
			if resp, err := h.client.Get(h.url(route)); err == nil {
				_, err = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if err == nil {
					t.Fatalf("%s: the client saw a complete, successful response", route)
				}
			}
			waitClosed(t, &closed, before+1)
		}
		if _, body := get("/"); string(body) != "hello\n" {
			t.Fatalf("connection unusable after failed streams: %q", body)
		}
	})
}

func waitClosed(t *testing.T, closed *atomic.Int32, want int32) {
	t.Helper()
	for i := 0; i < 200 && closed.Load() < want; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if got := closed.Load(); got != want {
		t.Fatalf("sources closed: %d, want %d", got, want)
	}
}

func TestStreamedResponsesManyAtOnce(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		var closed atomic.Int32
		h := startWith(t, streamRouter(&closed, t.TempDir()), 1, useTLS)
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				n := 200_000 + i*13_001
				resp, err := h.client.Get(h.url(fmt.Sprintf("/stream/%d", n)))
				if err != nil {
					t.Errorf("stream %d: %v", i, err)
					return
				}
				b, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || !bytes.Equal(b, pattern(n)) {
					t.Errorf("stream %d: err=%v, %d bytes", i, err, len(b))
				}
			}(i)
		}
		wg.Wait()
		waitClosed(t, &closed, 16)
	})
}

func TestStreamedResponseCancelledByClient(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		var closed atomic.Int32
		h := startWith(t, streamRouter(&closed, t.TempDir()), 1, useTLS)
		resp, err := h.client.Get(h.url("/stream/200000000")) // 200 MB
		if err != nil {
			t.Fatal(err)
		}
		io.CopyN(io.Discard, resp.Body, 100_000)
		resp.Body.Close() // RST_STREAM(CANCEL) mid-body
		waitClosed(t, &closed, 1)
		if _, body := h.do("GET", "/", nil); string(body) != "hello\n" {
			t.Fatalf("connection unusable after a cancelled stream: %q", body)
		}
	})
}

func TestServeFileOverHTTP2(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		dir := t.TempDir()
		data := pattern(500_000)
		os.WriteFile(filepath.Join(dir, "data.bin"), data, 0o644)
		var closed atomic.Int32
		h := startWith(t, streamRouter(&closed, dir), 1, useTLS)
		resp, body := h.do("GET", "/file/data.bin", nil)
		if resp.StatusCode != 200 || !bytes.Equal(body, data) || resp.Header.Get("Accept-Ranges") != "bytes" {
			t.Fatalf("whole file: %d, %d bytes", resp.StatusCode, len(body))
		}
		resp, body = h.do("GET", "/file/data.bin", nil, "Range", "bytes=1000-1999")
		if resp.StatusCode != 206 || !bytes.Equal(body, data[1000:2000]) || resp.Header.Get("Content-Range") != "bytes 1000-1999/500000" {
			t.Fatalf("range: %d %q, %d bytes", resp.StatusCode, resp.Header.Get("Content-Range"), len(body))
		}
		resp, _ = h.do("GET", "/file/data.bin", nil, "Range", "bytes=900000-")
		if resp.StatusCode != 416 {
			t.Fatalf("unsatisfiable: %d", resp.StatusCode)
		}
		if resp, _ := h.do("GET", "/file/missing", nil); resp.StatusCode != 404 {
			t.Fatalf("missing: %d", resp.StatusCode)
		}
	})
}
