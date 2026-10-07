//go:build linux

package http2_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ghttp "github.com/rm4n0s/gina/extensions/http"
	"github.com/rm4n0s/gina/extensions/http2"
)

type uploadStats struct {
	pieces, biggest atomic.Int64
	aborted         atomic.Int32
	completed       atomic.Int32
}

func uploadRouter(st *uploadStats) *ghttp.Router {
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
				st.completed.Add(1)
				c.String(200, fmt.Sprintf("%d %x", n, h.Sum(nil)[:6]))
			}
		})
	}
	r.POST("/upload", hashing).StreamBody()
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
	r.POST("/buffered", hashing)
	return r
}

func sum6(b []byte) string { s := sha256.Sum256(b); return fmt.Sprintf("%x", s[:6]) }

func TestStreamedUploads(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		st := &uploadStats{}
		h := startWith(t, uploadRouter(st), 1, useTLS, func(c *http2.Config) { c.MaxBodyBytes = 1000 }) // far below the uploads
		post := func(path string, body io.Reader) (*http.Response, string) {
			t.Helper()
			resp, err := h.client.Post(h.url(path), "application/octet-stream", body)
			if err != nil {
				t.Fatalf("POST %s: %v", path, err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, string(b)
		}

		// known length
		data := pattern(3 << 20)
		resp, body := post("/upload", bytes.NewReader(data))
		if resp.StatusCode != 200 || body != fmt.Sprintf("%d %s", len(data), sum6(data)) {
			t.Fatalf("known length: %d %q", resp.StatusCode, body)
		}
		if st.biggest.Load() > 64<<10 || st.pieces.Load() < 30 {
			t.Fatalf("not streamed: %d pieces, biggest %d", st.pieces.Load(), st.biggest.Load())
		}
		// unknown length (no content-length: the client streams from a pipe)
		pr, pw := io.Pipe()
		go func() {
			for i := 0; i < 50; i++ {
				pw.Write(pattern(20_000))
			}
			pw.Close()
		}()
		resp, body = post("/upload", pr)
		if want := bytes.Repeat(pattern(20_000), 50); resp.StatusCode != 200 || body != fmt.Sprintf("%d %s", len(want), sum6(want)) {
			t.Fatalf("unknown length: %d %q", resp.StatusCode, body)
		}
		// empty body: the callback still sees a (last, empty) piece
		resp, body = post("/upload", http.NoBody)
		if resp.StatusCode != 200 || body != "0 "+sum6(nil) {
			t.Fatalf("empty: %d %q", resp.StatusCode, body)
		}
		// route limits apply while streaming
		if resp, _ = post("/limited", bytes.NewReader(make([]byte, 100_000))); resp.StatusCode != 200 {
			t.Fatalf("at the limit: %d", resp.StatusCode)
		}
		if resp, _ = post("/limited", bytes.NewReader(make([]byte, 100_001))); resp.StatusCode != 413 {
			t.Fatalf("declared over the limit: %d", resp.StatusCode)
		}
		pr, pw = io.Pipe()
		go func() {
			for i := 0; i < 20; i++ {
				if _, err := pw.Write(make([]byte, 10_000)); err != nil {
					return
				}
			}
			pw.Close()
		}()
		if resp, err := h.client.Post(h.url("/limited"), "application/octet-stream", pr); err == nil {
			resp.Body.Close()
			if resp.StatusCode != 413 {
				t.Fatalf("undeclared, over the limit: %d", resp.StatusCode)
			}
		}
		pr.CloseWithError(io.ErrClosedPipe)
		// refusals: up front, and part-way
		if resp, body = post("/refuse", bytes.NewReader(make([]byte, 50_000))); resp.StatusCode != 403 || body != "no uploads for you\n" {
			t.Fatalf("refuse: %d %q", resp.StatusCode, body)
		}
		if resp, body = post("/refuse-late", bytes.NewReader(make([]byte, 200_000))); resp.StatusCode != 413 || body != "too big for me\n" {
			t.Fatalf("refuse late: %d %q", resp.StatusCode, body)
		}
		if resp, _ = post("/panics", strings.NewReader("abc")); resp.StatusCode != 500 {
			t.Fatalf("panic: %d", resp.StatusCode)
		}
		// an ordinary route can use OnBody too
		before := st.pieces.Load()
		if resp, body = post("/buffered", strings.NewReader("hello")); resp.StatusCode != 200 || body != "5 "+sum6([]byte("hello")) || st.pieces.Load() != before+1 {
			t.Fatalf("buffered: %d %q (%d calls)", resp.StatusCode, body, st.pieces.Load()-before)
		}
		// the connection is fine after all of that
		if _, b := h.do("GET", "/", nil); string(b) != "hello\n" {
			t.Fatalf("after uploads: %q", b)
		}
	})
}

func TestStreamedUploadsInParallel(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		st := &uploadStats{}
		h := startWith(t, uploadRouter(st), 1, useTLS)
		done := make(chan string, 12)
		for i := 0; i < 12; i++ {
			go func(i int) {
				data := pattern(400_000 + i*1000)
				resp, err := h.client.Post(h.url("/upload"), "application/octet-stream", bytes.NewReader(data))
				if err != nil {
					done <- err.Error()
					return
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if string(b) != fmt.Sprintf("%d %s", len(data), sum6(data)) {
					done <- fmt.Sprintf("upload %d: %q", i, b)
					return
				}
				done <- ""
			}(i)
		}
		for i := 0; i < 12; i++ {
			if e := <-done; e != "" {
				t.Error(e)
			}
		}
	})
}

func TestStreamedUploadAbortedByClient(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		st := &uploadStats{}
		h := startWith(t, uploadRouter(st), 1, useTLS)
		ctx, cancel := context.WithCancel(context.Background())
		pr, pw := io.Pipe()
		req, _ := http.NewRequestWithContext(ctx, "POST", h.url("/upload"), pr)
		go func() {
			pw.Write(pattern(50_000))
			time.Sleep(100 * time.Millisecond)
			cancel() // RST_STREAM mid-upload
			pw.CloseWithError(io.ErrClosedPipe)
		}()
		if resp, err := h.client.Do(req); err == nil {
			resp.Body.Close()
		}
		for i := 0; i < 200 && st.aborted.Load() == 0; i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if st.aborted.Load() != 1 || st.completed.Load() != 0 {
			t.Fatalf("aborted=%d completed=%d", st.aborted.Load(), st.completed.Load())
		}
		if _, b := h.do("GET", "/", nil); string(b) != "hello\n" {
			t.Fatalf("connection unusable: %q", b)
		}
	})
}
