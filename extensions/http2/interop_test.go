//go:build linux

package http2_test

import (
	"bytes"
	"crypto/rand"
	ctls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
	"github.com/rm4n0s/gina/extensions/http2"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

func routes() *ghttp.Router {
	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.String(200, "hello\n") })
	r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "hello "+c.Param("name")+"\n") })
	r.GET("/q", func(c *ghttp.Context) { c.String(200, c.Query("x")+"|"+string(c.Req.Query)) })
	r.POST("/echo", func(c *ghttp.Context) { c.Bytes(200, "application/octet-stream", c.Req.Body) })
	r.GET("/big", func(c *ghttp.Context) {
		n, _ := strconv.Atoi(c.Query("n"))
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i % 251)
		}
		c.Bytes(200, "application/octet-stream", b)
	})
	r.GET("/headers", func(c *ghttp.Context) {
		var sb strings.Builder
		for _, h := range c.Req.Headers {
			fmt.Fprintf(&sb, "%s=%s\n", h.Name, h.Value)
		}
		c.String(200, sb.String())
	})
	r.GET("/tls", func(c *ghttp.Context) {
		info, ok := c.TLS()
		c.String(200, fmt.Sprintf("%v %s %s", ok, info.ALPN, info.CipherName()))
	})
	r.GET("/panic", func(c *ghttp.Context) { panic("boom") })
	r.GET("/nocontent", func(c *ghttp.Context) { c.Status(204) })
	r.GET("/multi", func(c *ghttp.Context) {
		c.SetHeader("X-One", "1")
		c.SetHeader("Set-Cookie", "a=1")
		c.SetHeader("Set-Cookie", "b=2")
		c.SetHeader("Connection", "close") // must not reach an HTTP/2 client
		c.String(200, "ok")
	})
	echo := func(c *ghttp.Context) { c.Bytes(200, "application/octet-stream", c.Req.Body) }
	r.POST("/small", echo).MaxBody(10)
	r.POST("/big", echo).MaxBody(400_000)
	r.GET("/redirect", func(c *ghttp.Context) { c.Redirect(302, "/hello/redirected") })
	r.GET("/sse", func(c *ghttp.Context) { c.EventStream(0) })
	r.GET("/shard", func(c *ghttp.Context) { c.String(200, strconv.Itoa(int(c.Gina().ShardID()))) })
	return r
}

func freePort(t testing.TB) uint16 {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return uint16(l.Addr().(*net.TCPAddr).Port)
}

type srv struct {
	t      *testing.T
	sys    *gina.System
	srv    *http2.Server
	port   uint16
	client *http.Client
	scheme string
	pool   *x509.CertPool
}

type option func(*http2.Config)

func start(t *testing.T, shards int, useTLS bool, opts ...option) *srv {
	t.Helper()
	return startWith(t, routes(), shards, useTLS, opts...)
}

func startWith(t *testing.T, r *ghttp.Router, shards int, useTLS bool, opts ...option) *srv {
	t.Helper()
	port := freePort(t)
	cfg := http2.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: true, MaxConns: 512}
	for _, o := range opts {
		o(&cfg)
	}
	tr := &http.Transport{MaxIdleConnsPerHost: 64}
	scheme := "http"
	var pool *x509.CertPool
	if useTLS {
		cert, err := gtls.SelfSigned("localhost", "127.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := x509.ParseCertificate(cert.Certificate[0])
		pool = x509.NewCertPool()
		pool.AddCert(leaf)
		cfg.TLS = &gtls.Config{Certificates: []ctls.Certificate{cert}}
		tr.TLSClientConfig = &ctls.Config{RootCAs: pool}
		tr.ForceAttemptHTTP2 = true
		scheme = "https"
	} else {
		var p http.Protocols
		p.SetUnencryptedHTTP2(true) // h2c with prior knowledge
		tr.Protocols = &p
	}
	s := http2.New(cfg, r)
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, shards)}
	if err := s.Install(&spec); err != nil {
		t.Fatal(err)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatalf("NewSystem: %v (listen: %v)", err, s.ListenErr())
	}
	sys.Start(gina.RunOptions{ShutdownGrace: 3 * time.Second})
	h := &srv{t: t, sys: sys, srv: s, port: port, scheme: scheme, pool: pool,
		client: &http.Client{Transport: tr, Timeout: 15 * time.Second}}
	t.Cleanup(func() { tr.CloseIdleConnections(); sys.Stop(); sys.Close() })
	return h
}

func (h *srv) addr() string { return "127.0.0.1:" + strconv.Itoa(int(h.port)) }
func (h *srv) url(p string) string {
	return h.scheme + "://" + h.addr() + p
}

func (h *srv) do(method, path string, body io.Reader, hdr ...string) (*http.Response, []byte) {
	h.t.Helper()
	req, err := http.NewRequest(method, h.url(path), body)
	if err != nil {
		h.t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("%s %s: reading body: %v", method, path, err)
	}
	return resp, b
}

func eachMode(t *testing.T, f func(t *testing.T, tls bool)) {
	for _, useTLS := range []bool{false, true} {
		name := map[bool]string{false: "h2c", true: "h2-tls"}[useTLS]
		t.Run(name, func(t *testing.T) { f(t, useTLS) })
	}
}

func TestBasicRequests(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS)

		resp, body := h.do("GET", "/", nil)
		if resp.ProtoMajor != 2 || resp.StatusCode != 200 || string(body) != "hello\n" {
			t.Fatalf("GET /: proto %s status %d body %q", resp.Proto, resp.StatusCode, body)
		}
		if resp.Header.Get("Server") != "gina" || resp.Header.Get("Date") == "" ||
			resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" || resp.ContentLength != 6 {
			t.Fatalf("headers: %v (content length %d)", resp.Header, resp.ContentLength)
		}
		if _, body := h.do("GET", "/hello/there", nil); string(body) != "hello there\n" {
			t.Fatalf("param route: %q", body)
		}
		if _, body := h.do("GET", "/q?x=a%20b&y=2", nil); string(body) != "a b|x=a%20b&y=2" {
			t.Fatalf("query: %q", body)
		}
		if resp, body := h.do("GET", "/nope", nil); resp.StatusCode != 404 || string(body) != "not found\n" {
			t.Fatalf("404: %d %q", resp.StatusCode, body)
		}
		if resp, _ := h.do("POST", "/", nil); resp.StatusCode != 405 || resp.Header.Get("Allow") != "GET" {
			t.Fatalf("405: %d allow=%q", resp.StatusCode, resp.Header.Get("Allow"))
		}
		resp, body = h.do("HEAD", "/hello/x", nil)
		if resp.StatusCode != 200 || len(body) != 0 || resp.ContentLength != 8 {
			t.Fatalf("HEAD: %d len(body)=%d content-length=%d", resp.StatusCode, len(body), resp.ContentLength)
		}
		if resp, body := h.do("GET", "/nocontent", nil); resp.StatusCode != 204 || len(body) != 0 {
			t.Fatalf("204: %d %q", resp.StatusCode, body)
		}
		if resp, body := h.do("GET", "/panic", nil); resp.StatusCode != 500 || string(body) != "internal server error\n" {
			t.Fatalf("panicking handler: %d %q", resp.StatusCode, body)
		}
		// the connection survives a panic
		if _, body := h.do("GET", "/", nil); string(body) != "hello\n" {
			t.Fatalf("after panic: %q", body)
		}
		resp, body = h.do("GET", "/redirect", nil)
		if resp.StatusCode != 200 || string(body) != "hello redirected\n" {
			t.Fatalf("redirect followed to %v: %d %q", resp.Request.URL, resp.StatusCode, body)
		}
		resp, _ = h.do("GET", "/multi", nil)
		if got := resp.Header.Values("Set-Cookie"); len(got) != 2 || got[0] != "a=1" || got[1] != "b=2" ||
			resp.Header.Get("X-One") != "1" || resp.Header.Get("Connection") != "" {
			t.Fatalf("handler headers: %v", resp.Header)
		}
		if resp, body := h.do("GET", "/sse", nil); resp.StatusCode != 501 || !strings.Contains(string(body), "HTTP/2") {
			t.Fatalf("event stream: %d %q", resp.StatusCode, body)
		}
		if h.srv.Requests() == 0 || h.srv.Conns() != 1 {
			t.Fatalf("requests=%d conns=%d (one multiplexed connection expected)", h.srv.Requests(), h.srv.Conns())
		}
	})
}

func TestRequestHeaders(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS)
		_, body := h.do("GET", "/headers", nil, "X-Custom", "v1", "Cookie", "a=1", "Cookie", "b=2")
		got := string(body)
		for _, want := range []string{"x-custom=v1\n", "host=127.0.0.1:" + strconv.Itoa(int(h.port)) + "\n", "user-agent=", "accept-encoding=gzip\n"} {
			if !strings.Contains(got, want) {
				t.Errorf("handler did not see %q in:\n%s", want, got)
			}
		}
		// Go's client folds repeated Cookie headers itself, and the server must
		// present exactly one cookie header either way
		if n := strings.Count(got, "cookie="); n != 1 {
			t.Errorf("%d cookie headers in:\n%s", n, got)
		}
		if strings.Contains(got, ":method") || strings.Contains(got, ":path") {
			t.Errorf("pseudo-headers leaked into the header list:\n%s", got)
		}
	})
}

func TestTLSInfoAndALPN(t *testing.T) {
	h := start(t, 1, true)
	resp, body := h.do("GET", "/tls", nil)
	if resp.ProtoMajor != 2 || resp.TLS == nil || resp.TLS.NegotiatedProtocol != "h2" || resp.TLS.Version != ctls.VersionTLS13 {
		t.Fatalf("client side: proto %s tls %+v", resp.Proto, resp.TLS)
	}
	if got := string(body); !strings.HasPrefix(got, "true h2 TLS_AES_") {
		t.Fatalf("handler saw %q", got)
	}
	// a client that speaks only HTTP/1.1 cannot be served: the handshake fails
	c := ctls.Client(dial(t, h.addr()), &ctls.Config{RootCAs: h.pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if err := c.Handshake(); err == nil {
		t.Fatal("handshake offering only http/1.1 succeeded")
	}
}

func dial(t testing.TB, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestLargeBodies(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS, func(c *http2.Config) { c.MaxBodyBytes = 4 << 20 })
		for _, n := range []int{0, 1, 16383, 16384, 16385, 65535, 65536, 65537, 300_000, 3 << 20} {
			body := make([]byte, n)
			rand.Read(body)
			resp, got := h.do("POST", "/echo", bytes.NewReader(body))
			if resp.StatusCode != 200 || !bytes.Equal(got, body) {
				t.Fatalf("echo of %d bytes: status %d, got %d bytes, equal=%v", n, resp.StatusCode, len(got), bytes.Equal(got, body))
			}
		}
		for _, n := range []int{1, 100, 16384, 16385, 100_000, 5 << 20} {
			resp, got := h.do("GET", "/big?n="+strconv.Itoa(n), nil)
			if resp.StatusCode != 200 || len(got) != n {
				t.Fatalf("big %d: status %d, %d bytes", n, resp.StatusCode, len(got))
			}
			for i, b := range got {
				if b != byte(i%251) {
					t.Fatalf("big %d: byte %d is %d", n, i, b)
				}
			}
		}
	})
}

func TestBodyTooLarge(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS, func(c *http2.Config) { c.MaxBodyBytes = 100_000 })
		// declared up front: refused before any body is read
		resp, _ := h.do("POST", "/echo", bytes.NewReader(make([]byte, 200_000)))
		if resp.StatusCode != 413 {
			t.Fatalf("declared oversize body: status %d", resp.StatusCode)
		}
		// undeclared (chunked-style streaming upload): refused when it grows past the limit
		pr, pw := io.Pipe()
		go func() {
			chunk := make([]byte, 30_000)
			for i := 0; i < 20; i++ {
				if _, err := pw.Write(chunk); err != nil {
					return
				}
			}
			pw.Close()
		}()
		resp2, err := h.client.Post(h.url("/echo"), "application/octet-stream", pr)
		if err == nil {
			resp2.Body.Close()
			if resp2.StatusCode != 413 {
				t.Fatalf("streamed oversize body: status %d", resp2.StatusCode)
			}
		} // the client may also see the RST_STREAM(NO_ERROR) first: both are correct
		pr.CloseWithError(io.ErrClosedPipe)
		// and the connection still works
		if _, body := h.do("GET", "/", nil); string(body) != "hello\n" {
			t.Fatalf("after 413: %q", body)
		}
	})
}

func TestConcurrentStreamsOneConnection(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS)
		var wg sync.WaitGroup
		var bad atomic.Int64
		for w := 0; w < 64; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					name := fmt.Sprintf("w%di%d", w, i)
					resp, err := h.client.Get(h.url("/hello/" + name))
					if err != nil {
						bad.Add(1)
						continue
					}
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					if string(b) != "hello "+name+"\n" {
						bad.Add(1)
					}
				}
			}(w)
		}
		wg.Wait()
		if bad.Load() != 0 {
			t.Fatalf("%d of %d requests failed", bad.Load(), 64*20)
		}
		if got := h.srv.Requests(); got != 64*20 {
			t.Fatalf("server counted %d requests", got)
		}
	})
}

func TestStopClosesConnections(t *testing.T) {
	h := start(t, 2, false)
	if _, body := h.do("GET", "/", nil); string(body) != "hello\n" {
		t.Fatal(body)
	}
	begin := time.Now()
	h.sys.Stop()
	if d := time.Since(begin); d > 2*time.Second {
		t.Fatalf("Stop took %v with an idle connection open", d)
	}
	if h.srv.Conns() != 0 {
		t.Fatalf("%d connections still counted after Stop", h.srv.Conns())
	}
}

// Four shard threads, each with its own SO_REUSEPORT listener, and many separate
// connections hammering them, each carrying several multiplexed streams.
func TestShardThreads(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 4, useTLS, func(c *http2.Config) { c.MaxBodyBytes = 1 << 20 })
		var wg sync.WaitGroup
		var bad atomic.Int64
		var mu sync.Mutex
		shards := map[string]int{}
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// a private client, hence a private connection, per worker
				tr := h.client.Transport.(*http.Transport).Clone()
				defer tr.CloseIdleConnections()
				c := &http.Client{Transport: tr, Timeout: 15 * time.Second}
				body := make([]byte, 200_000)
				rand.Read(body)
				var inner sync.WaitGroup
				for s := 0; s < 4; s++ {
					inner.Add(1)
					go func() {
						defer inner.Done()
						for i := 0; i < 10; i++ {
							resp, err := c.Post(h.url("/echo"), "application/octet-stream", bytes.NewReader(body))
							if err != nil {
								bad.Add(1)
								continue
							}
							got, _ := io.ReadAll(resp.Body)
							resp.Body.Close()
							if !bytes.Equal(got, body) {
								bad.Add(1)
							}
						}
					}()
				}
				resp, err := c.Get(h.url("/shard"))
				if err == nil {
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					mu.Lock()
					shards[string(b)]++
					mu.Unlock()
				}
				inner.Wait()
			}(w)
		}
		wg.Wait()
		if bad.Load() != 0 {
			t.Fatalf("%d echo round trips failed or came back corrupted", bad.Load())
		}
		if len(shards) < 2 {
			t.Fatalf("connections reached only %d of 4 shard threads: %v", len(shards), shards)
		}
		t.Logf("connections per shard: %v", shards)
	})
}

func TestPerRouteBodyLimits(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS, func(c *http2.Config) { c.MaxBodyBytes = 1000 })
		for _, tc := range []struct {
			path   string
			n      int
			status int
		}{
			{"/echo", 1000, 200}, {"/echo", 1001, 413}, // the server default
			{"/big", 300_000, 200}, {"/big", 400_001, 413}, // raised for one route
			{"/small", 10, 200}, {"/small", 11, 413}, // lowered for another
		} {
			resp, _ := h.do("POST", tc.path, bytes.NewReader(make([]byte, tc.n)))
			if resp.StatusCode != tc.status {
				t.Errorf("POST %s with %d bytes: %d, want %d", tc.path, tc.n, resp.StatusCode, tc.status)
			}
		}
		// a streamed (undeclared length) upload hits the route's limit as it grows
		pr, pw := io.Pipe()
		go func() {
			for i := 0; i < 20; i++ {
				if _, err := pw.Write(make([]byte, 30)); err != nil {
					return
				}
			}
			pw.Close()
		}()
		if resp, err := h.client.Post(h.url("/small"), "application/octet-stream", pr); err == nil {
			resp.Body.Close()
			if resp.StatusCode != 413 {
				t.Errorf("streamed upload to /small: %d", resp.StatusCode)
			}
		}
		pr.CloseWithError(io.ErrClosedPipe)
	})
}
