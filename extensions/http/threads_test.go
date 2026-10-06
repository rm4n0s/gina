//go:build linux

// Threaded-runtime tests: the server on N shard threads (one SO_REUSEPORT
// listener each) hammered by concurrent clients. Run them with -race.

package http_test

import (
	"bytes"
	"crypto/rand"
	ctls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gina"
	ghttp "gina/extensions/http"
	gtls "gina/extensions/tls"
)

type threaded struct {
	t      *testing.T
	sys    *gina.System
	srv    *ghttp.Server
	port   uint16
	client *http.Client
	scheme string
}

func startThreaded(t *testing.T, shards int, useTLS bool) *threaded {
	t.Helper()
	port := freePort(t)
	cfg := ghttp.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: true, MaxConns: 512}
	tr := &http.Transport{MaxIdleConnsPerHost: 64}
	scheme := "http"
	if useTLS {
		cert, err := gtls.SelfSigned("localhost", "127.0.0.1")
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := x509.ParseCertificate(cert.Certificate[0])
		pool := x509.NewCertPool()
		pool.AddCert(leaf)
		cfg.TLS = &gtls.Config{Certificates: []ctls.Certificate{cert}}
		tr.TLSClientConfig = &ctls.Config{RootCAs: pool}
		scheme = "https"
	}
	srv := ghttp.New(cfg, routes())
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, shards)}
	if err := srv.Install(&spec); err != nil {
		t.Fatal(err)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatalf("NewSystem: %v (listen: %v)", err, srv.ListenErr())
	}
	sys.Start(gina.RunOptions{ShutdownGrace: 3 * time.Second})
	th := &threaded{t: t, sys: sys, srv: srv, port: port, scheme: scheme,
		client: &http.Client{Transport: tr, Timeout: 10 * time.Second}}
	t.Cleanup(func() { tr.CloseIdleConnections(); sys.Stop(); sys.Close() })
	return th
}

func (h *threaded) url(path string) string {
	return h.scheme + "://127.0.0.1:" + strconv.Itoa(int(h.port)) + path
}

func (h *threaded) get(path string) (int, string, error) {
	resp, err := h.client.Get(h.url(path))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

// hammer runs `workers` goroutines doing `each` requests apiece and returns the
// number of failures (first error is reported).
func (h *threaded) hammer(workers, each int, path func(w, i int) (string, string)) int64 {
	var fails atomic.Int64
	var first atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				p, want := path(w, i)
				code, body, err := h.get(p)
				if err != nil || code != 200 || body != want {
					fails.Add(1)
					first.CompareAndSwap(nil, "w"+strconv.Itoa(w)+" i"+strconv.Itoa(i)+": "+strconv.Itoa(code)+" "+body+" "+errString(err))
				}
			}
		}(w)
	}
	wg.Wait()
	if v := first.Load(); v != nil {
		h.t.Errorf("first failure: %v", v)
	}
	return fails.Load()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestThreadedHTTPConcurrentClients(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := map[bool]string{false: "http", true: "https"}[useTLS]
		t.Run(name, func(t *testing.T) {
			h := startThreaded(t, 4, useTLS)
			const workers, each = 16, 150
			if fails := h.hammer(workers, each, func(w, i int) (string, string) {
				n := "w" + strconv.Itoa(w) + "i" + strconv.Itoa(i)
				return "/hello/" + n, "hello " + n + "\n"
			}); fails != 0 {
				t.Fatalf("%d of %d requests failed", fails, workers*each)
			}
			if got := h.srv.Requests(); got != workers*each {
				t.Fatalf("server counted %d requests, want %d", got, workers*each)
			}
		})
	}
}

func TestThreadedHTTPSpreadsConnectionsAcrossShardThreads(t *testing.T) {
	h := startThreaded(t, 4, false)
	seen := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				// a fresh connection each time so SO_REUSEPORT can spread them
				c := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
				resp, err := c.Get(h.url("/shard"))
				if err != nil {
					t.Error(err)
					return
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				mu.Lock()
				seen[string(b)]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) < 3 {
		t.Fatalf("connections reached only %d of 4 shard threads: %v", len(seen), seen)
	}
	t.Logf("connections per shard: %v", seen)
}

func TestThreadedHTTPSLargeBodiesConcurrently(t *testing.T) {
	h := startThreaded(t, 4, true)
	var wg sync.WaitGroup
	var bad atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := make([]byte, 300_000)
			rand.Read(body)
			for i := 0; i < 5; i++ {
				resp, err := h.client.Post(h.url("/echo"), "application/octet-stream", bytes.NewReader(body))
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
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d echo round trips corrupted or failed", bad.Load())
	}
}

func TestThreadedHTTPEventStream(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		name := map[bool]string{false: "http", true: "https"}[useTLS]
		t.Run(name, func(t *testing.T) {
			h := startThreaded(t, 2, useTLS)
			resp, err := h.client.Get(h.url("/sse"))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
				t.Fatalf("content type %q", ct)
			}
			want := "retry: 100\n\ndata: hello\ndata: world\n\ndata: again\n\n"
			got := make([]byte, len(want))
			if _, err := io.ReadFull(resp.Body, got); err != nil || string(got) != want {
				t.Fatalf("stream = %q, %v; want %q", got, err, want)
			}
			// the stream stays open until the server stops, which closes it promptly
			stopped := time.Now()
			h.sys.Stop()
			if _, err := io.ReadAll(resp.Body); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Logf("read after stop: %v", err)
			}
			if d := time.Since(stopped); d > 2*time.Second {
				t.Fatalf("Stop took %v with an open event stream", d)
			}
		})
	}
}

func TestThreadedHTTPStopClosesConnections(t *testing.T) {
	h := startThreaded(t, 2, false)
	conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(h.port)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte(get("/")))
	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, err := conn.Read(buf); err != nil || n == 0 {
		t.Fatalf("first response: %d %v", n, err)
	}
	stopped := time.Now()
	h.sys.Stop()
	if d := time.Since(stopped); d > 2*time.Second {
		t.Fatalf("Stop took %v with an idle keep-alive connection open", d)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("keep-alive connection survived Stop")
	}
	if h.srv.Conns() != 0 {
		t.Fatalf("%d connections still counted open after Stop", h.srv.Conns())
	}
}
