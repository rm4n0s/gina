package http_test

import (
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"gina"
	ghttp "gina/extensions/http"
)

// The tests drive the server and a non-blocking client from one thread: with no
// goroutines available, every wait loop steps the System itself.

type harness struct {
	t   *testing.T
	sys *gina.System
	srv *ghttp.Server
}

func routes() *ghttp.Router {
	r := ghttp.NewRouter()
	r.GET("/", func(c *ghttp.Context) { c.String(200, "hello\n") })
	r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "hello "+c.Param("name")+"\n") })
	r.GET("/q", func(c *ghttp.Context) { c.String(200, c.Query("name")) })
	r.POST("/echo", func(c *ghttp.Context) { c.Bytes(200, "application/octet-stream", c.Req.Body) })
	r.GET("/boom", func(c *ghttp.Context) { var m map[string]int; m["x"] = 1 })
	r.GET("/json", func(c *ghttp.Context) { c.JSON(200, `{"ok":true}`) })
	r.GET("/shard", func(c *ghttp.Context) { c.String(200, strconv.Itoa(int(c.Gina().ShardID()))) })
	return r
}

func start(t *testing.T, cfg ghttp.Config, shards int) *harness {
	t.Helper()
	srv := ghttp.New(cfg, routes())
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, shards)}
	if err := srv.Install(&spec); err != nil {
		t.Fatal(err)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatalf("NewSystem: %v (listen: %v)", err, srv.ListenErr())
	}
	t.Cleanup(sys.Close)
	return &harness{t: t, sys: sys, srv: srv}
}

func (h *harness) dial(shard int) int {
	h.t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { syscall.Close(fd) })
	err = syscall.Connect(fd, &syscall.SockaddrInet4{Port: int(h.srv.Port(shard)), Addr: [4]byte{127, 0, 0, 1}})
	if err != nil && err != syscall.EINPROGRESS {
		h.t.Fatal(err)
	}
	return fd
}

func (h *harness) step() {
	if !h.sys.Step() {
		time.Sleep(20 * time.Microsecond)
	}
}

func (h *harness) write(fd int, s string) {
	h.t.Helper()
	b := []byte(s)
	deadline := time.Now().Add(3 * time.Second)
	for len(b) > 0 {
		n, err := syscall.Write(fd, b)
		if n > 0 {
			b = b[n:]
			continue
		}
		if err != syscall.EAGAIN && err != syscall.ENOTCONN && err != syscall.EINTR {
			h.t.Fatalf("write: %v", err)
		}
		if time.Now().After(deadline) {
			h.t.Fatal("write timed out")
		}
		h.step()
	}
}

// read steps the server until pred(acc) holds or the peer closes; closed
// reports whether the connection ended.
func (h *harness) read(fd int, acc []byte, pred func([]byte) bool, within time.Duration) (out []byte, closed bool) {
	h.t.Helper()
	deadline := time.Now().Add(within)
	tmp := make([]byte, 64<<10)
	for time.Now().Before(deadline) {
		h.step()
		n, err := syscall.Read(fd, tmp)
		if n > 0 {
			acc = append(acc, tmp[:n]...)
		} else if err == nil || (err != syscall.EAGAIN && err != syscall.EINTR) {
			return acc, true // EOF or reset
		}
		if pred != nil && pred(acc) {
			return acc, false
		}
	}
	return acc, false
}

type resp struct {
	status  int
	headers string
	body    string
}

// splitResponse parses one complete response off the front of b.
func splitResponse(b []byte) (r resp, rest []byte, ok bool) {
	s := string(b)
	end := strings.Index(s, "\r\n\r\n")
	if end < 0 {
		return
	}
	head := s[:end]
	line := head
	if i := strings.Index(head, "\r\n"); i >= 0 {
		line = head[:i]
	}
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return
	}
	r.status, _ = strconv.Atoi(parts[1])
	r.headers = head
	clen := 0
	for _, l := range strings.Split(head, "\r\n") {
		if k, v, found := strings.Cut(l, ":"); found && strings.EqualFold(k, "content-length") {
			clen, _ = strconv.Atoi(strings.TrimSpace(v))
		}
	}
	if len(s) < end+4+clen {
		return
	}
	r.body = s[end+4 : end+4+clen]
	return r, b[end+4+clen:], true
}

func hasResponses(n int) func([]byte) bool {
	return func(b []byte) bool {
		for i := 0; i < n; i++ {
			_, rest, ok := splitResponse(b)
			if !ok {
				return false
			}
			b = rest
		}
		return true
	}
}

func (h *harness) roundTrip(fd int, req string) resp {
	h.t.Helper()
	h.write(fd, req)
	b, _ := h.read(fd, nil, hasResponses(1), 3*time.Second)
	r, _, ok := splitResponse(b)
	if !ok {
		h.t.Fatalf("no complete response to %q, got %q", req, b)
	}
	return r
}

func get(path string) string { return "GET " + path + " HTTP/1.1\r\nHost: t\r\n\r\n" }

func TestBasicRoutes(t *testing.T) {
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	for _, tc := range []struct {
		req    string
		status int
		body   string
	}{
		{get("/"), 200, "hello\n"},
		{get("/hello/gina"), 200, "hello gina\n"},
		{get("/q?name=a%20b"), 200, "a b"},
		{get("/json"), 200, `{"ok":true}`},
		{get("/missing"), 404, "not found\n"},
		{"POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n", 405, "method not allowed\n"},
	} {
		r := h.roundTrip(c, tc.req)
		if r.status != tc.status || r.body != tc.body {
			t.Errorf("%q: got %d %q, want %d %q", tc.req, r.status, r.body, tc.status, tc.body)
		}
	}
	r := h.roundTrip(c, get("/"))
	for _, want := range []string{"Date: ", "Server: gina", "Content-Type: text/plain", "Content-Length: 6"} {
		if !strings.Contains(r.headers, want) {
			t.Errorf("missing %q in %q", want, r.headers)
		}
	}
	// HEAD: headers (with Content-Length) but no body
	h.write(c, "HEAD / HTTP/1.1\r\nHost: t\r\n\r\n")
	b, _ := h.read(c, nil, func(b []byte) bool { return strings.Contains(string(b), "\r\n\r\n") }, time.Second)
	if !strings.Contains(string(b), "Content-Length: 6") || strings.HasSuffix(string(b), "hello\n") {
		t.Fatalf("HEAD response: %q", b)
	}
	if h.srv.Requests() < 8 {
		t.Fatalf("requests = %d", h.srv.Requests())
	}
}

func TestKeepAliveAndPipelining(t *testing.T) {
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	h.write(c, get("/hello/a")+get("/hello/b")+get("/hello/c"))
	b, closed := h.read(c, nil, hasResponses(3), 3*time.Second)
	if closed {
		t.Fatal("connection closed during pipelining")
	}
	for _, want := range []string{"a", "b", "c"} {
		r, rest, ok := splitResponse(b)
		if !ok || r.body != "hello "+want+"\n" {
			t.Fatalf("pipelined response %q: %+v ok=%v", want, r, ok)
		}
		b = rest
	}
	if r := h.roundTrip(c, get("/")); r.body != "hello\n" { // still usable afterwards
		t.Fatalf("after pipeline: %+v", r)
	}
	// Connection: close is honoured
	h.write(c, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
	b, closed = h.read(c, nil, nil, time.Second)
	if !closed || !strings.Contains(string(b), "Connection: close") {
		t.Fatalf("closed=%v resp=%q", closed, b)
	}
}

func TestBodiesSplitAndLarge(t *testing.T) {
	h := start(t, ghttp.Config{ReadBufSize: 256}, 1)
	c := h.dial(0)
	body := strings.Repeat("0123456789", 5000) // 50 KB, far above the initial buffer
	req := "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	h.write(c, req+body[:10])
	for i := 0; i < 50; i++ {
		h.step() // headers + 10 body bytes arrived; the rest has not
	}
	h.write(c, body[10:])
	b, _ := h.read(c, nil, hasResponses(1), 3*time.Second)
	r, _, ok := splitResponse(b)
	if !ok || r.status != 200 || r.body != body {
		t.Fatalf("echo mismatch: ok=%v status=%d len=%d", ok, r.status, len(r.body))
	}
}

func TestProtocolErrorsAreAnsweredAndClosed(t *testing.T) {
	h := start(t, ghttp.Config{MaxBodyBytes: 1024}, 1)
	for _, tc := range []struct {
		name, req string
		status    int
	}{
		{"malformed", "GARBAGE\r\n\r\n", 400},
		{"http2", "GET / HTTP/2.0\r\nHost: t\r\n\r\n", 505},
		{"smuggling", "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n", 400},
		{"chunked", "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n", 501},
		{"huge header", "GET / HTTP/1.1\r\nHost: t\r\nX: " + strings.Repeat("a", 20000) + "\r\n\r\n", 431},
		{"long uri", "GET /" + strings.Repeat("a", 9000) + " HTTP/1.1\r\nHost: t\r\n\r\n", 414},
		{"body too large", "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 999999\r\n\r\n", 413},
		{"no host", "GET / HTTP/1.1\r\n\r\n", 400},
		{"unknown method", "BREW / HTTP/1.1\r\nHost: t\r\n\r\n", 501},
	} {
		c := h.dial(0)
		h.write(c, tc.req)
		b, closed := h.read(c, nil, hasResponses(1), 2*time.Second)
		r, _, ok := splitResponse(b)
		if !ok || r.status != tc.status {
			t.Errorf("%s: got %q, want status %d", tc.name, b, tc.status)
			continue
		}
		if !strings.Contains(r.headers, "Connection: close") {
			t.Errorf("%s: error response must close the connection: %q", tc.name, r.headers)
		}
		if !closed {
			if _, closed = h.read(c, nil, nil, time.Second); !closed {
				t.Errorf("%s: connection left open", tc.name)
			}
		}
	}
	// the server is still healthy after all of that
	if r := h.roundTrip(h.dial(0), get("/")); r.body != "hello\n" {
		t.Fatalf("server unhealthy: %+v", r)
	}
}

func TestHandlerPanicBecomes500AndServerSurvives(t *testing.T) {
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	r := h.roundTrip(c, get("/boom"))
	if r.status != 500 || !strings.Contains(r.headers, "Connection: close") {
		t.Fatalf("panic response: %+v", r)
	}
	if r := h.roundTrip(h.dial(0), get("/")); r.status != 200 {
		t.Fatalf("server did not survive: %+v", r)
	}
}

func TestTimeouts(t *testing.T) {
	h := start(t, ghttp.Config{IdleTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond}, 1)
	// idle keep-alive connection is dropped
	idle := h.dial(0)
	if r := h.roundTrip(idle, get("/")); r.status != 200 {
		t.Fatal("first request failed")
	}
	if _, closed := h.read(idle, nil, nil, 2*time.Second); !closed {
		t.Fatal("idle connection was not closed")
	}
	// a request that never completes gets 408, even if the client keeps dripping
	slow := h.dial(0)
	h.write(slow, "GET / HTTP/1.1\r\nHost: t\r\n")
	deadline := time.Now().Add(2 * time.Second)
	var got []byte
	for time.Now().Before(deadline) {
		h.write(slow, "X: y\r\n") // drip: must not extend the deadline
		var closed bool
		if got, closed = h.read(slow, got, hasResponses(1), 30*time.Millisecond); closed || len(got) > 0 {
			break
		}
	}
	if r, _, ok := splitResponse(got); !ok || r.status != 408 {
		t.Fatalf("slowloris: got %q, want 408", got)
	}
	for i := 0; i < 2000 && h.srv.Conns() > 0; i++ {
		h.step()
	}
	if h.srv.Conns() != 0 {
		t.Fatalf("connections leaked after timeouts: %d", h.srv.Conns())
	}
}

func TestManyConcurrentConnections(t *testing.T) {
	h := start(t, ghttp.Config{MaxConns: 300}, 1)
	const n = 200
	fds := make([]int, n)
	for i := range fds {
		fds[i] = h.dial(0)
	}
	for i, fd := range fds {
		h.write(fd, get("/hello/c"+strconv.Itoa(i)))
	}
	for i, fd := range fds {
		b, _ := h.read(fd, nil, hasResponses(1), 5*time.Second)
		if r, _, ok := splitResponse(b); !ok || r.body != "hello c"+strconv.Itoa(i)+"\n" {
			t.Fatalf("conn %d: %q", i, b)
		}
	}
	if h.srv.Conns() != n || h.srv.Requests() != n {
		t.Fatalf("conns=%d requests=%d, want %d", h.srv.Conns(), h.srv.Requests(), n)
	}
	for _, fd := range fds {
		syscall.Close(fd)
	}
	for i := 0; i < 2000 && h.srv.Conns() > 0; i++ {
		h.step()
	}
	if h.srv.Conns() != 0 || h.sys.Shard(0).Live() != 1 { // only the listener remains
		t.Fatalf("leak: conns=%d live=%d", h.srv.Conns(), h.sys.Shard(0).Live())
	}
	if err := h.sys.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestMaxConnsShedsExcessConnections(t *testing.T) {
	h := start(t, ghttp.Config{MaxConns: 2}, 1)
	fds := []int{h.dial(0), h.dial(0), h.dial(0), h.dial(0)}
	for i := 0; i < 500; i++ {
		h.step()
	}
	if h.srv.Rejected() != 2 || h.srv.Conns() != 2 {
		t.Fatalf("rejected=%d conns=%d, want 2 and 2", h.srv.Rejected(), h.srv.Conns())
	}
	served := 0
	for _, fd := range fds {
		h.write(fd, get("/"))
		if b, _ := h.read(fd, nil, hasResponses(1), 300*time.Millisecond); len(b) > 0 {
			served++
		}
	}
	if served != 2 {
		t.Fatalf("served %d connections, want 2", served)
	}
}

func freePort(t *testing.T) uint16 {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	sa, _ := syscall.Getsockname(fd)
	return uint16(sa.(*syscall.SockaddrInet4).Port)
}

func TestReusePortSharedAcrossShardsAndSystems(t *testing.T) {
	port := freePort(t)
	cfg := ghttp.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: true}
	h := start(t, cfg, 3)
	for i := 0; i < 3; i++ {
		if h.srv.Port(i) != port {
			t.Fatalf("shard %d bound port %d, want %d", i, h.srv.Port(i), port)
		}
	}
	// a second System (what a Prefork worker is) can bind the very same port
	h2 := start(t, cfg, 1)
	if h2.srv.Port(0) != port {
		t.Fatal("second system did not bind the shared port")
	}
	// every connection is served by one of the 4 listeners
	const n = 80
	done := 0
	seen := map[string]int{}
	for i := 0; i < n; i++ {
		fd := h.dial(0)
		h.write(fd, get("/shard"))
		var b []byte
		for tries := 0; tries < 2000 && len(b) == 0; tries++ {
			h2.sys.Step()
			b, _ = h.read(fd, nil, hasResponses(1), time.Millisecond)
		}
		if r, _, ok := splitResponse(b); ok && r.status == 200 {
			done++
			seen[r.body]++
		}
		syscall.Close(fd)
	}
	if done != n {
		t.Fatalf("only %d/%d requests answered", done, n)
	}
	total := h.srv.Requests() + h2.srv.Requests()
	if total != n {
		t.Fatalf("requests across systems = %d, want %d", total, n)
	}
	if h.srv.Requests() == 0 || h2.srv.Requests() == 0 {
		t.Fatalf("kernel did not spread connections across listeners: %d vs %d", h.srv.Requests(), h2.srv.Requests())
	}
	t.Logf("system A served %d, system B served %d, shards seen in A: %v", h.srv.Requests(), h2.srv.Requests(), seen)
}

func TestWithoutReusePortSecondBindFailsAndMultiShardIsRejected(t *testing.T) {
	port := freePort(t)
	cfg := ghttp.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port}
	h := start(t, cfg, 1)
	_ = h
	srv := ghttp.New(cfg, routes())
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 1)}
	srv.Install(&spec)
	if sys, err := gina.NewSystem(spec, gina.Options{}); err == nil {
		sys.Close()
		t.Fatal("second bind without SO_REUSEPORT unexpectedly succeeded")
	}
	if srv.ListenErr() != syscall.EADDRINUSE {
		t.Fatalf("listen error = %v, want EADDRINUSE", srv.ListenErr())
	}
	multi := ghttp.New(ghttp.Config{}, routes())
	if err := multi.Install(&gina.SystemSpec{Shards: make([]gina.ShardSpec, 2)}); err == nil {
		t.Fatal("2 shards without ReusePort should be rejected")
	}
}

func TestGracefulShutdownClosesConnectionsAndListener(t *testing.T) {
	h := start(t, ghttp.Config{}, 1)
	c := h.dial(0)
	if r := h.roundTrip(c, get("/")); r.status != 200 {
		t.Fatal("request failed")
	}
	port := h.srv.Port(0)
	if forced := h.sys.Shutdown(100); forced != 0 {
		t.Fatalf("%d isolates had to be force-killed", forced)
	}
	if _, closed := h.read(c, nil, nil, time.Second); !closed {
		t.Fatal("keep-alive connection survived shutdown")
	}
	fd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	defer syscall.Close(fd)
	if err := syscall.Connect(fd, &syscall.SockaddrInet4{Port: int(port), Addr: [4]byte{127, 0, 0, 1}}); err != syscall.ECONNREFUSED {
		t.Fatalf("listener still accepting after shutdown: %v", err)
	}
}
