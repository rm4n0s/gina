//go:build linux

package http_test

import (
	"bytes"
	"crypto/rand"
	ctls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// steppedConn is a blocking net.Conn over a non-blocking socket whose "blocking"
// is cooperative: while it waits for the network it steps the server System, so
// Go's crypto/tls client and the server can share one thread (no goroutines).
type steppedConn struct {
	fd  int
	sys *gina.System
}

func (c *steppedConn) wait(deadline time.Time) error {
	if time.Now().After(deadline) {
		return os.ErrDeadlineExceeded
	}
	if !c.sys.Step() {
		time.Sleep(20 * time.Microsecond)
	}
	return nil
}

func (c *steppedConn) Read(p []byte) (int, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, err := syscall.Read(c.fd, p)
		if n > 0 {
			return n, nil
		}
		if err == nil || err == syscall.ECONNRESET {
			return 0, io.EOF
		}
		if err != syscall.EAGAIN && err != syscall.EINTR {
			return 0, err
		}
		if err := c.wait(deadline); err != nil {
			return 0, err
		}
	}
}

func (c *steppedConn) Write(p []byte) (int, error) {
	deadline := time.Now().Add(5 * time.Second)
	done := 0
	for done < len(p) {
		n, err := syscall.Write(c.fd, p[done:])
		if n > 0 {
			done += n
			continue
		}
		if err != syscall.EAGAIN && err != syscall.ENOTCONN && err != syscall.EINTR {
			return done, err
		}
		if err := c.wait(deadline); err != nil {
			return done, err
		}
	}
	return done, nil
}

func (c *steppedConn) Close() error                       { return syscall.Close(c.fd) }
func (c *steppedConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *steppedConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *steppedConn) SetDeadline(t time.Time) error      { return nil }
func (c *steppedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *steppedConn) SetWriteDeadline(t time.Time) error { return nil }

// tlsState adds the IANA cipher name to a client's ConnectionState.
type tlsState struct{ ctls.ConnectionState }

func (s tlsState) CipherSuiteName() string { return ctls.CipherSuiteName(s.CipherSuite) }

type tlsHarness struct {
	*harness
	roots *x509.CertPool
}

func startTLS(t *testing.T, cfg ghttp.Config, shards int) *tlsHarness {
	t.Helper()
	return startTLSRoutes(t, cfg, routes(), shards)
}

func startTLSWith(t *testing.T, cfg ghttp.Config, r *ghttp.Router) *tlsHarness {
	t.Helper()
	return startTLSRoutes(t, cfg, r, 1)
}

func startTLSRoutes(t *testing.T, cfg ghttp.Config, r *ghttp.Router, shards int) *tlsHarness {
	t.Helper()
	cert, err := gtls.SelfSigned("localhost", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	cfg.TLS = &gtls.Config{Certificates: []ctls.Certificate{cert}}
	return &tlsHarness{harness: startWith(t, cfg, r, shards), roots: roots}
}

func (h *tlsHarness) client(cc *ctls.Config) (*ctls.Conn, error) {
	fd := h.dial(0)
	if cc == nil {
		cc = &ctls.Config{}
	}
	if cc.RootCAs == nil {
		cc.RootCAs = h.roots
	}
	if cc.ServerName == "" {
		cc.ServerName = "localhost"
	}
	c := ctls.Client(&steppedConn{fd: fd, sys: h.sys}, cc)
	return c, c.Handshake()
}

// readResponses reads from a TLS client until n complete responses arrived.
func readResponses(t *testing.T, c *ctls.Conn, n int) []resp {
	t.Helper()
	var acc []byte
	buf := make([]byte, 32<<10)
	var out []resp
	for len(out) < n {
		r, rest, ok := splitResponse(acc)
		if ok {
			out = append(out, r)
			acc = rest
			continue
		}
		m, err := c.Read(buf)
		if m > 0 {
			acc = append(acc, buf[:m]...)
		} else if err != nil {
			t.Fatalf("read after %d/%d responses: %v (have %d bytes)", len(out), n, err, len(acc))
		}
	}
	return out
}

func TestHTTPSRoutesKeepAliveAndTLSDetails(t *testing.T) {
	h := startTLS(t, ghttp.Config{}, 1)
	c, err := h.client(&ctls.Config{NextProtos: []string{"h2", "http/1.1"}})
	if err != nil {
		t.Fatalf("handshake: %v (listen err %v)", err, h.srv.ListenErr())
	}
	st := tlsState{c.ConnectionState()}
	if st.Version != ctls.VersionTLS13 || st.NegotiatedProtocol != "http/1.1" {
		t.Fatalf("version=%x alpn=%q", st.Version, st.NegotiatedProtocol)
	}
	for _, tc := range []struct {
		path, body string
		status     int
	}{{"/", "hello\n", 200}, {"/hello/tls", "hello tls\n", 200}, {"/nope", "not found\n", 404}, {"/json", `{"ok":true}`, 200}} {
		c.Write([]byte(get(tc.path)))
		r := readResponses(t, c, 1)[0]
		if r.status != tc.status || r.body != tc.body {
			t.Errorf("%s: %d %q, want %d %q", tc.path, r.status, r.body, tc.status, tc.body)
		}
	}
	c.Write([]byte(get("/tlsinfo")))
	if r := readResponses(t, c, 1)[0]; r.body != "tls sni=localhost alpn=http/1.1 cipher="+st.CipherSuiteName() {
		t.Errorf("Context.TLS(): %q", r.body)
	}
	c.Write([]byte(get("/hello/a") + get("/hello/b") + get("/hello/c"))) // pipelined inside TLS records
	for i, r := range readResponses(t, c, 3) {
		if want := "hello " + string(rune('a'+i)) + "\n"; r.body != want {
			t.Errorf("pipelined %d: %q want %q", i, r.body, want)
		}
	}
	c.Write([]byte("GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"))
	r := readResponses(t, c, 1)[0]
	if !strings.Contains(r.headers, "Connection: close") {
		t.Fatalf("missing Connection: close: %q", r.headers)
	}
	if _, err := c.Read(make([]byte, 1)); err != io.EOF { // server sent close_notify
		t.Fatalf("after Connection: close the client should see a clean EOF, got %v", err)
	}
}

func TestHTTPSLargeBodiesBothWays(t *testing.T) {
	h := startTLS(t, ghttp.Config{ReadBufSize: 512, MaxBodyBytes: 1 << 20}, 1)
	c, err := h.client(nil)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 300_000) // many TLS records each way
	rand.Read(body)
	c.Write([]byte("POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"))
	c.Write(body)
	r := readResponses(t, c, 1)[0]
	if r.status != 200 || !bytes.Equal([]byte(r.body), body) {
		t.Fatalf("echo: status=%d len=%d want %d", r.status, len(r.body), len(body))
	}
}

func TestHTTPSRejectsWhatIsNotTLS13(t *testing.T) {
	h := startTLS(t, ghttp.Config{}, 1)
	if _, err := h.client(&ctls.Config{MaxVersion: ctls.VersionTLS12}); err == nil {
		t.Fatal("a TLS 1.2-only client was accepted")
	}
	// plain HTTP spoken to the TLS port: closed promptly, no hang, nothing leaked
	plain := h.dial(0)
	h.write(plain, get("/"))
	if _, closed := h.read(plain, nil, nil, 2*time.Second); !closed {
		t.Fatal("plain HTTP on the TLS port was not closed")
	}
	// the wrong trust root is the client's decision, but the server must survive it
	other, _ := gtls.SelfSigned("localhost")
	leaf, _ := x509.ParseCertificate(other.Certificate[0])
	wrong := x509.NewCertPool()
	wrong.AddCert(leaf)
	if _, err := h.client(&ctls.Config{RootCAs: wrong}); err == nil {
		t.Fatal("client accepted an untrusted certificate")
	}
	c, err := h.client(nil)
	if err != nil {
		t.Fatalf("server unhealthy after bad handshakes: %v", err)
	}
	c.Write([]byte(get("/")))
	if r := readResponses(t, c, 1)[0]; r.status != 200 {
		t.Fatalf("%+v", r)
	}
	for i := 0; i < 2000 && h.srv.Conns() > 1; i++ {
		h.step()
	}
	if h.srv.Conns() != 1 {
		t.Fatalf("failed handshakes leaked connections: %d open", h.srv.Conns())
	}
}

func TestHTTPSHandshakeAndIdleTimeouts(t *testing.T) {
	h := startTLS(t, ghttp.Config{ReadTimeout: 100 * time.Millisecond, IdleTimeout: 150 * time.Millisecond}, 1)
	stalled := h.dial(0) // connects and never sends a ClientHello
	if _, closed := h.read(stalled, nil, nil, 2*time.Second); !closed {
		t.Fatal("stalled handshake was not timed out")
	}
	c, err := h.client(nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Write([]byte(get("/")))
	readResponses(t, c, 1)
	deadline := time.Now().Add(2 * time.Second)
	for h.srv.Conns() > 0 && time.Now().Before(deadline) {
		h.step()
	}
	if h.srv.Conns() != 0 {
		t.Fatal("idle TLS connection was not closed")
	}
}

func TestHTTPSManyConnectionsAndShutdown(t *testing.T) {
	h := startTLS(t, ghttp.Config{MaxConns: 100}, 1)
	var conns []*ctls.Conn
	for i := 0; i < 40; i++ {
		c, err := h.client(nil)
		if err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
		conns = append(conns, c)
	}
	for i, c := range conns {
		c.Write([]byte(get("/hello/n" + strconv.Itoa(i))))
	}
	for i, c := range conns {
		if r := readResponses(t, c, 1)[0]; r.body != "hello n"+strconv.Itoa(i)+"\n" {
			t.Fatalf("conn %d: %q", i, r.body)
		}
	}
	if forced := h.sys.Shutdown(200); forced != 0 {
		t.Fatalf("%d isolates were force-killed", forced)
	}
	if err := h.sys.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
	if _, err := conns[0].Read(make([]byte, 1)); err == nil || !(errors.Is(err, io.EOF) || strings.Contains(err.Error(), "reset")) {
		t.Fatalf("connection should be closed after shutdown, got %v", err)
	}
}

func TestHTTPSAcrossReusePortShards(t *testing.T) {
	port := freePort(t)
	cert, _ := gtls.SelfSigned("localhost")
	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	th := &tlsHarness{harness: start(t, ghttp.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: true,
		TLS: &gtls.Config{Certificates: []ctls.Certificate{cert}}}, 3), roots: roots}
	seen := map[string]bool{}
	for i := 0; i < 30; i++ {
		c, err := th.client(nil)
		if err != nil {
			t.Fatal(err)
		}
		c.Write([]byte(get("/shard")))
		seen[readResponses(t, c, 1)[0].body] = true
		c.Close()
	}
	if len(seen) < 2 {
		t.Fatalf("HTTPS connections were not spread across shards: %v", seen)
	}
}
