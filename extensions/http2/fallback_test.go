//go:build linux

package http2_test

import (
	"bytes"
	ctls "crypto/tls"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rm4n0s/gina/extensions/http2"
)

// With HTTP1Fallback one port serves both protocols: h2 clients get HTTP/2, and
// HTTP/1.1-only clients (including TLS clients that offer no ALPN at all, like
// many bots) get HTTP/1.1 from the same handlers.
func TestHTTP1FallbackSamePort(t *testing.T) {
	eachMode(t, func(t *testing.T, useTLS bool) {
		h := start(t, 1, useTLS, func(c *http2.Config) { c.HTTP1Fallback = true })

		resp, body := h.do("GET", "/hello/h2", nil)
		if resp.ProtoMajor != 2 || string(body) != "hello h2\n" {
			t.Fatalf("h2 client: %s %q", resp.Proto, body)
		}

		// a Go client restricted to HTTP/1.1
		tr := &http.Transport{DisableKeepAlives: false}
		if useTLS {
			tr.TLSClientConfig = &ctls.Config{RootCAs: h.pool}
			tr.TLSNextProto = map[string]func(string, *ctls.Conn) http.RoundTripper{} // no h2 offer
		}
		c1 := &http.Client{Transport: tr, Timeout: 10 * time.Second}
		t.Cleanup(tr.CloseIdleConnections)
		for i := 0; i < 3; i++ { // keep-alive: the same connection serves several requests
			r, err := c1.Get(h.url("/hello/h1"))
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(r.Body)
			r.Body.Close()
			if r.ProtoMajor != 1 || string(b) != "hello h1\n" {
				t.Fatalf("h1 client: %s %q", r.Proto, b)
			}
		}
		// a body, and an unknown-length (chunked) upload
		payload := strings.Repeat("0123456789abcdef", 4000)
		r, err := c1.Post(h.url("/echo"), "text/plain", io.NopCloser(strings.NewReader(payload)))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.ProtoMajor != 1 || string(b) != payload {
			t.Fatalf("chunked upload over HTTP/1.1: %s len %d", r.Proto, len(b))
		}
		if useTLS {
			// no ALPN extension at all
			c := ctls.Client(dial(t, h.addr()), &ctls.Config{RootCAs: h.pool, ServerName: "localhost"})
			c.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.Write([]byte("GET /hello/bot HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")); err != nil {
				t.Fatal(err)
			}
			all, _ := io.ReadAll(c)
			if !bytes.HasPrefix(all, []byte("HTTP/1.1 200")) || !bytes.HasSuffix(all, []byte("hello bot\n")) {
				t.Fatalf("client without ALPN: %q", all)
			}
			if st := c.ConnectionState(); st.NegotiatedProtocol != "" {
				t.Fatalf("alpn %q", st.NegotiatedProtocol)
			}
			// and one that offers only http/1.1
			c = ctls.Client(dial(t, h.addr()), &ctls.Config{RootCAs: h.pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
			c.SetDeadline(time.Now().Add(5 * time.Second))
			c.Write([]byte("GET /hello/h1only HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"))
			all, _ = io.ReadAll(c)
			if c.ConnectionState().NegotiatedProtocol != "http/1.1" || !bytes.HasSuffix(all, []byte("hello h1only\n")) {
				t.Fatalf("http/1.1 client: %q", all)
			}
		} else {
			// cleartext: the first bytes arrive in pieces, before anything is decided
			nc := dial(t, h.addr())
			nc.SetDeadline(time.Now().Add(5 * time.Second))
			nc.Write([]byte("GE"))
			time.Sleep(30 * time.Millisecond)
			nc.Write([]byte("T /hello/slow HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"))
			all, _ := io.ReadAll(nc)
			if !bytes.HasPrefix(all, []byte("HTTP/1.1 200")) || !bytes.HasSuffix(all, []byte("hello slow\n")) {
				t.Fatalf("cleartext HTTP/1.1: %q", all)
			}
		}
		// the count covers both protocols
		if got := h.srv.Requests(); got < 6 {
			t.Fatalf("Requests() = %d", got)
		}
		client := h.client
		client.CloseIdleConnections()
		tr.CloseIdleConnections()
		for i := 0; i < 100 && h.srv.Conns() != 0; i++ {
			time.Sleep(20 * time.Millisecond)
		}
		if n := h.srv.Conns(); n != 0 {
			t.Fatalf("%d connections still counted after closing every client", n)
		}
	})
}

// A client that sends the HTTP/2 preface in dribbles is still recognised.
func TestHTTP1FallbackPrefaceInPieces(t *testing.T) {
	h := start(t, 1, false, func(c *http2.Config) { c.HTTP1Fallback = true })
	nc := dial(t, h.addr())
	nc.SetDeadline(time.Now().Add(5 * time.Second))
	preface := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	for i := 0; i < len(preface); i += 5 {
		nc.Write([]byte(preface[i:min(i+5, len(preface))]))
		time.Sleep(5 * time.Millisecond)
	}
	nc.Write([]byte{0, 0, 0, 4, 0, 0, 0, 0, 0}) // an empty SETTINGS frame
	buf := make([]byte, 9)
	if _, err := io.ReadFull(nc, buf); err != nil || buf[3] != 4 { // the server's own SETTINGS
		t.Fatalf("no SETTINGS frame back: %v %x", err, buf)
	}
}

// Without the option, HTTP/1.1 stays refused as before.
func TestNoFallbackByDefault(t *testing.T) {
	h := start(t, 1, false)
	nc := dial(t, h.addr())
	nc.SetDeadline(time.Now().Add(3 * time.Second))
	nc.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
	if b, _ := io.ReadAll(nc); len(b) != 0 {
		t.Fatalf("expected the connection to be closed silently, got %q", b)
	}
}
