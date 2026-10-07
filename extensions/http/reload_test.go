//go:build linux

package http_test

import (
	"bytes"
	ctls "crypto/tls"
	"io"
	"testing"

	ghttp "github.com/rm4n0s/gina/extensions/http"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// A renewed certificate reaches a running server without a restart; connections
// opened before the swap are unaffected.
func TestHTTPSCertificateHotSwap(t *testing.T) {
	a, _ := gtls.SelfSigned("localhost")
	b, _ := gtls.SelfSigned("localhost")
	tcfg := &gtls.Config{Certificates: []ctls.Certificate{a}}
	h := &tlsHarness{harness: start(t, ghttp.Config{TLS: tcfg}, 1)}
	insecure := func() *ctls.Config { return &ctls.Config{InsecureSkipVerify: true, ServerName: "localhost"} }

	old, err := h.client(insecure())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old.ConnectionState().PeerCertificates[0].Raw, a.Certificate[0]) {
		t.Fatal("expected certificate A")
	}
	if err := tcfg.SetCertificates(b); err != nil {
		t.Fatal(err)
	}
	fresh, err := h.client(insecure())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fresh.ConnectionState().PeerCertificates[0].Raw, b.Certificate[0]) {
		t.Fatal("new connection should get certificate B")
	}
	old.Write([]byte(get("/")))
	if r := readResponses(t, old, 1)[0]; r.status != 200 {
		t.Fatalf("old connection broke: %d", r.status)
	}
}

// A TLS-ALPN-01 validation connection completes its handshake and is then
// closed by the server, without a request being read.
func TestHTTPSALPN01ChallengeConnectionClosedAfterHandshake(t *testing.T) {
	def, _ := gtls.SelfSigned("localhost")
	challenge, err := gtls.ALPNChallengeCertificate("localhost", "token.thumb")
	if err != nil {
		t.Fatal(err)
	}
	tcfg := &gtls.Config{
		Certificates: []ctls.Certificate{def},
		NextProtos:   []string{"http/1.1", gtls.ACMETLS1},
		GetCertificate: func(hi *gtls.ClientHelloInfo) (*gtls.Certificate, error) {
			if len(hi.SupportedProtos) == 1 && hi.SupportedProtos[0] == gtls.ACMETLS1 {
				return challenge, nil
			}
			return nil, nil
		},
	}
	h := &tlsHarness{harness: start(t, ghttp.Config{TLS: tcfg}, 1)}
	c, err := h.client(&ctls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{gtls.ACMETLS1}})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if c.ConnectionState().NegotiatedProtocol != gtls.ACMETLS1 {
		t.Fatalf("alpn = %q", c.ConnectionState().NegotiatedProtocol)
	}
	if _, err := c.Read(make([]byte, 16)); err != io.EOF {
		t.Fatalf("expected the server to close the connection, got %v", err)
	}
	// and a normal client is still served
	n, err := h.client(&ctls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	n.Write([]byte(get("/")))
	if r := readResponses(t, n, 1)[0]; r.status != 200 {
		t.Fatalf("status %d", r.status)
	}
}
