//go:build linux

package http2_test

import (
	"bytes"
	ctls "crypto/tls"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
	"github.com/rm4n0s/gina/extensions/http2"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// The server serves h2 from a private copy of the TLS config; a renewal made on
// the user's config must still reach it, and TLS-ALPN-01 connections are closed
// after their handshake.
func TestCertificateHotSwapAndALPN01(t *testing.T) {
	a, _ := gtls.SelfSigned("localhost")
	b, _ := gtls.SelfSigned("localhost")
	challenge, _ := gtls.ALPNChallengeCertificate("localhost", "ka")
	tcfg := &gtls.Config{
		Certificates: []ctls.Certificate{a},
		NextProtos:   []string{"h2", gtls.ACMETLS1},
		GetCertificate: func(h *gtls.ClientHelloInfo) (*gtls.Certificate, error) {
			if len(h.SupportedProtos) == 1 && h.SupportedProtos[0] == gtls.ACMETLS1 {
				return challenge, nil
			}
			return nil, nil
		},
	}
	port := freePort(t)
	s := http2.New(http2.Config{Addr: [4]byte{127, 0, 0, 1}, Port: port, ReusePort: true, TLS: tcfg}, routes())
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 1)}
	if err := s.Install(&spec); err != nil {
		t.Fatal(err)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sys.Start(gina.RunOptions{ShutdownGrace: time.Second})
	t.Cleanup(func() { sys.Stop(); sys.Close() })
	h := &srv{t: t, port: port}

	handshake := func(protos ...string) *ctls.Conn {
		c := ctls.Client(dial(t, h.addr()), &ctls.Config{InsecureSkipVerify: true, ServerName: "localhost", NextProtos: protos})
		c.SetDeadline(time.Now().Add(3 * time.Second))
		if err := c.Handshake(); err != nil {
			t.Fatalf("handshake %v: %v", protos, err)
		}
		return c
	}
	if c := handshake("h2"); !bytes.Equal(c.ConnectionState().PeerCertificates[0].Raw, a.Certificate[0]) {
		t.Fatal("expected certificate A")
	}
	if err := tcfg.SetCertificates(b); err != nil {
		t.Fatal(err)
	}
	if c := handshake("h2"); !bytes.Equal(c.ConnectionState().PeerCertificates[0].Raw, b.Certificate[0]) {
		t.Fatal("expected certificate B after the swap")
	}
	c := handshake(gtls.ACMETLS1)
	if c.ConnectionState().NegotiatedProtocol != gtls.ACMETLS1 {
		t.Fatal("acme-tls/1 not negotiated")
	}
	if _, err := c.Read(make([]byte, 8)); err == nil {
		t.Fatal("expected the server to close the validation connection")
	}
}
