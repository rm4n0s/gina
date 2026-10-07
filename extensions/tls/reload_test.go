package tls

import (
	"bytes"
	"crypto/sha256"
	ctls "crypto/tls"
	"crypto/x509"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func clientCfg(t *testing.T, name string, protos []string, certs ...ctls.Certificate) *ctls.Config {
	return &ctls.Config{RootCAs: rootsFor(t, certs...), ServerName: name, MinVersion: ctls.VersionTLS13, NextProtos: protos}
}

func seenLeaf(t *testing.T, cfg *Config, cc *ctls.Config) *x509.Certificate {
	t.Helper()
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := handshake(t, srv, cc, 0)
	return client.ConnectionState().PeerCertificates[0]
}

func TestSetCertificatesSwapsForNewHandshakes(t *testing.T) {
	a, _ := SelfSigned("localhost")
	b, _ := SelfSigned("localhost")
	cfg := &Config{Certificates: []ctls.Certificate{a}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	first := seenLeaf(t, cfg, clientCfg(t, "localhost", nil, a, b))
	if !bytes.Equal(first.Raw, a.Certificate[0]) {
		t.Fatal("expected certificate A")
	}
	if err := cfg.SetCertificates(b); err != nil {
		t.Fatal(err)
	}
	second := seenLeaf(t, cfg, clientCfg(t, "localhost", nil, a, b))
	if !bytes.Equal(second.Raw, b.Certificate[0]) {
		t.Fatal("expected certificate B after the swap")
	}
	if cfg.SetCertificates() == nil {
		t.Fatal("an empty set must be refused")
	}
	// a copy that offers other protocols shares the store
	cp, err := cfg.WithNextProtos("h2")
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.SetCertificates(a); err != nil {
		t.Fatal(err)
	}
	if got := seenLeaf(t, cp, clientCfg(t, "localhost", []string{"h2"}, a, b)); !bytes.Equal(got.Raw, a.Certificate[0]) {
		t.Fatal("the copy did not see the swap")
	}
}

// Swapping while handshakes run on other goroutines (what the shard threads do).
func TestSetCertificatesConcurrentWithHandshakes(t *testing.T) {
	a, _ := SelfSigned("localhost")
	b, _ := SelfSigned("localhost")
	cfg := &Config{Certificates: []ctls.Certificate{a}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			if i%2 == 0 {
				cfg.SetCertificates(b)
			} else {
				cfg.SetCertificates(a)
			}
		}
	}()
	var hs sync.WaitGroup
	for g := 0; g < 4; g++ {
		hs.Add(1)
		go func() {
			defer hs.Done()
			for i := 0; i < 20; i++ {
				srv, _ := NewServer(cfg)
				p := &pipe{srv: srv}
				cc := clientCfg(t, "localhost", nil, a, b)
				if err := ctls.Client(p, cc).Handshake(); err != nil {
					t.Errorf("handshake: %v", err)
					return
				}
			}
		}()
	}
	hs.Wait()
	stop.Store(true)
	wg.Wait()
}

func TestGetCertificate(t *testing.T) {
	a, _ := SelfSigned("a.test")
	b, _ := SelfSigned("b.test")
	def, _ := SelfSigned("default.test")
	pa, _ := NewCertificate(a)
	pb, _ := NewCertificate(b)
	var calls atomic.Int32
	cfg := &Config{
		Certificates: []ctls.Certificate{def},
		GetCertificate: func(h *ClientHelloInfo) (*Certificate, error) {
			calls.Add(1)
			switch h.ServerName {
			case "a.test":
				return pa, nil
			case "b.test":
				return pb, nil
			case "boom.test":
				return nil, errors.New("no")
			}
			return nil, nil // fall back to Certificates
		},
	}
	for name, want := range map[string]ctls.Certificate{"a.test": a, "b.test": b, "default.test": def} {
		got := seenLeaf(t, cfg, clientCfg(t, name, nil, a, b, def))
		if !bytes.Equal(got.Raw, want.Certificate[0]) {
			t.Fatalf("%s: wrong certificate", name)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("GetCertificate called %d times", calls.Load())
	}
	// an error from the callback fails the handshake
	srv, _ := NewServer(cfg)
	p := &pipe{srv: srv}
	if err := ctls.Client(p, clientCfg(t, "boom.test", nil, def)).Handshake(); err == nil {
		t.Fatal("handshake should fail when GetCertificate fails")
	}
	// GetCertificate alone is a valid configuration
	if err := (&Config{GetCertificate: cfg.GetCertificate}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestALPNChallenge(t *testing.T) {
	def, _ := SelfSigned("example.test")
	const keyAuth = "token.thumbprint"
	var challenge *Certificate
	cfg := &Config{
		Certificates: []ctls.Certificate{def},
		NextProtos:   []string{"http/1.1", ACMETLS1},
		GetCertificate: func(h *ClientHelloInfo) (*Certificate, error) {
			if len(h.SupportedProtos) == 1 && h.SupportedProtos[0] == ACMETLS1 {
				return challenge, nil
			}
			return nil, nil
		},
	}
	var err error
	if challenge, err = ALPNChallengeCertificate("example.test", keyAuth); err != nil {
		t.Fatal(err)
	}
	srv, _ := NewServer(cfg)
	p := &pipe{srv: srv}
	// the validator accepts any certificate that carries the right extension
	cc := &ctls.Config{ServerName: "example.test", MinVersion: ctls.VersionTLS13, NextProtos: []string{ACMETLS1}, InsecureSkipVerify: true}
	client := ctls.Client(p, cc)
	if err := client.Handshake(); err != nil {
		t.Fatalf("%v (server: %v)", err, srv.Err())
	}
	st := client.ConnectionState()
	if st.NegotiatedProtocol != ACMETLS1 || srv.ALPN() != ACMETLS1 {
		t.Fatalf("alpn = %q / %q", st.NegotiatedProtocol, srv.ALPN())
	}
	leaf := st.PeerCertificates[0]
	want := sha256.Sum256([]byte(keyAuth))
	found := false
	for _, e := range leaf.Extensions {
		if e.Id.String() == "1.3.6.1.5.5.7.1.31" {
			found = e.Critical && bytes.HasSuffix(e.Value, want[:])
		}
	}
	if !found || len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "example.test" {
		t.Fatalf("challenge certificate wrong: %+v", leaf.Extensions)
	}
	// ordinary clients still get the real certificate
	if got := seenLeaf(t, cfg, clientCfg(t, "example.test", []string{"http/1.1"}, def)); !bytes.Equal(got.Raw, def.Certificate[0]) {
		t.Fatal("normal client got the wrong certificate")
	}
}
