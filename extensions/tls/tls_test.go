package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	ctls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// pipe connects Go's crypto/tls *client* (the independent reference
// implementation) to our sans-I/O server without goroutines: every client
// Write feeds the server immediately, and the server's reply is queued for the
// client's next Read.
type pipe struct {
	srv      *Conn
	toClient []byte
	chunk    int // if > 0, feed the server this many bytes at a time
}

func (p *pipe) pull() {
	p.toClient = append(p.toClient, p.srv.Outgoing()...)
	p.srv.ConsumeOut(p.srv.OutLen())
}

func (p *pipe) Write(b []byte) (int, error) {
	data := b
	for len(data) > 0 {
		n := len(data)
		if p.chunk > 0 && n > p.chunk {
			n = p.chunk
		}
		p.srv.Feed(data[:n]) // a failure queues an alert; the client will read it
		data = data[n:]
	}
	p.pull()
	return len(b), nil
}

func (p *pipe) Read(b []byte) (int, error) {
	if len(p.toClient) == 0 {
		return 0, errors.New("pipe: client would block")
	}
	n := copy(b, p.toClient)
	p.toClient = p.toClient[n:]
	return n, nil
}

func (p *pipe) Close() error                       { return nil }
func (p *pipe) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (p *pipe) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (p *pipe) SetDeadline(t time.Time) error      { return nil }
func (p *pipe) SetReadDeadline(t time.Time) error  { return nil }
func (p *pipe) SetWriteDeadline(t time.Time) error { return nil }

func rootsFor(t *testing.T, certs ...ctls.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, c := range certs {
		leaf, err := x509.ParseCertificate(c.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		pool.AddCert(leaf)
	}
	return pool
}

func keyOf(t testing.TB, kind string) crypto.Signer {
	switch kind {
	case "ecdsa-p256":
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		return k
	case "ecdsa-p384":
		k, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		return k
	case "ed25519":
		_, k, _ := ed25519.GenerateKey(rand.Reader)
		return k
	case "rsa":
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	t.Fatal("unknown key kind")
	return nil
}

func handshake(t *testing.T, srv *Conn, cc *ctls.Config, chunk int) (*ctls.Conn, *pipe) {
	t.Helper()
	p := &pipe{srv: srv, chunk: chunk}
	client := ctls.Client(p, cc)
	if err := client.Handshake(); err != nil {
		t.Fatalf("client handshake failed: %v (server error: %v)", err, srv.Err())
	}
	if !srv.HandshakeComplete() {
		t.Fatal("client finished but server did not")
	}
	return client, p
}

func TestInteropWithGoClient(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "ed25519", "rsa"} {
		for _, order := range [][]uint16{
			{TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384},
			{TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256},
		} {
			for _, curve := range []ctls.CurveID{ctls.X25519, ctls.CurveP256} {
				cert, err := selfSignedWithKey(keyOf(t, kind), "localhost")
				if err != nil {
					t.Fatal(err)
				}
				cfg := &Config{Certificates: []ctls.Certificate{cert}, CipherSuites: order}
				srv, err := NewServer(cfg)
				if err != nil {
					t.Fatal(err)
				}
				client, p := handshake(t, srv, &ctls.Config{
					RootCAs: rootsFor(t, cert), ServerName: "localhost", MinVersion: ctls.VersionTLS13,
					NextProtos: []string{"h2", "http/1.1"}, CurvePreferences: []ctls.CurveID{curve},
				}, 0)
				st := client.ConnectionState()
				if st.Version != ctls.VersionTLS13 || st.CipherSuite != order[0] || st.NegotiatedProtocol != "http/1.1" {
					t.Fatalf("%s/%x/%v: version=%x suite=%x alpn=%q", kind, order[0], curve, st.Version, st.CipherSuite, st.NegotiatedProtocol)
				}
				if srv.ALPN() != "http/1.1" || srv.CipherSuite() != order[0] || srv.ServerName() != "localhost" {
					t.Fatalf("server view: alpn=%q suite=%x sni=%q", srv.ALPN(), srv.CipherSuite(), srv.ServerName())
				}

				// application data both ways, including multi-record payloads
				for _, size := range []int{1, 100, 16384, 100_000} {
					up := make([]byte, size)
					rand.Read(up)
					if _, err := client.Write(up); err != nil {
						t.Fatal(err)
					}
					got := make([]byte, srv.PlainLen())
					srv.ReadPlain(got)
					if !bytes.Equal(got, up) {
						t.Fatalf("%s: client->server mismatch at %d bytes", kind, size)
					}
					down := make([]byte, size)
					rand.Read(down)
					if err := srv.Write(down); err != nil {
						t.Fatal(err)
					}
					p.pull()
					back := make([]byte, size)
					if _, err := io.ReadFull(client, back); err != nil {
						t.Fatalf("%s: read %d: %v", kind, size, err)
					}
					if !bytes.Equal(back, down) {
						t.Fatalf("%s: server->client mismatch at %d bytes", kind, size)
					}
				}
			}
		}
	}
}

func TestHandshakeFedOneByteAtATime(t *testing.T) {
	cert, _ := SelfSigned("localhost")
	srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
	handshake(t, srv, &ctls.Config{RootCAs: rootsFor(t, cert), ServerName: "localhost"}, 1)
}

func TestSNICertificateSelection(t *testing.T) {
	a, _ := SelfSigned("a.test")
	b, _ := SelfSigned("b.test")
	srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{a, b}})
	client, _ := handshake(t, srv, &ctls.Config{RootCAs: rootsFor(t, a, b), ServerName: "b.test"}, 0)
	if names := client.ConnectionState().PeerCertificates[0].DNSNames; len(names) != 1 || names[0] != "b.test" {
		t.Fatalf("server sent the wrong certificate: %v", names)
	}
	srv2, _ := NewServer(&Config{Certificates: []ctls.Certificate{a, b}})
	client2, _ := handshake(t, srv2, &ctls.Config{RootCAs: rootsFor(t, a, b), ServerName: "a.test"}, 0)
	if names := client2.ConnectionState().PeerCertificates[0].DNSNames; names[0] != "a.test" {
		t.Fatalf("wrong certificate for a.test: %v", names)
	}
}

func TestClientVerifiesOurCertificateChain(t *testing.T) {
	cert, _ := SelfSigned("localhost")
	other, _ := SelfSigned("localhost")
	srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
	p := &pipe{srv: srv}
	// trusting a *different* root must fail: proves the client really verified us
	client := ctls.Client(p, &ctls.Config{RootCAs: rootsFor(t, other), ServerName: "localhost"})
	if err := client.Handshake(); err == nil {
		t.Fatal("client accepted a certificate it does not trust")
	}
}

func TestCloseNotify(t *testing.T) {
	cert, _ := SelfSigned("localhost")
	srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
	client, p := handshake(t, srv, &ctls.Config{RootCAs: rootsFor(t, cert), ServerName: "localhost"}, 0)
	srv.CloseNotify()
	p.pull()
	if n, err := client.Read(make([]byte, 8)); n != 0 || err != io.EOF {
		t.Fatalf("client read after close_notify: %d, %v", n, err)
	}
	srv2, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
	client2, _ := handshake(t, srv2, &ctls.Config{RootCAs: rootsFor(t, cert), ServerName: "localhost"}, 0)
	client2.Close()
	if !srv2.PeerClosed() {
		t.Fatal("server did not see the client's close_notify")
	}
}

func alertCode(t *testing.T, err error) uint8 {
	t.Helper()
	var ae *AlertError
	if !errors.As(err, &ae) {
		t.Fatalf("not an AlertError: %v", err)
	}
	return ae.Code
}

func TestHandshakeFailuresSendTheRightAlerts(t *testing.T) {
	cert, _ := SelfSigned("localhost")
	roots := rootsFor(t, cert)
	for _, tc := range []struct {
		name string
		cc   *ctls.Config
		want uint8
	}{
		{"tls1.2 only", &ctls.Config{RootCAs: roots, ServerName: "localhost", MaxVersion: ctls.VersionTLS12}, alertProtocolVersion},
		{"alpn mismatch", &ctls.Config{RootCAs: roots, ServerName: "localhost", NextProtos: []string{"h2"}}, alertNoApplicationProtocol},
	} {
		srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
		client := ctls.Client(&pipe{srv: srv}, tc.cc)
		if err := client.Handshake(); err == nil {
			t.Fatalf("%s: handshake unexpectedly succeeded", tc.name)
		}
		if got := alertCode(t, srv.Err()); got != tc.want {
			t.Errorf("%s: server alert %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestProtocolViolations(t *testing.T) {
	cert, _ := SelfSigned("localhost")
	newSrv := func() *Conn { s, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}}); return s }

	srv := newSrv() // plain HTTP on a TLS port: rejected at once, no alert bytes
	if err := srv.Feed([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")); err == nil || srv.OutLen() != 0 {
		t.Fatalf("plain HTTP: err=%v out=%d", err, srv.OutLen())
	}
	srv = newSrv() // oversized record
	if err := srv.Feed([]byte{22, 3, 3, 0x7f, 0xff}); alertCode(t, err) != alertRecordOverflow {
		t.Fatalf("oversized record: %v", err)
	}
	srv = newSrv() // application data before any handshake
	if err := srv.Feed([]byte{23, 3, 3, 0, 1, 0}); alertCode(t, err) != alertUnexpectedMessage {
		t.Fatalf("early app data: %v", err)
	}
	srv = newSrv() // garbage ClientHello
	if err := srv.Feed([]byte{22, 3, 1, 0, 6, 1, 0, 0, 2, 0, 0}); alertCode(t, err) != alertDecodeError {
		t.Fatalf("garbage ClientHello: %v", err)
	}

	// a tampered record after the handshake is rejected with bad_record_mac
	client, p := handshake(t, newSrv(), &ctls.Config{RootCAs: rootsFor(t, cert), ServerName: "localhost"}, 0)
	_ = client
	bad := append([]byte{23, 3, 3, 0, 40}, make([]byte, 40)...)
	if err := p.srv.Feed(bad); alertCode(t, err) != alertBadRecordMAC {
		t.Fatalf("tampered record: %v", err)
	}
	if p.srv.Feed([]byte{23, 3, 3, 0, 1, 0}) == nil {
		t.Fatal("a failed connection must stay failed")
	}
}

// expandLabelRef is an independent HKDF-Expand-Label built on crypto/hkdf.
func expandLabelRef(secret []byte, label string, n int) []byte {
	info := []byte{byte(n >> 8), byte(n), byte(6 + len(label))}
	info = append(info, "tls13 "...)
	info = append(info, label...)
	info = append(info, 0)
	out, err := hkdf.Expand(sha256.New, secret, string(info), n)
	if err != nil {
		panic(err)
	}
	return out
}

func TestKeyUpdate(t *testing.T) {
	cert, _ := SelfSigned("localhost")
	srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}, CipherSuites: []uint16{TLS_AES_128_GCM_SHA256}})
	_, _ = handshake(t, srv, &ctls.Config{RootCAs: rootsFor(t, cert), ServerName: "localhost"}, 0)

	// a hand-made client record layer sharing the server's read-direction secret
	clientW, err := newRecCipher(srv.suite, srv.rd.secret)
	if err != nil {
		t.Fatal(err)
	}
	clientW.seq = srv.rd.seq
	serverOld, _ := newRecCipher(srv.suite, srv.wr.secret)
	serverOld.seq = srv.wr.seq
	oldClient, oldServer := srv.rd.secret, srv.wr.secret

	// client sends KeyUpdate(update_requested) under its current key
	if err := srv.Feed(clientW.seal(nil, recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 1})); err != nil {
		t.Fatalf("KeyUpdate rejected: %v", err)
	}
	// the server answers with KeyUpdate(update_not_requested) under its OLD key
	out := srv.Outgoing()
	inner, body, err := serverOld.open(out[:5], out[5:])
	if err != nil || inner != recHandshake || !bytes.Equal(body, []byte{hsKeyUpdate, 0, 0, 1, 0}) {
		t.Fatalf("server KeyUpdate reply: inner=%d body=%x err=%v", inner, body, err)
	}
	srv.ConsumeOut(srv.OutLen())

	// both directions now use independently derived "traffic upd" secrets
	nextClient := expandLabelRef(oldClient, "traffic upd", 32)
	nextServer := expandLabelRef(oldServer, "traffic upd", 32)
	if !bytes.Equal(srv.rd.secret, nextClient) || !bytes.Equal(srv.wr.secret, nextServer) {
		t.Fatal("updated traffic secrets differ from the independent derivation")
	}
	newClientW, _ := newRecCipher(srv.suite, nextClient)
	if err := srv.Feed(newClientW.seal(nil, recAppData, []byte("after key update"))); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, srv.PlainLen())
	srv.ReadPlain(got)
	if string(got) != "after key update" {
		t.Fatalf("plaintext after KeyUpdate: %q", got)
	}
	srv.Write([]byte("reply"))
	newServerR, _ := newRecCipher(srv.suite, nextServer)
	out = srv.Outgoing()
	if _, body, err := newServerR.open(out[:5], out[5:]); err != nil || string(body) != "reply" {
		t.Fatalf("server data under the new key: %q %v", body, err)
	}
}

func TestHKDFMatchesStdlib(t *testing.T) {
	ikm, salt, info := bytes.Repeat([]byte{0x0b}, 22), []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, []byte{0xf0, 0xf1, 0xf2, 0xf3}
	prk := hkdfExtract(sha256.New, salt, ikm)
	want, _ := hkdf.Extract(sha256.New, ikm, salt)
	if !bytes.Equal(prk, want) {
		t.Fatal("extract differs from crypto/hkdf")
	}
	for _, n := range []int{1, 32, 33, 64, 100} {
		wantOKM, _ := hkdf.Expand(sha256.New, prk, string(info), n)
		if !bytes.Equal(hkdfExpand(sha256.New, prk, info, n), wantOKM) {
			t.Fatalf("expand(%d) differs from crypto/hkdf", n)
		}
	}
}
