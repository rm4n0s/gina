package tls

import (
	"bytes"
	ctls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// shuttle runs a client against a server, both sans-I/O, until neither has
// anything left to say. chunk > 0 delivers the bytes that many at a time.
func shuttle(c, s *Conn, chunk int) {
	for i := 0; i < 100; i++ {
		moved := false
		for _, d := range []struct{ from, to *Conn }{{c, s}, {s, c}} {
			out := append([]byte(nil), d.from.Outgoing()...)
			d.from.ConsumeOut(len(out))
			for len(out) > 0 {
				n := len(out)
				if chunk > 0 && n > chunk {
					n = chunk
				}
				d.to.Feed(out[:n])
				out = out[n:]
				moved = true
			}
		}
		if !moved {
			return
		}
	}
}

func clientFor(t *testing.T, name string, roots *x509.CertPool, protos ...string) *Conn {
	t.Helper()
	c, err := NewClient(&ClientConfig{ServerName: name, RootCAs: roots, NextProtos: protos})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientAgainstOurServer(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "ed25519", "rsa"} {
		for _, order := range [][]uint16{
			{TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384},
			{TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256},
		} {
			for _, chunk := range []int{0, 1, 7} {
				key := keyOf(t, kind)
				cert, err := selfSignedWithKey(key, "push.example", "127.0.0.1")
				if err != nil {
					t.Fatal(err)
				}
				srv, err := NewServer(&Config{Certificates: []ctls.Certificate{cert}, CipherSuites: order, NextProtos: []string{"h2", "http/1.1"}})
				if err != nil {
					t.Fatal(err)
				}
				cli := clientFor(t, "push.example", rootsFor(t, cert), "http/1.1")
				shuttle(cli, srv, chunk)
				if !cli.HandshakeComplete() || !srv.HandshakeComplete() {
					t.Fatalf("%s %x chunk %d: handshake incomplete: client %v server %v", kind, order, chunk, cli.Err(), srv.Err())
				}
				if cli.ALPN() != "http/1.1" || srv.ServerName() != "push.example" || cli.CipherSuite() != order[0] {
					t.Fatalf("%s: alpn %q sni %q suite %x", kind, cli.ALPN(), srv.ServerName(), cli.CipherSuite())
				}
				// data both ways, large enough for several records
				big := bytes.Repeat([]byte("0123456789abcdef"), 5000)
				if err := cli.Write(big); err != nil {
					t.Fatal(err)
				}
				if err := srv.Write([]byte("reply")); err != nil {
					t.Fatal(err)
				}
				shuttle(cli, srv, chunk)
				got := make([]byte, len(big)+10)
				if n := srv.ReadPlain(got); !bytes.Equal(got[:n], big) {
					t.Fatalf("%s: server read %d bytes, want %d", kind, n, len(big))
				}
				if n := cli.ReadPlain(got); string(got[:n]) != "reply" {
					t.Fatalf("%s: client read %q", kind, got[:n])
				}
				cli.CloseNotify()
				shuttle(cli, srv, chunk)
				if !srv.PeerClosed() {
					t.Fatal("close_notify not seen")
				}
			}
		}
	}
}

func TestClientIPAddressServerName(t *testing.T) {
	cert, _ := SelfSigned("127.0.0.1")
	srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
	cli := clientFor(t, "127.0.0.1", rootsFor(t, cert))
	shuttle(cli, srv, 0)
	if !cli.HandshakeComplete() {
		t.Fatal(cli.Err())
	}
	if srv.ServerName() != "" {
		t.Fatalf("SNI sent for an IP address: %q", srv.ServerName())
	}
}

func TestClientRejects(t *testing.T) {
	cert, _ := SelfSigned("push.example")
	other, _ := SelfSigned("push.example")

	cases := []struct {
		name  string
		serve func() *Conn
		host  string
		roots func() *x509.CertPool
		want  string
	}{
		{"wrong host", func() *Conn { s, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}}); return s }, "evil.example", func() *x509.CertPool { return rootsFor(t, cert) }, "certificate rejected"},
		{"untrusted", func() *Conn { s, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}}); return s }, "push.example", func() *x509.CertPool { return rootsFor(t, other) }, "certificate rejected"},
	}
	for _, tc := range cases {
		cli := clientFor(t, tc.host, tc.roots())
		srv := tc.serve()
		shuttle(cli, srv, 0)
		if cli.HandshakeComplete() || cli.Err() == nil || !strings.Contains(cli.Err().Error(), tc.want) {
			t.Fatalf("%s: err %v", tc.name, cli.Err())
		}
		var ae *AlertError
		if !errors.As(cli.Err(), &ae) || ae.Code != alertBadCertificate {
			t.Fatalf("%s: not a bad_certificate alert: %v", tc.name, cli.Err())
		}
		if err := cli.Write([]byte("x")); err == nil {
			t.Fatalf("%s: Write succeeded on a failed connection", tc.name)
		}
		if srv.Err() == nil { // the alert we queued reached the server
			t.Fatalf("%s: server did not see the client's alert", tc.name)
		}
	}

	t.Run("expired", func(t *testing.T) {
		srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
		cli, _ := NewClient(&ClientConfig{ServerName: "push.example", RootCAs: rootsFor(t, cert), Now: func() time.Time { return time.Now().Add(5 * 365 * 24 * time.Hour) }})
		shuttle(cli, srv, 0)
		if cli.Err() == nil || !strings.Contains(cli.Err().Error(), "expired") {
			t.Fatalf("err %v", cli.Err())
		}
	})
	t.Run("tampered flight", func(t *testing.T) {
		srv, _ := NewServer(&Config{Certificates: []ctls.Certificate{cert}})
		cli := clientFor(t, "push.example", rootsFor(t, cert))
		srv.Feed(cli.Outgoing())
		flight := append([]byte(nil), srv.Outgoing()...)
		flight[len(flight)-3] ^= 1 // inside the last record: Finished
		cli.ConsumeOut(cli.OutLen())
		cli.Feed(flight)
		if cli.HandshakeComplete() || cli.Err() == nil {
			t.Fatal("tampered handshake accepted")
		}
	})
	t.Run("config", func(t *testing.T) {
		if _, err := NewClient(&ClientConfig{RootCAs: x509.NewCertPool()}); err == nil {
			t.Fatal("no server name accepted")
		}
		if _, err := NewClient(&ClientConfig{ServerName: "a"}); err == nil {
			t.Fatal("no roots accepted")
		}
		if _, err := NewClient(&ClientConfig{ServerName: "a", RootCAs: x509.NewCertPool(), CipherSuites: []uint16{0x1303}}); err == nil {
			t.Fatal("unsupported suite accepted")
		}
	})
}

// goServer runs Go's crypto/tls server on one end of a net.Pipe and serves one
// request, so the client is checked against the reference implementation.
func goServer(t *testing.T, cc *ctls.Config) (net.Conn, <-chan error) {
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer b.Close()
		s := ctls.Server(b, cc)
		if err := s.Handshake(); err != nil {
			done <- err
			return
		}
		buf := make([]byte, 64)
		n, err := s.Read(buf)
		if err != nil {
			done <- err
			return
		}
		_, err = s.Write(append([]byte(s.ConnectionState().NegotiatedProtocol+":"), buf[:n]...))
		s.Close()
		done <- err
	}()
	return a, done
}

// drive moves bytes between a sans-I/O client and a net.Conn until the client
// has read want bytes of application data or the connection ends.
func drive(t *testing.T, c *Conn, nc net.Conn, request []byte, want int) (string, error) {
	t.Helper()
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	var plain []byte
	sent := false
	buf := make([]byte, 4096)
	for len(plain) < want {
		if out := c.Outgoing(); len(out) > 0 {
			if _, err := nc.Write(out); err != nil {
				return string(plain), err
			}
			c.ConsumeOut(len(out))
		}
		if c.Err() != nil {
			return string(plain), c.Err()
		}
		if c.HandshakeComplete() && !sent {
			sent = true
			c.Write(request)
			continue
		}
		n, err := nc.Read(buf)
		if n > 0 {
			c.Feed(buf[:n])
			tmp := make([]byte, c.PlainLen())
			c.ReadPlain(tmp)
			plain = append(plain, tmp...)
		}
		if err != nil {
			if err == io.EOF && len(plain) >= want {
				break
			}
			if c.Err() != nil {
				return string(plain), c.Err()
			}
			return string(plain), err
		}
	}
	return string(plain), nil
}

func TestClientAgainstGoServer(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ed25519", "rsa"} {
		for _, curve := range []ctls.CurveID{ctls.X25519, ctls.CurveP256} {
			cert, err := selfSignedWithKey(keyOf(t, kind), "go.example")
			if err != nil {
				t.Fatal(err)
			}
			nc, done := goServer(t, &ctls.Config{Certificates: []ctls.Certificate{cert}, MinVersion: ctls.VersionTLS13, NextProtos: []string{"h2", "http/1.1"}, CurvePreferences: []ctls.CurveID{curve, ctls.X25519}})
			cli := clientFor(t, "go.example", rootsFor(t, cert), "http/1.1")
			got, err := drive(t, cli, nc, []byte("hello"), len("http/1.1:hello"))
			nc.Close()
			serr := <-done
			if err != nil || got != "http/1.1:hello" {
				t.Fatalf("%s/%v: got %q err %v (server: %v)", kind, curve, got, err, serr)
			}
		}
	}
}

func TestClientAgainstGoServerRefusals(t *testing.T) {
	cert, _ := selfSignedWithKey(keyOf(t, "ecdsa-p256"), "go.example")
	t.Run("tls 1.2 only", func(t *testing.T) {
		nc, done := goServer(t, &ctls.Config{Certificates: []ctls.Certificate{cert}, MaxVersion: ctls.VersionTLS12})
		cli := clientFor(t, "go.example", rootsFor(t, cert))
		_, err := drive(t, cli, nc, []byte("x"), 1)
		nc.Close()
		<-done
		if err == nil {
			t.Fatal("TLS 1.2 server accepted")
		}
	})
	t.Run("no common group", func(t *testing.T) {
		// We offer only X25519; a server that will only do P-256 refuses.
		nc, done := goServer(t, &ctls.Config{Certificates: []ctls.Certificate{cert}, MinVersion: ctls.VersionTLS13, CurvePreferences: []ctls.CurveID{ctls.CurveP256}})
		cli := clientFor(t, "go.example", rootsFor(t, cert))
		_, err := drive(t, cli, nc, []byte("x"), 1)
		nc.Close()
		<-done
		if err == nil {
			t.Fatal("handshake succeeded without a common group")
		}
	})
	t.Run("wrong name", func(t *testing.T) {
		nc, done := goServer(t, &ctls.Config{Certificates: []ctls.Certificate{cert}, MinVersion: ctls.VersionTLS13})
		cli := clientFor(t, "other.example", rootsFor(t, cert))
		_, err := drive(t, cli, nc, []byte("x"), 1)
		nc.Close()
		<-done
		if err == nil || !strings.Contains(err.Error(), "certificate rejected") {
			t.Fatalf("err %v", err)
		}
	})
}

func TestClientHelloRetryRequestIsRefused(t *testing.T) {
	cert, _ := SelfSigned("push.example")
	cli := clientFor(t, "push.example", rootsFor(t, cert))
	sid := cli.cl.sessionID
	sh := newBuilder(hsServerHello)
	sh.u16(0x0303)
	sh.bytes(helloRetryRandom)
	sh.u8(len(sid))
	sh.bytes(sid)
	sh.u16(TLS_AES_128_GCM_SHA256)
	sh.u8(0)
	x := sh.start16()
	sh.u16(extSupportedVersions)
	sh.u16(2)
	sh.u16(0x0304)
	sh.end16(x)
	msg := sh.finish()
	rec := append([]byte{recHandshake, 3, 3, byte(len(msg) >> 8), byte(len(msg))}, msg...)
	err := cli.Feed(rec)
	var ae *AlertError
	if !errors.As(err, &ae) || ae.Code != alertHandshakeFailure || !strings.Contains(ae.Msg, "HelloRetryRequest") {
		t.Fatalf("err %v", err)
	}
}
