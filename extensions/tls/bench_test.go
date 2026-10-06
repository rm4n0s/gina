package tls

import (
	"crypto"
	ctls "crypto/tls"
	"errors"
	"net"
	"testing"
	"time"
)

// capConn records the first thing a client writes (its ClientHello) and then fails.
type capConn struct{ first []byte }

func (c *capConn) Write(b []byte) (int, error) {
	if c.first == nil {
		c.first = append([]byte(nil), b...)
	}
	return len(b), nil
}
func (c *capConn) Read([]byte) (int, error)         { return 0, errors.New("capture only") }
func (c *capConn) Close() error                     { return nil }
func (c *capConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *capConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *capConn) SetDeadline(time.Time) error      { return nil }
func (c *capConn) SetReadDeadline(time.Time) error  { return nil }
func (c *capConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkServerFlight measures everything the server does on receipt of a
// ClientHello: key generation + ECDH, the key schedule, and signing
// CertificateVerify. (A recorded ClientHello can be replayed because the server
// keeps no per-client state.) Add roughly the same again for the client Finished
// round, which is cheap.
func BenchmarkServerFlight(b *testing.B) {
	for _, kind := range []string{"ecdsa-p256", "ed25519", "rsa-2048"} {
		b.Run(kind, func(b *testing.B) {
			var key crypto.Signer
			switch kind {
			case "rsa-2048":
				key = keyOf(b, "rsa")
			default:
				key = keyOf(b, kind)
			}
			cert, err := selfSignedWithKey(key, "localhost")
			if err != nil {
				b.Fatal(err)
			}
			cc := &capConn{}
			ctls.Client(cc, &ctls.Config{ServerName: "localhost", InsecureSkipVerify: true}).Handshake()
			cfg := &Config{Certificates: []ctls.Certificate{cert}}
			if err := cfg.Validate(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				srv, _ := NewServer(cfg)
				if err := srv.Feed(cc.first); err != nil || srv.OutLen() == 0 {
					b.Fatalf("flight failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkRecordLayer measures AES-GCM seal+open of full 16 KiB records.
func BenchmarkRecordLayer(b *testing.B) {
	for _, s := range suites {
		b.Run(map[uint16]string{TLS_AES_128_GCM_SHA256: "AES-128-GCM", TLS_AES_256_GCM_SHA384: "AES-256-GCM"}[s.id], func(b *testing.B) {
			secret := make([]byte, s.hash().Size())
			w, _ := newRecCipher(&s, secret)
			r, _ := newRecCipher(&s, secret)
			payload := make([]byte, maxPlaintext)
			out := make([]byte, 0, maxCiphertext+5)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out = w.seal(out[:0], recAppData, payload)
				if _, _, err := r.open(out[:5], out[5:]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
