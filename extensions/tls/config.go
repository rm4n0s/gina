// Package tls is a sans-I/O TLS 1.3 server for Gina. It is a state machine over
// bytes: feed it ciphertext read from a socket, take plaintext out; write
// plaintext, take ciphertext to send. It never blocks, never reads a socket and
// uses no goroutines, which is what lets it live inside an isolate (crypto/tls
// needs a blocking net.Conn and cannot be driven this way).
//
// All cryptography comes from the Go standard library (AES-GCM, ECDH, ECDSA,
// Ed25519, RSA-PSS, HMAC, x509); this package implements only the protocol.
//
// Supported: TLS 1.3, TLS_AES_128_GCM_SHA256 and TLS_AES_256_GCM_SHA384, key
// exchange X25519 and P-256, ECDSA (P-256/384/521), Ed25519 and RSA-PSS
// certificates, ALPN, SNI-based certificate choice, KeyUpdate, close_notify.
// Not supported: TLS 1.2 and earlier, ChaCha20-Poly1305, HelloRetryRequest,
// session resumption/tickets, 0-RTT, client certificates.
package tls

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	ctls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"time"
)

// Signature schemes (RFC 8446 §4.2.3).
const (
	sigECDSAP256 = 0x0403
	sigECDSAP384 = 0x0503
	sigECDSAP521 = 0x0603
	sigEd25519   = 0x0807
	sigPSSSHA256 = 0x0804
	sigPSSSHA384 = 0x0805
	sigPSSSHA512 = 0x0806
)

const (
	TLS_AES_128_GCM_SHA256 = 0x1301
	TLS_AES_256_GCM_SHA384 = 0x1302
)

var suites = []suite{
	{TLS_AES_128_GCM_SHA256, 16, sha256.New},
	{TLS_AES_256_GCM_SHA384, 32, sha512.New384},
}

func findSuite(id uint16) *suite {
	for i := range suites {
		if suites[i].id == id {
			return &suites[i]
		}
	}
	return nil
}

// Config configures a server connection. It must not be modified after first use.
type Config struct {
	// Certificates are tried in order; the first whose leaf matches the client's
	// SNI is used, else the first one. At least one is required.
	Certificates []ctls.Certificate
	// NextProtos is the ALPN list in server preference order (default "http/1.1").
	// If the client offers ALPN and nothing matches, the handshake fails.
	NextProtos []string
	// CipherSuites in server preference order (default AES-128-GCM, AES-256-GCM).
	CipherSuites []uint16

	prepared bool
	certs    []preparedCert
}

type preparedCert struct {
	cert    *ctls.Certificate
	leaf    *x509.Certificate
	signer  crypto.Signer
	sigAlgs []uint16
}

// Validate checks and prepares the configuration; it is called automatically by
// NewServer, but call it at startup to fail fast.
func (c *Config) Validate() error {
	if c.prepared {
		return nil
	}
	if len(c.Certificates) == 0 {
		return errors.New("tls: no certificates")
	}
	if len(c.NextProtos) == 0 {
		c.NextProtos = []string{"http/1.1"}
	}
	if len(c.CipherSuites) == 0 {
		c.CipherSuites = []uint16{TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384}
	}
	for _, id := range c.CipherSuites {
		if findSuite(id) == nil {
			return errors.New("tls: unsupported cipher suite (only TLS_AES_128_GCM_SHA256 and TLS_AES_256_GCM_SHA384)")
		}
	}
	c.certs = nil
	for i := range c.Certificates {
		cert := &c.Certificates[i]
		if len(cert.Certificate) == 0 {
			return errors.New("tls: certificate has no chain")
		}
		signer, ok := cert.PrivateKey.(crypto.Signer)
		if !ok {
			return errors.New("tls: certificate private key cannot sign")
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return err
		}
		var algs []uint16
		switch pub := signer.Public().(type) {
		case *ecdsa.PublicKey:
			switch pub.Curve {
			case elliptic.P256():
				algs = []uint16{sigECDSAP256}
			case elliptic.P384():
				algs = []uint16{sigECDSAP384}
			case elliptic.P521():
				algs = []uint16{sigECDSAP521}
			}
		case ed25519.PublicKey:
			algs = []uint16{sigEd25519}
		case *rsa.PublicKey:
			algs = []uint16{sigPSSSHA256, sigPSSSHA384, sigPSSSHA512}
		}
		if algs == nil {
			return errors.New("tls: unsupported private key type")
		}
		c.certs = append(c.certs, preparedCert{cert: cert, leaf: leaf, signer: signer, sigAlgs: algs})
	}
	c.prepared = true
	return nil
}

// pick chooses the certificate for an SNI name (first match, else the first).
func (c *Config) pick(sni string) *preparedCert {
	if sni != "" {
		for i := range c.certs {
			if c.certs[i].leaf.VerifyHostname(sni) == nil {
				return &c.certs[i]
			}
		}
	}
	return &c.certs[0]
}

func sign(key crypto.Signer, scheme uint16, content []byte) ([]byte, error) {
	switch scheme {
	case sigECDSAP256:
		d := sha256.Sum256(content)
		return key.Sign(rand.Reader, d[:], crypto.SHA256)
	case sigECDSAP384:
		d := sha512.Sum384(content)
		return key.Sign(rand.Reader, d[:], crypto.SHA384)
	case sigECDSAP521:
		d := sha512.Sum512(content)
		return key.Sign(rand.Reader, d[:], crypto.SHA512)
	case sigEd25519:
		return key.Sign(rand.Reader, content, crypto.Hash(0))
	case sigPSSSHA256:
		d := sha256.Sum256(content)
		return key.Sign(rand.Reader, d[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA256})
	case sigPSSSHA384:
		d := sha512.Sum384(content)
		return key.Sign(rand.Reader, d[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA384})
	case sigPSSSHA512:
		d := sha512.Sum512(content)
		return key.Sign(rand.Reader, d[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: crypto.SHA512})
	}
	return nil, errors.New("tls: unknown signature scheme")
}

// LoadX509KeyPair reads a PEM certificate chain and private key.
func LoadX509KeyPair(certFile, keyFile string) (ctls.Certificate, error) {
	return ctls.LoadX509KeyPair(certFile, keyFile)
}

// SelfSigned creates a throwaway ECDSA P-256 certificate for development. It is
// valid for the given host names / IP addresses (default "localhost", 127.0.0.1).
func SelfSigned(hosts ...string) (ctls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return ctls.Certificate{}, err
	}
	return selfSignedWithKey(key, hosts...)
}

func selfSignedWithKey(key crypto.Signer, hosts ...string) (ctls.Certificate, error) {
	if len(hosts) == 0 {
		hosts = []string{"localhost", "127.0.0.1"}
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: hosts[0], Organization: []string{"gina development"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true, // self-signed: usable as its own trust root in tests
	}
	for _, h := range hosts {
		if ip := parseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return ctls.Certificate{}, err
	}
	return ctls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

func parseIP(s string) []byte {
	var ip [4]byte
	n, v, dots := 0, 0, 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '.' {
			if n == 0 || v > 255 {
				return nil
			}
			if dots < 4 {
				ip[dots] = byte(v)
			}
			dots++
			n, v = 0, 0
			continue
		}
		if s[i] < '0' || s[i] > '9' {
			return nil
		}
		v, n = v*10+int(s[i]-'0'), n+1
	}
	if dots != 4 {
		return nil
	}
	return ip[:]
}
