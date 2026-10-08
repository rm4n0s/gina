// Package tls is a sans-I/O TLS 1.3 server and client for Gina. It is a state machine over
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
//
// NewServer makes the server side; NewClient (client.go) the client side, which
// offers X25519 only and verifies the server's chain with crypto/x509 against
// ClientConfig.RootCAs and the name in ClientConfig.ServerName. Both are a Conn
// with the same data plane.
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
	"encoding/asn1"
	"errors"
	"math/big"
	"sync/atomic"
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

// Config configures a server connection. Its settings must not be modified
// after first use, with one exception: the certificates can be replaced at any
// time, from any goroutine, with SetCertificates.
type Config struct {
	// Certificates are tried in order; the first whose leaf matches the client's
	// SNI is used, else the first one. At least one is required unless
	// GetCertificate is set.
	Certificates []ctls.Certificate
	// GetCertificate, when set, is asked for the certificate of every handshake
	// before Certificates are consulted. Return (nil, nil) to fall back to them.
	// It runs on the shard thread inside the handshake, so it must be quick,
	// must not block, and is called from several threads at once: keep the
	// certificates ready in an atomic.Pointer and return a *Certificate made
	// once with NewCertificate (never parse one per call). Typical uses:
	// per-domain certificates and answering TLS-ALPN-01 challenges.
	GetCertificate func(*ClientHelloInfo) (*Certificate, error)
	// NextProtos is the ALPN list in server preference order (default "http/1.1").
	// If the client offers ALPN and nothing matches, the handshake fails. Add
	// ACMETLS1 to answer TLS-ALPN-01 challenges (see ALPNChallengeCertificate);
	// the HTTP servers close such a connection once its handshake is done.
	NextProtos []string
	// CipherSuites in server preference order (default AES-128-GCM, AES-256-GCM).
	CipherSuites []uint16

	prepared bool
	store    *certStore // shared by copies made with WithNextProtos
}

// ACMETLS1 is the ALPN protocol of TLS-ALPN-01 validation connections (RFC 8737).
const ACMETLS1 = "acme-tls/1"

// ClientHelloInfo is what GetCertificate may base its choice on. The slices are
// only valid during the call.
type ClientHelloInfo struct {
	ServerName       string   // SNI ("" if none)
	SupportedProtos  []string // the client's ALPN offer
	SignatureSchemes []uint16 // the client's signature_algorithms
}

type certStore struct {
	set atomic.Pointer[[]preparedCert]
}

type preparedCert struct {
	cert    *ctls.Certificate
	leaf    *x509.Certificate
	signer  crypto.Signer
	sigAlgs []uint16
}

// Certificate is a certificate prepared for handshakes (leaf parsed, signature
// schemes worked out). Make it once, at load time; it is safe for concurrent use.
type Certificate struct{ pc preparedCert }

// NewCertificate prepares a certificate chain and key for use in handshakes.
func NewCertificate(c ctls.Certificate) (*Certificate, error) {
	pc, err := prepareCert(c)
	if err != nil {
		return nil, err
	}
	return &Certificate{pc}, nil
}

func prepareCert(in ctls.Certificate) (preparedCert, error) {
	cert := &in
	if len(cert.Certificate) == 0 {
		return preparedCert{}, errors.New("tls: certificate has no chain")
	}
	signer, ok := cert.PrivateKey.(crypto.Signer)
	if !ok {
		return preparedCert{}, errors.New("tls: certificate private key cannot sign")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return preparedCert{}, err
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
		return preparedCert{}, errors.New("tls: unsupported private key type")
	}
	return preparedCert{cert: cert, leaf: leaf, signer: signer, sigAlgs: algs}, nil
}

func prepareCerts(in []ctls.Certificate) ([]preparedCert, error) {
	out := make([]preparedCert, 0, len(in))
	for _, c := range in {
		pc, err := prepareCert(c)
		if err != nil {
			return nil, err
		}
		out = append(out, pc)
	}
	return out, nil
}

// Validate checks and prepares the configuration; it is called automatically by
// NewServer, but call it at startup to fail fast (the servers' Install does).
// It is not safe to call concurrently with the first handshakes.
func (c *Config) Validate() error {
	if c.prepared {
		return nil
	}
	if len(c.Certificates) == 0 && c.GetCertificate == nil {
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
	certs, err := prepareCerts(c.Certificates)
	if err != nil {
		return err
	}
	c.store = &certStore{}
	c.store.set.Store(&certs)
	c.prepared = true
	return nil
}

// SetCertificates atomically replaces the certificates used for new handshakes
// (renewal without a restart). Connections already established keep what they
// negotiated. Safe to call from any goroutine once the config has been
// validated (the servers do that in Install). The old Certificates field is not
// updated.
func (c *Config) SetCertificates(certs ...ctls.Certificate) error {
	if !c.prepared {
		return errors.New("tls: SetCertificates before Validate (the server's Install validates the config)")
	}
	p, err := prepareCerts(certs)
	if err != nil {
		return err
	}
	if len(p) == 0 && c.GetCertificate == nil {
		return errors.New("tls: no certificates")
	}
	c.store.set.Store(&p)
	return nil
}

// WithNextProtos validates c and returns a copy that offers different ALPN
// protocols but shares c's certificate store, so SetCertificates on either one
// reaches both.
func (c *Config) WithNextProtos(protos ...string) (*Config, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Config{Certificates: c.Certificates, GetCertificate: c.GetCertificate, NextProtos: protos,
		CipherSuites: c.CipherSuites, prepared: true, store: c.store}, nil
}

// choose selects the certificate for a ClientHello: GetCertificate first, then
// the first static certificate matching the SNI, else the first one.
func (c *Config) choose(ch *clientHello) (*preparedCert, error) {
	if c.GetCertificate != nil {
		cert, err := c.GetCertificate(&ClientHelloInfo{ServerName: ch.sni, SupportedProtos: ch.alpn, SignatureSchemes: ch.sigAlgs})
		if err != nil {
			return nil, err
		}
		if cert != nil {
			return &cert.pc, nil
		}
	}
	certs := *c.store.set.Load()
	if len(certs) == 0 {
		return nil, errors.New("no certificate for this client")
	}
	if ch.sni != "" {
		for i := range certs {
			if certs[i].leaf.VerifyHostname(ch.sni) == nil {
				return &certs[i], nil
			}
		}
	}
	return &certs[0], nil
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

// ALPNChallengeCertificate builds the certificate a TLS-ALPN-01 validation
// connection must be answered with (RFC 8737 §3): self-signed, valid for domain,
// carrying the critical acmeIdentifier extension with the SHA-256 of the key
// authorization your ACME client computed. Return it from GetCertificate when the
// ClientHello offers only ACMETLS1 (and list ACMETLS1 in Config.NextProtos).
func ALPNChallengeCertificate(domain, keyAuthorization string) (*Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(keyAuthorization))
	val, err := asn1.Marshal(digest[:]) // an OCTET STRING holding the digest
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		ExtraExtensions: []pkix.Extension{{
			Id: asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 31}, Critical: true, Value: val,
		}},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, err
	}
	return NewCertificate(ctls.Certificate{Certificate: [][]byte{der}, PrivateKey: key})
}
