package tls

// The client side of the handshake. It is the mirror of the server in conn.go and
// shares its record layer and key schedule, so it has the same shape: a state
// machine over bytes that never blocks. NewClient puts the ClientHello in
// Outgoing; Feed consumes the server's flight and queues our Finished.
//
// What it does: TLS 1.3, AES-128/256-GCM, X25519 or P-256 key exchange (a key share for each), server
// certificates chained to ClientConfig.RootCAs and checked against ServerName
// (ECDSA, Ed25519 and RSA-PSS signatures), ALPN, KeyUpdate. What it does not:
// HelloRetryRequest (it offers the one share every TLS 1.3 server accepts),
// session tickets (they are read and dropped), client certificates, TLS 1.2,
// 0-RTT, OCSP or certificate transparency checks.

import (
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"errors"
	"hash"
	"net/netip"
	"slices"
	"time"
)

// ClientConfig configures an outbound connection.
type ClientConfig struct {
	// ServerName is the name the certificate must be valid for: a host name or an
	// IP address. Required. It is sent as SNI unless it is an IP address.
	ServerName string
	// RootCAs are the trusted roots. Required: there is no implicit use of the
	// system's, because loading them reads files, which does not belong in a
	// handler. Load them once at startup with x509.SystemCertPool.
	RootCAs *x509.CertPool
	// NextProtos is the ALPN offer, in preference order. Empty sends no extension.
	NextProtos []string
	// CipherSuites in preference order (default AES-128-GCM, AES-256-GCM).
	CipherSuites []uint16
	// Now is the clock for certificate validity (default time.Now).
	Now func() time.Time
}

type clientState struct {
	cfg          ClientConfig
	privX, privP *ecdh.PrivateKey // one key share per group offered
	sessionID    []byte
	hello        []byte // our ClientHello message, written to the transcript once the suite is known
	hsSecret     []byte
	cHS, sHS     []byte
	leaf         *x509.Certificate
	transcript   hash.Hash
}

var sigAlgsOffered = []uint16{sigECDSAP256, sigECDSAP384, sigECDSAP521, sigEd25519, sigPSSSHA256, sigPSSSHA384, sigPSSSHA512}

// helloRetryRandom is the "random" a HelloRetryRequest carries (RFC 8446 §4.1.3).
var helloRetryRandom = []byte{
	0xCF, 0x21, 0xAD, 0x74, 0xE5, 0x9A, 0x61, 0x11, 0xBE, 0x1D, 0x8C, 0x02, 0x1E, 0x65, 0xB8, 0x91,
	0xC2, 0xA2, 0x11, 0x16, 0x7A, 0xBB, 0x8C, 0x5E, 0x07, 0x9E, 0x09, 0xE2, 0xC8, 0xA8, 0x33, 0x9C,
}

// NewClient starts a client session. The ClientHello is waiting in Outgoing.
func NewClient(cfg *ClientConfig) (*Conn, error) {
	cc := *cfg
	if cc.ServerName == "" {
		return nil, errors.New("tls: ClientConfig.ServerName is required")
	}
	if cc.RootCAs == nil {
		return nil, errors.New("tls: ClientConfig.RootCAs is required")
	}
	if len(cc.CipherSuites) == 0 {
		cc.CipherSuites = []uint16{TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384}
	}
	for _, id := range cc.CipherSuites {
		if findSuite(id) == nil {
			return nil, errors.New("tls: unsupported cipher suite (only TLS_AES_128_GCM_SHA256 and TLS_AES_256_GCM_SHA384)")
		}
	}
	for _, p := range cc.NextProtos {
		if p == "" || len(p) > 255 {
			return nil, errors.New("tls: bad ALPN protocol name")
		}
	}
	if cc.Now == nil {
		cc.Now = time.Now
	}
	// Two key shares, so that a server that supports only one of the groups (OpenSSL
	// servers configured for P-256 only, PostgreSQL's default up to version 17) needs no
	// HelloRetryRequest.
	privX, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	privP, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	cs := &clientState{cfg: cc, privX: privX, privP: privP, sessionID: make([]byte, 32)}
	if _, err := rand.Read(cs.sessionID); err != nil {
		return nil, err
	}
	c := &Conn{cl: cs, st: stWaitServerHello, sni: cc.ServerName}

	var random [32]byte
	rand.Read(random[:])
	ch := newBuilder(hsClientHello)
	ch.u16(0x0303)
	ch.bytes(random[:])
	ch.u8(len(cs.sessionID))
	ch.bytes(cs.sessionID)
	ch.u16(2 * len(cc.CipherSuites))
	for _, id := range cc.CipherSuites {
		ch.u16(int(id))
	}
	ch.u8(1)
	ch.u8(0)
	x := ch.start16()
	if _, err := netip.ParseAddr(cc.ServerName); err != nil { // SNI is for names, not addresses
		ch.u16(extServerName)
		ch.u16(5 + len(cc.ServerName))
		ch.u16(3 + len(cc.ServerName))
		ch.u8(0)
		ch.u16(len(cc.ServerName))
		ch.bytes([]byte(cc.ServerName))
	}
	ch.u16(extSupportedGroups)
	ch.u16(2 + 4)
	ch.u16(4)
	ch.u16(groupX25519)
	ch.u16(groupP256)
	ch.u16(extSigAlgs)
	ch.u16(2 + 2*len(sigAlgsOffered))
	ch.u16(2 * len(sigAlgsOffered))
	for _, a := range sigAlgsOffered {
		ch.u16(int(a))
	}
	// Chain signatures may use PKCS#1 v1.5 (many CAs still do); x509 checks them.
	certAlgs := append(slices.Clone(sigAlgsOffered), 0x0401, 0x0501, 0x0601)
	ch.u16(extSigAlgsCert)
	ch.u16(2 + 2*len(certAlgs))
	ch.u16(2 * len(certAlgs))
	for _, a := range certAlgs {
		ch.u16(int(a))
	}
	ch.u16(extSupportedVersions)
	ch.u16(3)
	ch.u8(2)
	ch.u16(0x0304)
	pubX, pubP := privX.PublicKey().Bytes(), privP.PublicKey().Bytes()
	ch.u16(extKeyShare)
	ch.u16(2 + 4 + len(pubX) + 4 + len(pubP))
	ch.u16(4 + len(pubX) + 4 + len(pubP))
	ch.u16(groupX25519)
	ch.u16(len(pubX))
	ch.bytes(pubX)
	ch.u16(groupP256)
	ch.u16(len(pubP))
	ch.bytes(pubP)
	if len(cc.NextProtos) > 0 {
		n := 0
		for _, p := range cc.NextProtos {
			n += 1 + len(p)
		}
		ch.u16(extALPN)
		ch.u16(2 + n)
		ch.u16(n)
		for _, p := range cc.NextProtos {
			ch.u8(len(p))
			ch.bytes([]byte(p))
		}
	}
	ch.end16(x)
	cs.hello = ch.finish()
	c.out = append(c.out, recHandshake, 3, 1, byte(len(cs.hello)>>8), byte(len(cs.hello)))
	c.out = append(c.out, cs.hello...)
	return c, nil
}

const extSigAlgsCert = 50

func (c *Conn) onClientHandshake(typ byte, msg []byte) error {
	switch {
	case c.st == stWaitServerHello && typ == hsServerHello:
		return c.onServerHello(msg)
	case c.st == stWaitEncryptedExtensions && typ == hsEncryptedExtensions:
		return c.onEncryptedExtensions(msg)
	case c.st == stWaitCertificate && typ == hsCertificate:
		return c.onCertificate(msg)
	case c.st == stWaitCertificateVerify && typ == hsCertificateVerify:
		return c.onCertificateVerify(msg)
	case c.st == stWaitServerFinished && typ == hsFinished:
		return c.onServerFinished(msg)
	case c.st == stConnected && typ == hsKeyUpdate:
		return c.onKeyUpdate(msg[4:])
	case c.st == stConnected && typ == hsNewSessionTicket:
		return nil // resumption is not implemented
	}
	return alert(alertUnexpectedMessage, "unexpected handshake message")
}

func (c *Conn) onServerHello(msg []byte) error {
	cs := c.cl
	bad := func() error { return alert(alertDecodeError, "malformed ServerHello") }
	r := &reader{b: msg[4:]}
	r.take(2) // legacy_version
	random := r.take(32)
	sid := r.vec8()
	suiteID := r.u16()
	comp := r.u8()
	exts := r.vec16()
	if r.bad || len(r.b) != 0 {
		return bad()
	}
	if hmac.Equal(random, helloRetryRandom) {
		return alert(alertHandshakeFailure, "server sent HelloRetryRequest, which is not implemented")
	}
	if !slices.Equal(sid, cs.sessionID) {
		return alert(alertIllegalParameter, "server did not echo the session id")
	}
	if comp != 0 {
		return alert(alertIllegalParameter, "compression is not allowed")
	}
	if !slices.Contains(cs.cfg.CipherSuites, uint16(suiteID)) {
		return alert(alertIllegalParameter, "server chose a cipher suite we did not offer")
	}
	c.suite = findSuite(uint16(suiteID))

	var share []byte
	group := 0
	version := 0
	er := &reader{b: exts}
	seen := map[int]bool{}
	for len(er.b) > 0 {
		typ := er.u16()
		data := er.vec16()
		if er.bad {
			return bad()
		}
		if seen[typ] {
			return alert(alertIllegalParameter, "duplicate extension")
		}
		seen[typ] = true
		dr := &reader{b: data}
		switch typ {
		case extSupportedVersions:
			version = dr.u16()
			if dr.bad || len(dr.b) != 0 {
				return bad()
			}
		case extKeyShare:
			g := dr.u16()
			share = dr.vec16()
			if dr.bad || len(dr.b) != 0 {
				return bad()
			}
			if g != groupX25519 && g != groupP256 {
				return alert(alertIllegalParameter, "server chose a key exchange group we did not offer")
			}
			group = g
		default:
			return alert(alertUnsupportedExtension, "unexpected extension in ServerHello")
		}
	}
	if version != 0x0304 {
		return alert(alertProtocolVersion, "server does not speak TLS 1.3")
	}
	if share == nil {
		return alert(alertMissingExtension, "no key_share in ServerHello")
	}
	if len(c.hsBuf) != 0 {
		return alert(alertUnexpectedMessage, "data after ServerHello in its record")
	}
	curve, priv := ecdh.X25519(), cs.privX
	if group == groupP256 {
		curve, priv = ecdh.P256(), cs.privP
	}
	peer, err := curve.NewPublicKey(share)
	if err != nil {
		return alert(alertIllegalParameter, "invalid key_share")
	}
	shared, err := priv.ECDH(peer)
	if err != nil {
		return alert(alertIllegalParameter, "key exchange failed")
	}

	h := c.suite.hash
	cs.transcript = h()
	cs.transcript.Write(cs.hello)
	cs.transcript.Write(msg)
	zeros := make([]byte, h().Size())
	empty := h().Sum(nil)
	early := hkdfExtract(h, nil, zeros)
	cs.hsSecret = hkdfExtract(h, deriveSecret(h, early, "derived", empty), shared)
	th := cs.transcript.Sum(nil)
	cs.cHS = deriveSecret(h, cs.hsSecret, "c hs traffic", th)
	cs.sHS = deriveSecret(h, cs.hsSecret, "s hs traffic", th)
	if c.rd, err = newRecCipher(c.suite, cs.sHS); err != nil {
		return alert(alertInternalError, err.Error())
	}
	if c.wr, err = newRecCipher(c.suite, cs.cHS); err != nil {
		return alert(alertInternalError, err.Error())
	}
	cs.privX, cs.privP, cs.hello = nil, nil, nil
	c.st = stWaitEncryptedExtensions
	return nil
}

func (c *Conn) onEncryptedExtensions(msg []byte) error {
	r := &reader{b: msg[4:]}
	exts := &reader{b: r.vec16()}
	if r.bad || len(r.b) != 0 {
		return alert(alertDecodeError, "malformed EncryptedExtensions")
	}
	for len(exts.b) > 0 {
		typ := exts.u16()
		data := exts.vec16()
		if exts.bad {
			return alert(alertDecodeError, "malformed EncryptedExtensions")
		}
		if typ != extALPN {
			continue // server_name, supported_groups and the like: nothing to do
		}
		dr := &reader{b: data}
		list := &reader{b: dr.vec16()}
		p := list.vec8()
		if dr.bad || list.bad || len(dr.b) != 0 || len(list.b) != 0 || len(p) == 0 {
			return alert(alertDecodeError, "malformed ALPN extension")
		}
		if !slices.Contains(c.cl.cfg.NextProtos, string(p)) {
			return alert(alertIllegalParameter, "server chose a protocol we did not offer")
		}
		c.alpn = string(p)
	}
	c.cl.transcript.Write(msg)
	c.st = stWaitCertificate
	return nil
}

func (c *Conn) onCertificate(msg []byte) error {
	cs := c.cl
	r := &reader{b: msg[4:]}
	if ctx := r.vec8(); r.bad || len(ctx) != 0 {
		return alert(alertDecodeError, "malformed Certificate")
	}
	list := &reader{b: r.vec24()}
	if r.bad || len(r.b) != 0 {
		return alert(alertDecodeError, "malformed Certificate")
	}
	var certs []*x509.Certificate
	for len(list.b) > 0 {
		der := list.vec24()
		list.vec16() // per-certificate extensions (OCSP, SCTs): not used
		if list.bad {
			return alert(alertDecodeError, "malformed Certificate")
		}
		if len(certs) == 8 {
			return alert(alertBadCertificate, "certificate chain is too long")
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return alert(alertBadCertificate, "unparseable certificate")
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return alert(alertBadCertificate, "server sent no certificate")
	}
	inter := x509.NewCertPool()
	for _, ic := range certs[1:] {
		inter.AddCert(ic)
	}
	_, err := certs[0].Verify(x509.VerifyOptions{
		DNSName:       cs.cfg.ServerName,
		Roots:         cs.cfg.RootCAs,
		Intermediates: inter,
		CurrentTime:   cs.cfg.Now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return alert(alertBadCertificate, "certificate rejected: "+err.Error())
	}
	cs.leaf = certs[0]
	cs.transcript.Write(msg)
	c.st = stWaitCertificateVerify
	return nil
}

func (c *Conn) onCertificateVerify(msg []byte) error {
	cs := c.cl
	r := &reader{b: msg[4:]}
	scheme := uint16(r.u16())
	sig := r.vec16()
	if r.bad || len(r.b) != 0 {
		return alert(alertDecodeError, "malformed CertificateVerify")
	}
	if !slices.Contains(sigAlgsOffered, scheme) {
		return alert(alertIllegalParameter, "server signed with a scheme we did not offer")
	}
	content := make([]byte, 0, 64+34+c.suite.hash().Size())
	for i := 0; i < 64; i++ {
		content = append(content, ' ')
	}
	content = append(content, "TLS 1.3, server CertificateVerify"...)
	content = append(content, 0)
	content = append(content, cs.transcript.Sum(nil)...)
	if err := verifySig(cs.leaf.PublicKey, scheme, content, sig); err != nil {
		return alert(alertDecryptError, "bad CertificateVerify signature: "+err.Error())
	}
	cs.transcript.Write(msg)
	c.st = stWaitServerFinished
	return nil
}

func (c *Conn) onServerFinished(msg []byte) error {
	cs := c.cl
	h := c.suite.hash
	want := finishedMAC(h, cs.sHS, cs.transcript.Sum(nil))
	if len(msg)-4 != len(want) || !hmac.Equal(msg[4:], want) {
		return alert(alertDecryptError, "bad server Finished")
	}
	if len(c.hsBuf) != 0 {
		return alert(alertUnexpectedMessage, "data after Finished in its record")
	}
	cs.transcript.Write(msg)
	thFin := cs.transcript.Sum(nil)
	zeros := make([]byte, h().Size())
	empty := h().Sum(nil)
	master := hkdfExtract(h, deriveSecret(h, cs.hsSecret, "derived", empty), zeros)
	cAP := deriveSecret(h, master, "c ap traffic", thFin)
	sAP := deriveSecret(h, master, "s ap traffic", thFin)

	fin := newBuilder(hsFinished)
	fin.bytes(finishedMAC(h, cs.cHS, thFin))
	c.out = append(c.out, recChangeCipherSpec, 3, 3, 0, 1, 1) // middlebox compatibility
	c.out = c.wr.seal(c.out, recHandshake, fin.finish())

	var err error
	if c.wr, err = newRecCipher(c.suite, cAP); err != nil {
		return alert(alertInternalError, err.Error())
	}
	if c.rd, err = newRecCipher(c.suite, sAP); err != nil {
		return alert(alertInternalError, err.Error())
	}
	cs.hsSecret, cs.cHS, cs.sHS, cs.transcript, cs.leaf = nil, nil, nil, nil, nil
	c.st = stConnected
	return nil
}

// verifySig checks a CertificateVerify signature (RFC 8446 §4.4.3) made with the
// leaf's key. The scheme must suit the key: an ECDSA scheme names its curve, and an
// RSA key signs with PSS, never PKCS#1 v1.5.
func verifySig(pub crypto.PublicKey, scheme uint16, content, sig []byte) error {
	switch scheme {
	case sigECDSAP256, sigECDSAP384, sigECDSAP521:
		k, ok := pub.(*ecdsa.PublicKey)
		if !ok {
			return errors.New("scheme does not match the certificate key")
		}
		var d []byte
		switch scheme {
		case sigECDSAP256:
			s := sha256.Sum256(content)
			d = s[:]
			if k.Curve != elliptic.P256() {
				return errors.New("curve does not match the scheme")
			}
		case sigECDSAP384:
			s := sha512.Sum384(content)
			d = s[:]
			if k.Curve != elliptic.P384() {
				return errors.New("curve does not match the scheme")
			}
		default:
			s := sha512.Sum512(content)
			d = s[:]
			if k.Curve != elliptic.P521() {
				return errors.New("curve does not match the scheme")
			}
		}
		if !ecdsa.VerifyASN1(k, d, sig) {
			return errors.New("invalid ECDSA signature")
		}
		return nil
	case sigEd25519:
		k, ok := pub.(ed25519.PublicKey)
		if !ok {
			return errors.New("scheme does not match the certificate key")
		}
		if !ed25519.Verify(k, content, sig) {
			return errors.New("invalid Ed25519 signature")
		}
		return nil
	case sigPSSSHA256, sigPSSSHA384, sigPSSSHA512:
		k, ok := pub.(*rsa.PublicKey)
		if !ok {
			return errors.New("scheme does not match the certificate key")
		}
		var d []byte
		var ht crypto.Hash
		switch scheme {
		case sigPSSSHA256:
			s := sha256.Sum256(content)
			d, ht = s[:], crypto.SHA256
		case sigPSSSHA384:
			s := sha512.Sum384(content)
			d, ht = s[:], crypto.SHA384
		default:
			s := sha512.Sum512(content)
			d, ht = s[:], crypto.SHA512
		}
		return rsa.VerifyPSS(k, ht, d, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	}
	return errors.New("unknown signature scheme")
}
