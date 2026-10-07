package tls

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"errors"
	"hash"
)

const (
	hsClientHello         = 1
	hsServerHello         = 2
	hsEncryptedExtensions = 8
	hsCertificate         = 11
	hsCertificateVerify   = 15
	hsFinished            = 20
	hsKeyUpdate           = 24

	extServerName        = 0
	extSupportedGroups   = 10
	extSigAlgs           = 13
	extALPN              = 16
	extSupportedVersions = 43
	extKeyShare          = 51

	groupX25519 = 0x001d
	groupP256   = 0x0017

	maxHandshakeMsg = 1 << 16
)

type state uint8

const (
	stWaitClientHello state = iota
	stWaitClientFinished
	stConnected
	stFailed
)

// Conn is one server-side TLS 1.3 session. It is not safe for concurrent use.
type Conn struct {
	cfg *Config
	st  state
	err error

	inStore []byte // buffered, not yet parsed ciphertext
	in      []byte
	out     []byte // ciphertext waiting to be sent
	plain   []byte // decrypted application data waiting to be read
	hsBuf   []byte // handshake bytes spanning records
	eof     bool   // peer sent close_notify

	rd, wr recCipher

	suite      *suite
	transcript hash.Hash
	cHS        []byte // client handshake traffic secret
	cAP        []byte // client application traffic secret (installed once Finished verifies)
	expectFin  []byte
	alpn       string
	sni        string
}

// NewServer starts a server session. cfg is validated on first use.
func NewServer(cfg *Config) (*Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Conn{cfg: cfg}, nil
}

func (c *Conn) HandshakeComplete() bool { return c.st == stConnected }

// HelloReceived reports whether the ClientHello has been processed, which is
// when ALPN and ServerName become known.
func (c *Conn) HelloReceived() bool { return c.st != stWaitClientHello }
func (c *Conn) ALPN() string        { return c.alpn }
func (c *Conn) ServerName() string  { return c.sni }
func (c *Conn) Err() error          { return c.err }
func (c *Conn) PeerClosed() bool    { return c.eof }
func (c *Conn) CipherSuite() uint16 {
	if c.suite == nil {
		return 0
	}
	return c.suite.id
}

// ---- data plane ----

// PlainLen is the number of decrypted bytes waiting to be read.
func (c *Conn) PlainLen() int { return len(c.plain) }

// ReadPlain copies decrypted application data into dst and returns the count.
func (c *Conn) ReadPlain(dst []byte) int {
	n := copy(dst, c.plain)
	c.plain = append(c.plain[:0], c.plain[n:]...)
	return n
}

// Outgoing returns the ciphertext waiting to be sent (valid until the next call
// that appends output). Report what was sent with ConsumeOut.
func (c *Conn) Outgoing() []byte { return c.out }
func (c *Conn) OutLen() int      { return len(c.out) }
func (c *Conn) ConsumeOut(n int) { c.out = append(c.out[:0], c.out[n:]...) }

// Write queues application data (split into records) for sending.
func (c *Conn) Write(p []byte) error {
	if c.st != stConnected {
		return errors.New("tls: write before handshake completed")
	}
	for len(p) > 0 {
		n := min(len(p), maxPlaintext)
		c.out = c.wr.seal(c.out, recAppData, p[:n])
		p = p[n:]
	}
	return nil
}

// CloseNotify queues a close_notify alert; send Outgoing() and then close the socket.
func (c *Conn) CloseNotify() {
	if c.wr.active && c.err == nil {
		c.out = c.wr.seal(c.out, recAlert, []byte{1, alertCloseNotify})
	}
}

// Feed consumes ciphertext read from the peer. After it returns, check
// PlainLen, OutLen (handshake flights, alerts) and Err. A non-nil error is
// fatal: a matching alert is already queued in Outgoing; send it, then close.
func (c *Conn) Feed(data []byte) error {
	if c.err != nil {
		return c.err
	}
	c.inStore = append(c.inStore, data...)
	c.in = c.inStore
	for len(c.in) >= 5 {
		typ := c.in[0]
		if typ < recChangeCipherSpec || typ > recAppData || c.in[1] != 3 {
			return c.fail(alert(alertNotTLS, "not a TLS record (plain HTTP on a TLS port?)"))
		}
		n := int(c.in[3])<<8 | int(c.in[4])
		if n > maxCiphertext {
			return c.fail(alert(alertRecordOverflow, "record too large"))
		}
		if len(c.in) < 5+n {
			break
		}
		hdr, body := c.in[:5], c.in[5:5+n]
		err := c.handleRecord(typ, hdr, body)
		c.in = c.in[5+n:]
		if err != nil {
			return c.fail(err)
		}
	}
	c.inStore = append(c.inStore[:0], c.in...)
	c.in = nil
	return nil
}

func (c *Conn) fail(err error) error {
	var ae *AlertError
	if !errors.As(err, &ae) {
		ae = alert(alertInternalError, err.Error())
	}
	if !ae.Peer && c.st != stFailed && ae.Code != alertNotTLS {
		if c.wr.active {
			c.out = c.wr.seal(c.out, recAlert, []byte{2, ae.Code})
		} else {
			c.out = append(c.out, recAlert, 3, 3, 0, 2, 2, ae.Code)
		}
	}
	c.st, c.err = stFailed, ae
	return ae
}

func (c *Conn) handleRecord(typ byte, hdr, body []byte) error {
	if typ == recChangeCipherSpec { // middlebox-compat CCS: must be a single 0x01, then ignored
		if c.st == stConnected || len(body) != 1 || body[0] != 1 {
			return alert(alertUnexpectedMessage, "unexpected change_cipher_spec")
		}
		return nil
	}
	inner := typ
	if c.rd.active {
		if typ != recAppData {
			return alert(alertUnexpectedMessage, "unprotected record after keys were installed")
		}
		var err error
		if inner, body, err = c.rd.open(hdr, body); err != nil {
			return err
		}
	} else if typ == recAppData {
		return alert(alertUnexpectedMessage, "application data before handshake")
	}
	switch inner {
	case recHandshake:
		if len(body) == 0 {
			return alert(alertDecodeError, "empty handshake record")
		}
		c.hsBuf = append(c.hsBuf, body...)
		return c.drainHandshake()
	case recAppData:
		if c.st != stConnected {
			return alert(alertUnexpectedMessage, "application data before handshake finished")
		}
		c.plain = append(c.plain, body...)
		return nil
	case recAlert:
		if len(body) != 2 {
			return alert(alertDecodeError, "malformed alert")
		}
		if body[1] == alertCloseNotify {
			c.eof = true
			return nil
		}
		return &AlertError{Code: body[1], Msg: "fatal alert", Peer: true}
	}
	return alert(alertUnexpectedMessage, "unexpected record type")
}

func (c *Conn) drainHandshake() error {
	for len(c.hsBuf) >= 4 {
		n := int(c.hsBuf[1])<<16 | int(c.hsBuf[2])<<8 | int(c.hsBuf[3])
		if n > maxHandshakeMsg {
			return alert(alertIllegalParameter, "handshake message too large")
		}
		if len(c.hsBuf) < 4+n {
			return nil
		}
		msg := append([]byte(nil), c.hsBuf[:4+n]...)
		c.hsBuf = c.hsBuf[4+n:]
		if err := c.onHandshake(msg[0], msg); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) onHandshake(typ byte, msg []byte) error {
	switch {
	case c.st == stWaitClientHello && typ == hsClientHello:
		return c.onClientHello(msg)
	case c.st == stWaitClientFinished && typ == hsFinished:
		if len(msg)-4 != len(c.expectFin) || !hmac.Equal(msg[4:], c.expectFin) {
			return alert(alertDecryptError, "bad client Finished")
		}
		rd, err := newRecCipher(c.suite, c.cAP)
		if err != nil {
			return alert(alertInternalError, err.Error())
		}
		c.rd, c.st = rd, stConnected
		c.cHS, c.cAP, c.expectFin, c.transcript = nil, nil, nil, nil
		return nil
	case c.st == stConnected && typ == hsKeyUpdate:
		return c.onKeyUpdate(msg[4:])
	}
	return alert(alertUnexpectedMessage, "unexpected handshake message")
}

func (c *Conn) onKeyUpdate(body []byte) error {
	if len(body) != 1 || body[0] > 1 {
		return alert(alertIllegalParameter, "bad KeyUpdate")
	}
	h := c.suite.hash
	next := expandLabel(h, c.rd.secret, "traffic upd", nil, h().Size())
	rd, err := newRecCipher(c.suite, next)
	if err != nil {
		return alert(alertInternalError, err.Error())
	}
	c.rd = rd
	if body[0] == 1 { // peer asked us to update too: answer under the old write key
		c.out = c.wr.seal(c.out, recHandshake, []byte{hsKeyUpdate, 0, 0, 1, 0})
		wr, err := newRecCipher(c.suite, expandLabel(h, c.wr.secret, "traffic upd", nil, h().Size()))
		if err != nil {
			return alert(alertInternalError, err.Error())
		}
		c.wr = wr
	}
	return nil
}

// ---- ClientHello ----

type clientHello struct {
	sessionID []byte
	suites    []uint16
	versions  []uint16
	groups    []uint16
	sigAlgs   []uint16
	shares    []keyShare
	alpn      []string
	sni       string
	hasSigs   bool
}

type keyShare struct {
	group uint16
	data  []byte
}

type reader struct {
	b   []byte
	bad bool
}

func (r *reader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}
func (r *reader) u8() int {
	if v := r.take(1); v != nil {
		return int(v[0])
	}
	return 0
}
func (r *reader) u16() int {
	if v := r.take(2); v != nil {
		return int(v[0])<<8 | int(v[1])
	}
	return 0
}
func (r *reader) vec8() []byte  { return r.take(r.u8()) }
func (r *reader) vec16() []byte { return r.take(r.u16()) }

func u16list(b []byte) ([]uint16, bool) {
	if len(b)%2 != 0 {
		return nil, false
	}
	out := make([]uint16, 0, len(b)/2)
	for i := 0; i < len(b); i += 2 {
		out = append(out, uint16(b[i])<<8|uint16(b[i+1]))
	}
	return out, true
}

func parseClientHello(body []byte) (*clientHello, error) {
	bad := func() (*clientHello, error) { return nil, alert(alertDecodeError, "malformed ClientHello") }
	r := &reader{b: body}
	r.take(2 + 32) // legacy_version, random
	ch := &clientHello{sessionID: r.vec8()}
	if len(ch.sessionID) > 32 {
		return bad()
	}
	suitesRaw := r.vec16()
	comp := r.vec8()
	exts := r.vec16()
	if r.bad || len(r.b) != 0 || len(suitesRaw) < 2 {
		return bad()
	}
	var ok bool
	if ch.suites, ok = u16list(suitesRaw); !ok {
		return bad()
	}
	hasNull := false
	for _, m := range comp {
		hasNull = hasNull || m == 0
	}
	if !hasNull {
		return nil, alert(alertIllegalParameter, "ClientHello offers no null compression")
	}
	var seen [64]bool
	er := &reader{b: exts}
	for len(er.b) > 0 {
		typ := er.u16()
		data := er.vec16()
		if er.bad {
			return bad()
		}
		if typ < len(seen) {
			if seen[typ] {
				return nil, alert(alertIllegalParameter, "duplicate extension")
			}
			seen[typ] = true
		}
		dr := &reader{b: data}
		switch typ {
		case extSupportedVersions:
			vs := dr.vec8()
			if dr.bad || len(dr.b) != 0 {
				return bad()
			}
			for i := 0; i+1 < len(vs); i += 2 {
				ch.versions = append(ch.versions, uint16(vs[i])<<8|uint16(vs[i+1]))
			}
		case extSupportedGroups:
			g := dr.vec16()
			if ch.groups, ok = u16list(g); dr.bad || !ok {
				return bad()
			}
		case extSigAlgs:
			s := dr.vec16()
			ch.hasSigs = true
			if ch.sigAlgs, ok = u16list(s); dr.bad || !ok {
				return bad()
			}
		case extKeyShare:
			list := &reader{b: dr.vec16()}
			if dr.bad {
				return bad()
			}
			for len(list.b) > 0 {
				g := list.u16()
				kx := list.vec16()
				if list.bad {
					return bad()
				}
				ch.shares = append(ch.shares, keyShare{uint16(g), kx})
			}
		case extServerName:
			list := &reader{b: dr.vec16()}
			for len(list.b) > 0 && !list.bad {
				nt, name := list.u8(), list.vec16()
				if nt == 0 && !list.bad && ch.sni == "" {
					ch.sni = string(name)
				}
			}
		case extALPN:
			list := &reader{b: dr.vec16()}
			if dr.bad {
				return bad()
			}
			for len(list.b) > 0 {
				p := list.vec8()
				if list.bad || len(p) == 0 {
					return bad()
				}
				ch.alpn = append(ch.alpn, string(p))
			}
		}
	}
	return ch, nil
}

func (c *Conn) onClientHello(msg []byte) error {
	ch, err := parseClientHello(msg[4:])
	if err != nil {
		return err
	}
	offered13 := false
	for _, v := range ch.versions {
		offered13 = offered13 || v == 0x0304
	}
	if !offered13 {
		return alert(alertProtocolVersion, "client does not offer TLS 1.3")
	}
	if len(ch.shares) == 0 && len(ch.groups) == 0 {
		return alert(alertMissingExtension, "no key_share / supported_groups")
	}
	if !ch.hasSigs {
		return alert(alertMissingExtension, "no signature_algorithms")
	}

	// cipher suite: server preference among what the client offered
	for _, id := range c.cfg.CipherSuites {
		for _, off := range ch.suites {
			if off == id {
				c.suite = findSuite(id)
				break
			}
		}
		if c.suite != nil {
			break
		}
	}
	if c.suite == nil {
		return alert(alertHandshakeFailure, "no common cipher suite")
	}

	// key exchange: server preference among the shares the client sent (no HelloRetryRequest)
	var curve ecdh.Curve
	var share keyShare
	for _, g := range []uint16{groupX25519, groupP256} {
		for _, s := range ch.shares {
			if s.group == g {
				share = s
				curve = ecdh.X25519()
				if g == groupP256 {
					curve = ecdh.P256()
				}
			}
		}
		if curve != nil {
			break
		}
	}
	if curve == nil {
		return alert(alertHandshakeFailure, "no supported key_share (X25519 or P-256 required; HelloRetryRequest is not implemented)")
	}
	peer, err := curve.NewPublicKey(share.data)
	if err != nil {
		return alert(alertIllegalParameter, "invalid key_share")
	}
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return alert(alertInternalError, err.Error())
	}
	shared, err := priv.ECDH(peer)
	if err != nil {
		return alert(alertIllegalParameter, "key exchange failed")
	}

	// certificate and signature scheme
	pc, err := c.cfg.choose(ch)
	if err != nil {
		return alert(alertInternalError, err.Error())
	}
	var scheme uint16
pickSig:
	for _, mine := range pc.sigAlgs {
		for _, theirs := range ch.sigAlgs {
			if mine == theirs {
				scheme = mine
				break pickSig
			}
		}
	}
	if scheme == 0 {
		return alert(alertHandshakeFailure, "no common signature algorithm for the server certificate")
	}

	// ALPN
	if len(ch.alpn) > 0 {
	pickALPN:
		for _, mine := range c.cfg.NextProtos {
			for _, theirs := range ch.alpn {
				if mine == theirs {
					c.alpn = mine
					break pickALPN
				}
			}
		}
		if c.alpn == "" {
			return alert(alertNoApplicationProtocol, "no common application protocol")
		}
	}
	c.sni = ch.sni

	h := c.suite.hash
	c.transcript = h()
	c.transcript.Write(msg)

	// ServerHello (+ compat CCS), the only plaintext handshake record we send
	var random [32]byte
	rand.Read(random[:])
	sh := newBuilder(hsServerHello)
	sh.u16(0x0303)
	sh.bytes(random[:])
	sh.u8(len(ch.sessionID))
	sh.bytes(ch.sessionID)
	sh.u16(int(c.suite.id))
	sh.u8(0)
	x := sh.start16()
	sh.u16(extSupportedVersions)
	sh.u16(2)
	sh.u16(0x0304)
	sh.u16(extKeyShare)
	pub := priv.PublicKey().Bytes()
	sh.u16(4 + len(pub))
	sh.u16(int(share.group))
	sh.u16(len(pub))
	sh.bytes(pub)
	sh.end16(x)
	shMsg := sh.finish()
	c.transcript.Write(shMsg)
	c.out = append(c.out, recHandshake, 3, 3, byte(len(shMsg)>>8), byte(len(shMsg)))
	c.out = append(c.out, shMsg...)
	c.out = append(c.out, recChangeCipherSpec, 3, 3, 0, 1, 1)

	// handshake keys
	size := h().Size()
	zeros := make([]byte, size)
	empty := h().Sum(nil)
	early := hkdfExtract(h, nil, zeros)
	hsSecret := hkdfExtract(h, deriveSecret(h, early, "derived", empty), shared)
	th := c.transcript.Sum(nil)
	c.cHS = deriveSecret(h, hsSecret, "c hs traffic", th)
	sHS := deriveSecret(h, hsSecret, "s hs traffic", th)
	if c.wr, err = newRecCipher(c.suite, sHS); err != nil {
		return alert(alertInternalError, err.Error())
	}
	if c.rd, err = newRecCipher(c.suite, c.cHS); err != nil {
		return alert(alertInternalError, err.Error())
	}

	// encrypted flight: EncryptedExtensions, Certificate, CertificateVerify, Finished
	var flight []byte
	add := func(m []byte) { c.transcript.Write(m); flight = append(flight, m...) }

	ee := newBuilder(hsEncryptedExtensions)
	x = ee.start16()
	if c.alpn != "" {
		ee.u16(extALPN)
		ee.u16(2 + 1 + len(c.alpn))
		ee.u16(1 + len(c.alpn))
		ee.u8(len(c.alpn))
		ee.bytes([]byte(c.alpn))
	}
	ee.end16(x)
	add(ee.finish())

	cert := newBuilder(hsCertificate)
	cert.u8(0) // empty certificate_request_context
	x3 := cert.start24()
	for _, der := range pc.cert.Certificate {
		cert.u24(len(der))
		cert.bytes(der)
		cert.u16(0) // no per-certificate extensions
	}
	cert.end24(x3)
	add(cert.finish())

	content := make([]byte, 0, 64+34+size)
	for i := 0; i < 64; i++ {
		content = append(content, ' ')
	}
	content = append(content, "TLS 1.3, server CertificateVerify"...)
	content = append(content, 0)
	content = append(content, c.transcript.Sum(nil)...)
	sig, err := sign(pc.signer, scheme, content)
	if err != nil {
		return alert(alertInternalError, "signing failed: "+err.Error())
	}
	cv := newBuilder(hsCertificateVerify)
	cv.u16(int(scheme))
	cv.u16(len(sig))
	cv.bytes(sig)
	add(cv.finish())

	fin := newBuilder(hsFinished)
	fin.bytes(finishedMAC(h, sHS, c.transcript.Sum(nil)))
	add(fin.finish())
	for p := flight; len(p) > 0; {
		n := min(len(p), maxPlaintext)
		c.out = c.wr.seal(c.out, recHandshake, p[:n])
		p = p[n:]
	}

	// application keys (transcript up to and including server Finished)
	thFin := c.transcript.Sum(nil)
	master := hkdfExtract(h, deriveSecret(h, hsSecret, "derived", empty), zeros)
	c.cAP = deriveSecret(h, master, "c ap traffic", thFin)
	sAP := deriveSecret(h, master, "s ap traffic", thFin)
	if c.wr, err = newRecCipher(c.suite, sAP); err != nil {
		return alert(alertInternalError, err.Error())
	}
	c.expectFin = finishedMAC(h, c.cHS, thFin)
	c.st = stWaitClientFinished
	return nil
}

func finishedMAC(h func() hash.Hash, base, transcript []byte) []byte {
	key := expandLabel(h, base, "finished", nil, h().Size())
	m := hmac.New(h, key)
	m.Write(transcript)
	return m.Sum(nil)
}

// builder assembles a handshake message with length prefixes patched at the end.
type builder struct{ b []byte }

func newBuilder(typ byte) *builder { return &builder{b: []byte{typ, 0, 0, 0}} }

func (b *builder) u8(v int)       { b.b = append(b.b, byte(v)) }
func (b *builder) u16(v int)      { b.b = append(b.b, byte(v>>8), byte(v)) }
func (b *builder) u24(v int)      { b.b = append(b.b, byte(v>>16), byte(v>>8), byte(v)) }
func (b *builder) bytes(p []byte) { b.b = append(b.b, p...) }
func (b *builder) start16() int   { b.b = append(b.b, 0, 0); return len(b.b) }
func (b *builder) start24() int   { b.b = append(b.b, 0, 0, 0); return len(b.b) }
func (b *builder) end16(at int) {
	n := len(b.b) - at
	b.b[at-2], b.b[at-1] = byte(n>>8), byte(n)
}
func (b *builder) end24(at int) {
	n := len(b.b) - at
	b.b[at-3], b.b[at-2], b.b[at-1] = byte(n>>16), byte(n>>8), byte(n)
}
func (b *builder) finish() []byte {
	n := len(b.b) - 4
	b.b[1], b.b[2], b.b[3] = byte(n>>16), byte(n>>8), byte(n)
	return b.b
}
