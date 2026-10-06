package tls

import (
	"crypto/aes"
	"crypto/cipher"
	"hash"
	"slices"
)

const (
	recChangeCipherSpec = 20
	recAlert            = 21
	recHandshake        = 22
	recAppData          = 23

	maxPlaintext  = 1 << 14
	maxCiphertext = maxPlaintext + 256
)

// recCipher protects one direction of the record layer (RFC 8446 §5.2).
type recCipher struct {
	active bool
	aead   cipher.AEAD
	iv     [12]byte
	seq    uint64
	secret []byte
}

func newRecCipher(s *suite, secret []byte) (recCipher, error) {
	key := expandLabel(s.hash, secret, "key", nil, s.keyLen)
	iv := expandLabel(s.hash, secret, "iv", nil, 12)
	block, err := aes.NewCipher(key)
	if err != nil {
		return recCipher{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return recCipher{}, err
	}
	rc := recCipher{active: true, aead: aead, secret: secret}
	copy(rc.iv[:], iv)
	return rc, nil
}

func (rc *recCipher) nonce() [12]byte {
	n := rc.iv
	for i := 0; i < 8; i++ {
		n[11-i] ^= byte(rc.seq >> (8 * i))
	}
	return n
}

// seal appends one protected record carrying pt with the given inner content type.
func (rc *recCipher) seal(out []byte, inner byte, pt []byte) []byte {
	n := len(pt) + 1 + rc.aead.Overhead()
	hdr := [5]byte{recAppData, 3, 3, byte(n >> 8), byte(n)}
	out = slices.Grow(out, 5+n)
	out = append(out, hdr[:]...)
	start := len(out)
	out = append(out, pt...)
	out = append(out, inner)
	nonce := rc.nonce()
	out = rc.aead.Seal(out[:start], nonce[:], out[start:], hdr[:])
	rc.seq++
	return out
}

// open decrypts a record body in place and returns the inner type and content.
func (rc *recCipher) open(hdr, body []byte) (byte, []byte, error) {
	nonce := rc.nonce()
	pt, err := rc.aead.Open(body[:0], nonce[:], body, hdr)
	if err != nil {
		return 0, nil, alert(alertBadRecordMAC, "bad record MAC")
	}
	rc.seq++
	i := len(pt) - 1
	for i >= 0 && pt[i] == 0 { // strip zero padding
		i--
	}
	if i < 0 {
		return 0, nil, alert(alertUnexpectedMessage, "record has no content type")
	}
	if i > maxPlaintext {
		return 0, nil, alert(alertRecordOverflow, "record too large")
	}
	return pt[i], pt[:i], nil
}

// suite is a TLS 1.3 AEAD cipher suite (AES-GCM only).
type suite struct {
	id     uint16
	keyLen int
	hash   func() hash.Hash
}
