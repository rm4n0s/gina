package webpush

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

// MaxPayload is the longest message that can be sent (RFC 8291 §4): a push
// service must accept a 4096-byte body, and the encrypted form adds an 86-byte
// header, a 16-byte tag and a 1-byte delimiter.
const MaxPayload = 3993

const recordSize = 4096

// Encrypt encrypts payload for the subscription with the aes128gcm content
// coding of RFC 8291 and RFC 8188, and returns the request body to POST to the
// subscription's endpoint (with "Content-Encoding: aes128gcm"). Every call uses a
// fresh ephemeral key and salt. pad adds that many zero bytes of padding inside
// the encrypted record, to hide the message's length from the push service; the
// total is still limited to MaxPayload.
func Encrypt(sub Subscription, payload []byte, pad int) ([]byte, error) {
	return encrypt(sub, payload, pad, rand.Reader, nil, nil)
}

// encrypt is Encrypt with its randomness injectable: asKey is the ephemeral
// ("application server") key and salt the 16-byte salt; nil means draw them from
// rnd. The RFC's test vector fixes both.
func encrypt(sub Subscription, payload []byte, pad int, rnd io.Reader, asKey *ecdh.PrivateKey, salt []byte) ([]byte, error) {
	if pad < 0 || len(payload)+pad > MaxPayload {
		return nil, errors.New("webpush: payload too large (limit 3993 bytes including padding)")
	}
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	curve := ecdh.P256()
	uaPub, err := curve.NewPublicKey(sub.P256dh)
	if err != nil {
		return nil, err
	}
	if asKey == nil {
		if asKey, err = curve.GenerateKey(rnd); err != nil {
			return nil, err
		}
	}
	if salt == nil {
		salt = make([]byte, 16)
		if _, err := io.ReadFull(rnd, salt); err != nil {
			return nil, err
		}
	}
	asPub := asKey.PublicKey().Bytes()

	secret, err := asKey.ECDH(uaPub)
	if err != nil {
		return nil, err
	}
	// RFC 8291 §3.4: mix the shared secret with the subscription's auth secret and
	// both public keys, then RFC 8188 §2.2-2.3: derive the content key and nonce.
	prkKey, err := hkdf.Extract(sha256.New, secret, sub.Auth)
	if err != nil {
		return nil, err
	}
	keyInfo := make([]byte, 0, len("WebPush: info\x00")+65+65)
	keyInfo = append(keyInfo, "WebPush: info\x00"...)
	keyInfo = append(keyInfo, sub.P256dh...)
	keyInfo = append(keyInfo, asPub...)
	ikm, err := hkdf.Expand(sha256.New, prkKey, string(keyInfo), 32)
	if err != nil {
		return nil, err
	}
	prk, err := hkdf.Extract(sha256.New, ikm, salt)
	if err != nil {
		return nil, err
	}
	cek, err := hkdf.Expand(sha256.New, prk, "Content-Encoding: aes128gcm\x00", 16)
	if err != nil {
		return nil, err
	}
	nonce, err := hkdf.Expand(sha256.New, prk, "Content-Encoding: nonce\x00", 12)
	if err != nil {
		return nil, err
	}
	blk, err := aes.NewCipher(cek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, err
	}

	// One record: payload, the 0x02 delimiter of the last record, zero padding.
	plain := make([]byte, len(payload)+1+pad)
	copy(plain, payload)
	plain[len(payload)] = 2

	// Header (RFC 8188 §2.1): salt, record size, key id length, key id (our public key).
	out := make([]byte, 0, 16+4+1+65+len(plain)+gcm.Overhead())
	out = append(out, salt...)
	out = binary.BigEndian.AppendUint32(out, recordSize)
	out = append(out, byte(len(asPub)))
	out = append(out, asPub...)
	return gcm.Seal(out, nonce, plain, nil), nil
}
