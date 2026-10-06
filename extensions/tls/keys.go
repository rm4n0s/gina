package tls

import (
	"crypto/hmac"
	"hash"
)

// HKDF (RFC 5869) and the TLS 1.3 key schedule helpers (RFC 8446 §7.1), built
// only on crypto/hmac.

func hkdfExtract(h func() hash.Hash, salt, ikm []byte) []byte {
	if salt == nil {
		salt = make([]byte, h().Size())
	}
	m := hmac.New(h, salt)
	m.Write(ikm)
	return m.Sum(nil)
}

func hkdfExpand(h func() hash.Hash, prk, info []byte, n int) []byte {
	out := make([]byte, 0, n)
	var t []byte
	for i := byte(1); len(out) < n; i++ {
		m := hmac.New(h, prk)
		m.Write(t)
		m.Write(info)
		m.Write([]byte{i})
		t = m.Sum(nil)
		out = append(out, t...)
	}
	return out[:n]
}

// expandLabel is HKDF-Expand-Label: HkdfLabel = uint16 length, "tls13 "+label, context.
func expandLabel(h func() hash.Hash, secret []byte, label string, context []byte, n int) []byte {
	info := make([]byte, 0, 2+1+6+len(label)+1+len(context))
	info = append(info, byte(n>>8), byte(n), byte(6+len(label)))
	info = append(info, "tls13 "...)
	info = append(info, label...)
	info = append(info, byte(len(context)))
	info = append(info, context...)
	return hkdfExpand(h, secret, info, n)
}

func deriveSecret(h func() hash.Hash, secret []byte, label string, transcript []byte) []byte {
	return expandLabel(h, secret, label, transcript, h().Size())
}
