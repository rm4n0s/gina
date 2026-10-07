package webpush

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/big"
	"net/url"
	"time"
)

// VAPID is the application server's identity (RFC 8292): a P-256 key pair. The
// public half goes to the browser as the applicationServerKey of
// pushManager.subscribe(); a subscription is bound to it, so keep the private
// half (PrivateKey) somewhere that survives restarts, or every subscriber has to
// subscribe again.
type VAPID struct {
	key *ecdsa.PrivateKey
	pub []byte // 65-byte uncompressed point
}

// GenerateVAPID makes a new key pair.
func GenerateVAPID() (*VAPID, error) {
	k, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return vapidFrom(k)
}

// ParseVAPID loads a private key from its base64url form, the 32-byte scalar that
// PrivateKey returns (and that most other Web Push tools print).
func ParseVAPID(private string) (*VAPID, error) {
	raw, err := decodeB64(private)
	if err != nil {
		return nil, errors.New("webpush: VAPID private key is not base64url")
	}
	k, err := ecdh.P256().NewPrivateKey(raw)
	if err != nil {
		return nil, errors.New("webpush: VAPID private key is not a P-256 scalar")
	}
	return vapidFrom(k)
}

func vapidFrom(k *ecdh.PrivateKey) (*VAPID, error) {
	pub := k.PublicKey().Bytes() // 0x04 || X || Y
	return &VAPID{
		key: &ecdsa.PrivateKey{
			PublicKey: ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(pub[1:33]), Y: new(big.Int).SetBytes(pub[33:65])},
			D:         new(big.Int).SetBytes(k.Bytes()),
		},
		pub: pub,
	}, nil
}

// PublicKey is the key to give the browser, base64url (applicationServerKey).
func (v *VAPID) PublicKey() string { return encodeB64(v.pub) }

// PrivateKey is the secret to store, base64url; ParseVAPID reads it back.
func (v *VAPID) PrivateKey() string {
	b := make([]byte, 32)
	return encodeB64(v.key.D.FillBytes(b))
}

// sign makes the ES256 JWT of RFC 8292 §2 for a push service. aud is the origin
// of the subscription's endpoint, sub a contact ("mailto:..." or "https://...").
func (v *VAPID) sign(aud, sub string, exp time.Time) (string, error) {
	claims, err := json.Marshal(struct {
		Aud string `json:"aud"`
		Exp int64  `json:"exp"`
		Sub string `json:"sub,omitempty"`
	}{aud, exp.Unix(), sub})
	if err != nil {
		return "", err
	}
	in := encodeB64([]byte(`{"typ":"JWT","alg":"ES256"}`)) + "." + encodeB64(claims)
	h := sha256.Sum256([]byte(in))
	r, s, err := ecdsa.Sign(rand.Reader, v.key, h[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64) // JWS wants R||S, 32 bytes each, not ASN.1
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return in + "." + encodeB64(sig), nil
}

// authorization returns the Authorization header value for a push to endpoint.
func (v *VAPID) authorization(endpoint, sub string, exp time.Time) (string, error) {
	aud, err := audience(endpoint)
	if err != nil {
		return "", err
	}
	jwt, err := v.sign(aud, sub, exp)
	if err != nil {
		return "", err
	}
	return "vapid t=" + jwt + ", k=" + v.PublicKey(), nil
}

// audience is the scheme and host of the endpoint: the JWT is valid for a push
// service, not for one subscription.
func audience(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", errors.New("webpush: bad endpoint")
	}
	return u.Scheme + "://" + u.Host, nil
}
