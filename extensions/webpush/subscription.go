package webpush

import (
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
)

// Subscription is what a browser returns from PushManager.subscribe(): the
// address of its push service and the keys a sender needs to encrypt for it.
type Subscription struct {
	Endpoint string // URL of the push service; POST the message here
	P256dh   []byte // the browser's P-256 public key, 65 bytes (uncompressed point)
	Auth     []byte // the browser's 16-byte authentication secret
}

// ParseSubscription reads the JSON a page gets from subscription.toJSON() (or
// JSON.stringify(subscription)):
//
//	{"endpoint": "https://...", "keys": {"p256dh": "...", "auth": "..."}}
//
// Keys are base64url, with or without padding. It checks that the keys are
// usable (the public key must be a point on P-256) but does not look at the
// endpoint's host: see Config.AllowPrivate for how the sender treats it.
func ParseSubscription(data []byte) (Subscription, error) {
	var j struct {
		Endpoint string `json:"endpoint"`
		Keys     struct {
			P256dh string `json:"p256dh"`
			Auth   string `json:"auth"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &j); err != nil {
		return Subscription{}, errors.New("webpush: subscription is not valid JSON: " + err.Error())
	}
	p, err := decodeB64(j.Keys.P256dh)
	if err != nil {
		return Subscription{}, errors.New("webpush: keys.p256dh is not base64url")
	}
	a, err := decodeB64(j.Keys.Auth)
	if err != nil {
		return Subscription{}, errors.New("webpush: keys.auth is not base64url")
	}
	s := Subscription{Endpoint: j.Endpoint, P256dh: p, Auth: a}
	return s, s.Validate()
}

// Validate reports whether the subscription can be encrypted for.
func (s Subscription) Validate() error {
	u, err := url.Parse(s.Endpoint)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("webpush: subscription endpoint is not an http(s) URL")
	}
	if len(s.Auth) != 16 {
		return errors.New("webpush: auth secret must be 16 bytes")
	}
	if len(s.P256dh) != 65 {
		return errors.New("webpush: p256dh must be a 65-byte uncompressed P-256 point")
	}
	if _, err := ecdh.P256().NewPublicKey(s.P256dh); err != nil {
		return errors.New("webpush: p256dh is not a point on P-256")
	}
	return nil
}

// decodeB64 accepts base64url with or without padding (browsers omit it, some
// libraries add it).
func decodeB64(s string) ([]byte, error) {
	for len(s) > 0 && s[len(s)-1] == '=' {
		s = s[:len(s)-1]
	}
	return base64.RawURLEncoding.DecodeString(s)
}

func encodeB64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
