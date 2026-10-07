package webpush

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func mustB64(t testing.TB, s string) []byte {
	t.Helper()
	b, err := decodeB64(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestEncryptRFC8291Vector is the worked example of RFC 8291 Appendix A: a fixed
// sender key and salt must give exactly the published body.
func TestEncryptRFC8291Vector(t *testing.T) {
	asPriv := mustB64(t, "yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw")
	sub := Subscription{
		Endpoint: "https://push.example.net/push/JzLQ3raZJfFBR0aqvOMsLrt54w4rJUsV",
		P256dh:   mustB64(t, "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"),
		Auth:     mustB64(t, "BTBZMqHH6r4Tts7J_aSIgg"),
	}
	key, err := ecdh.P256().NewPrivateKey(asPriv)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := encodeB64(key.PublicKey().Bytes()), "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8"; got != want {
		t.Fatalf("as_public = %s, want %s", got, want)
	}
	body, err := encrypt(sub, []byte("When I grow up, I want to be a watermelon"), 0, rand.Reader, key, mustB64(t, "DGv6ra1nlYgDCS1FRnbzlw"))
	if err != nil {
		t.Fatal(err)
	}
	want := "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPTpK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN"
	if got := encodeB64(body); got != want {
		t.Fatalf("body mismatch\n got %s\nwant %s", got, want)
	}
}

// decrypt is the receiver's side of RFC 8291 (what the browser does), written
// from the RFC independently of encrypt.
func decrypt(uaPriv *ecdh.PrivateKey, auth, body []byte) ([]byte, error) {
	if len(body) < 16+4+1 {
		return nil, errors.New("short")
	}
	salt, rs, idlen := body[:16], binary.BigEndian.Uint32(body[16:20]), int(body[20])
	if len(body) < 21+idlen || rs < 18 || uint32(len(body)-21-idlen) > rs {
		return nil, errors.New("bad header")
	}
	asPubB := body[21 : 21+idlen]
	rec := body[21+idlen:]
	asPub, err := ecdh.P256().NewPublicKey(asPubB)
	if err != nil {
		return nil, err
	}
	secret, err := uaPriv.ECDH(asPub)
	if err != nil {
		return nil, err
	}
	info := append(append([]byte("WebPush: info\x00"), uaPriv.PublicKey().Bytes()...), asPubB...)
	prk1, _ := hkdf.Extract(sha256.New, secret, auth)
	ikm, _ := hkdf.Expand(sha256.New, prk1, string(info), 32)
	prk, _ := hkdf.Extract(sha256.New, ikm, salt)
	cek, _ := hkdf.Expand(sha256.New, prk, "Content-Encoding: aes128gcm\x00", 16)
	nonce, _ := hkdf.Expand(sha256.New, prk, "Content-Encoding: nonce\x00", 12)
	blk, _ := aes.NewCipher(cek)
	gcm, _ := cipher.NewGCM(blk)
	plain, err := gcm.Open(nil, nonce, rec, nil)
	if err != nil {
		return nil, err
	}
	i := len(plain) - 1
	for i >= 0 && plain[i] == 0 {
		i--
	}
	if i < 0 || plain[i] != 2 {
		return nil, errors.New("missing last-record delimiter")
	}
	return plain[:i], nil
}

func newUA(t testing.TB) (*ecdh.PrivateKey, Subscription) {
	t.Helper()
	k, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	auth := make([]byte, 16)
	rand.Read(auth)
	return k, Subscription{Endpoint: "https://push.example.net/x", P256dh: k.PublicKey().Bytes(), Auth: auth}
}

func TestEncryptRoundTrip(t *testing.T) {
	ua, sub := newUA(t)
	for _, tc := range []struct{ n, pad int }{{0, 0}, {1, 0}, {100, 0}, {100, 50}, {MaxPayload, 0}, {10, MaxPayload - 10}} {
		msg := make([]byte, tc.n)
		rand.Read(msg)
		body, err := Encrypt(sub, msg, tc.pad)
		if err != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		if len(body) > recordSize {
			t.Fatalf("%+v: body is %d bytes, over the %d a push service must accept", tc, len(body), recordSize)
		}
		got, err := decrypt(ua, sub.Auth, body)
		if err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("%+v: decrypt = %q, %v", tc, got, err)
		}
	}
}

func TestEncryptFreshKeysEachTime(t *testing.T) {
	_, sub := newUA(t)
	a, _ := Encrypt(sub, []byte("same"), 0)
	b, _ := Encrypt(sub, []byte("same"), 0)
	if bytes.Equal(a, b) || bytes.Equal(a[:16], b[:16]) || bytes.Equal(a[21:86], b[21:86]) {
		t.Fatal("salt or ephemeral key repeated")
	}
}

func TestEncryptRejects(t *testing.T) {
	ua, sub := newUA(t)
	if _, err := Encrypt(sub, make([]byte, MaxPayload+1), 0); err == nil {
		t.Error("oversized payload accepted")
	}
	if _, err := Encrypt(sub, make([]byte, 10), MaxPayload); err == nil {
		t.Error("padding beyond the limit accepted")
	}
	if _, err := Encrypt(sub, nil, -1); err == nil {
		t.Error("negative padding accepted")
	}
	bad := sub
	bad.Auth = bad.Auth[:15]
	if _, err := Encrypt(bad, []byte("x"), 0); err == nil {
		t.Error("short auth accepted")
	}
	bad = sub
	bad.P256dh = append([]byte{4}, make([]byte, 64)...) // not on the curve
	if _, err := Encrypt(bad, []byte("x"), 0); err == nil {
		t.Error("off-curve key accepted")
	}
	// A tampered body must fail authentication at the receiver.
	body, _ := Encrypt(sub, []byte("hello"), 0)
	body[len(body)-1] ^= 1
	if _, err := decrypt(ua, sub.Auth, body); err == nil {
		t.Error("tampered body decrypted")
	}
}

func TestParseSubscription(t *testing.T) {
	_, sub := newUA(t)
	j := `{"endpoint":"` + sub.Endpoint + `","expirationTime":null,"keys":{"p256dh":"` + encodeB64(sub.P256dh) + `","auth":"` + encodeB64(sub.Auth) + `=="}}`
	got, err := ParseSubscription([]byte(j))
	if err != nil {
		t.Fatal(err)
	}
	if got.Endpoint != sub.Endpoint || !bytes.Equal(got.P256dh, sub.P256dh) || !bytes.Equal(got.Auth, sub.Auth) {
		t.Fatalf("got %+v", got)
	}
	for name, bad := range map[string]string{
		"not json":     `{`,
		"no endpoint":  `{"keys":{"p256dh":"` + encodeB64(sub.P256dh) + `","auth":"` + encodeB64(sub.Auth) + `"}}`,
		"ftp endpoint": `{"endpoint":"ftp://x/y","keys":{"p256dh":"` + encodeB64(sub.P256dh) + `","auth":"` + encodeB64(sub.Auth) + `"}}`,
		"userinfo":     `{"endpoint":"https://u:p@x/y","keys":{"p256dh":"` + encodeB64(sub.P256dh) + `","auth":"` + encodeB64(sub.Auth) + `"}}`,
		"no keys":      `{"endpoint":"https://x/y"}`,
		"bad base64":   `{"endpoint":"https://x/y","keys":{"p256dh":"!!","auth":"!!"}}`,
	} {
		if _, err := ParseSubscription([]byte(bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
