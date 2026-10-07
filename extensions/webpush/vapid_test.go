package webpush

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

// verifyJWT checks an ES256 JWT against a 65-byte public key and returns its claims.
func verifyJWT(t testing.TB, jwt string, pub []byte) (claims struct {
	Aud string `json:"aud"`
	Exp int64  `json:"exp"`
	Sub string `json:"sub"`
}) {
	t.Helper()
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatalf("jwt has %d parts", len(parts))
	}
	if h := string(mustB64(t, parts[0])); h != `{"typ":"JWT","alg":"ES256"}` {
		t.Fatalf("header %s", h)
	}
	sig := mustB64(t, parts[2])
	if len(sig) != 64 {
		t.Fatalf("signature is %d bytes, JWS wants 64", len(sig))
	}
	k, err := ecdh.P256().NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	b := k.Bytes()
	ek := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(b[1:33]), Y: new(big.Int).SetBytes(b[33:])}
	h := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(ek, h[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		t.Fatal("signature does not verify")
	}
	if err := json.Unmarshal(mustB64(t, parts[1]), &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

func TestVAPIDSignVerifies(t *testing.T) {
	v, err := GenerateVAPID()
	if err != nil {
		t.Fatal(err)
	}
	exp := time.Now().Add(12 * time.Hour)
	auth, err := v.authorization("https://fcm.googleapis.com/fcm/send/abc:def", "mailto:ops@example.com", exp)
	if err != nil {
		t.Fatal(err)
	}
	const pre = "vapid t="
	if !strings.HasPrefix(auth, pre) {
		t.Fatalf("authorization = %q", auth)
	}
	jwt, k, ok := strings.Cut(auth[len(pre):], ", k=")
	if !ok || k != v.PublicKey() {
		t.Fatalf("authorization = %q, want k=%s", auth, v.PublicKey())
	}
	c := verifyJWT(t, jwt, mustB64(t, k))
	if c.Aud != "https://fcm.googleapis.com" || c.Sub != "mailto:ops@example.com" || c.Exp != exp.Unix() {
		t.Fatalf("claims %+v", c)
	}
}

// The example of RFC 8292 (§2.4) is signed by a key we do not hold, but its public
// key is published with it, so we can check our verifier against a third party's JWT.
func TestVAPIDVerifiesRFCExample(t *testing.T) {
	const jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJhdWQiOiJodHRwczovL3B1c2guZXhhbXBsZS5uZXQiLCJleHAiOjE0NTM1MjM3NjgsInN1YiI6Im1haWx0bzpwdXNoQGV4YW1wbGUuY29tIn0.i3CYb7t4xfxCDquptFOepC9GAu_HLGkMlMuCGSK2rpiUfnK9ojFwDXb1JrErtmysazNjjvW2L9OkSSHzvoD1oA"
	c := verifyJWT(t, jwt, mustB64(t, "BA1Hxzyi1RUM1b5wjxsn7nGxAszw2u61m164i3MrAIxHF6YK5h4SDYic-dRuU_RCPCfA5aq9ojSwk5Y2EmClBPs"))
	if c.Aud != "https://push.example.net" || c.Sub != "mailto:push@example.com" || c.Exp != 1453523768 {
		t.Fatalf("claims %+v", c)
	}
}

func TestVAPIDKeyRoundTrip(t *testing.T) {
	v, _ := GenerateVAPID()
	w, err := ParseVAPID(v.PrivateKey())
	if err != nil {
		t.Fatal(err)
	}
	if w.PublicKey() != v.PublicKey() || w.PrivateKey() != v.PrivateKey() {
		t.Fatal("key changed through PrivateKey/ParseVAPID")
	}
	if len(mustB64(t, v.PublicKey())) != 65 || len(mustB64(t, v.PrivateKey())) != 32 {
		t.Fatal("unexpected key sizes")
	}
	for _, bad := range []string{"", "!!", "AAAA", encodeB64(make([]byte, 32))} { // empty, not base64, short, zero scalar
		if _, err := ParseVAPID(bad); err == nil {
			t.Errorf("ParseVAPID(%q) accepted", bad)
		}
	}
}
