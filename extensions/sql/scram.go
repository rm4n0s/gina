package sql

// SCRAM-SHA-256 (RFC 5802, RFC 7677) as PostgreSQL uses it for password
// authentication, without channel binding (SCRAM-SHA-256-PLUS is not offered).
// The user name is sent in the startup message, so the SCRAM user name is empty,
// as libpq does. The password is used as is; SASLprep is not applied, which only
// matters for passwords with non-ASCII characters that normalise differently.

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
)

const scramMech = "SCRAM-SHA-256"

type scramClient struct {
	password    string
	nonce       string
	firstBare   string
	serverFirst string
	serverSig   []byte
}

func newSCRAM(password string) (*scramClient, error) {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, err
	}
	return newSCRAMNonce(password, base64.StdEncoding.EncodeToString(raw[:])), nil
}

func newSCRAMNonce(password, nonce string) *scramClient {
	return &scramClient{password: password, nonce: nonce, firstBare: "n=,r=" + nonce}
}

// first is the client-first-message.
func (s *scramClient) first() []byte { return []byte("n,," + s.firstBare) }

var errSCRAM = errors.New("sql: SCRAM authentication failed")

// final answers the server-first-message with the client-final-message.
func (s *scramClient) final(serverFirst []byte) ([]byte, error) {
	s.serverFirst = string(serverFirst)
	var nonce, salt string
	iter := 0
	for _, part := range strings.Split(s.serverFirst, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok || len(k) != 1 {
			continue
		}
		switch k {
		case "r":
			nonce = v
		case "s":
			salt = v
		case "i":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, errSCRAM
			}
			iter = n
		}
	}
	if !strings.HasPrefix(nonce, s.nonce) || len(nonce) == len(s.nonce) || salt == "" || iter < 1 || iter > 10_000_000 {
		return nil, errSCRAM
	}
	saltBytes, err := base64.StdEncoding.DecodeString(salt)
	if err != nil {
		return nil, errSCRAM
	}
	salted, err := pbkdf2.Key(sha256.New, s.password, saltBytes, iter, sha256.Size)
	if err != nil {
		return nil, err
	}
	clientKey := hmacSHA256(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + nonce // biws = base64("n,,")
	authMsg := s.firstBare + "," + s.serverFirst + "," + withoutProof
	sig := hmacSHA256(stored[:], authMsg)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	s.serverSig = hmacSHA256(hmacSHA256(salted, "Server Key"), authMsg)
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

// verify checks the server-final-message: the server proves it knows the password
// too, so an impostor that merely says "authentication ok" is caught.
func (s *scramClient) verify(serverFinal []byte) error {
	v, ok := strings.CutPrefix(string(serverFinal), "v=")
	if !ok {
		return errSCRAM
	}
	got, err := base64.StdEncoding.DecodeString(v)
	if err != nil || subtle.ConstantTimeCompare(got, s.serverSig) != 1 {
		return errSCRAM
	}
	return nil
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// md5Password is the answer to AuthenticationMD5Password.
func md5Password(user, password string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	h := md5.New()
	h.Write([]byte(hex.EncodeToString(inner[:])))
	h.Write(salt)
	return "md5" + hex.EncodeToString(h.Sum(nil))
}
