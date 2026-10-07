//go:build linux

package websocket_test

import (
	"crypto/sha1"
	"encoding/base64"
)

// wsAccept is the Sec-WebSocket-Accept value for a key, computed independently
// of the package under test.
func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}
