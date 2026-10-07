package websocket

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"
)

// Opcode is a WebSocket frame type (RFC 6455 §5.2).
type Opcode byte

const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// Close status codes (RFC 6455 §7.4.1). 1005, 1006 and 1015 never appear on the
// wire: Conn reports 1005 when a close frame carried no code and 1006 when the
// connection ended without one.
const (
	CloseNormal          uint16 = 1000
	CloseGoingAway       uint16 = 1001
	CloseProtocolError   uint16 = 1002
	CloseUnsupportedData uint16 = 1003
	CloseNoStatus        uint16 = 1005
	CloseAbnormal        uint16 = 1006
	CloseInvalidPayload  uint16 = 1007
	ClosePolicyViolation uint16 = 1008
	CloseTooBig          uint16 = 1009
	CloseMandatoryExt    uint16 = 1010
	CloseInternalError   uint16 = 1011
)

var (
	ErrClosed     = errors.New("websocket: connection is closing or closed")
	ErrQueueFull  = errors.New("websocket: too much unsent output (Config.MaxQueued)")
	ErrBadOpcode  = errors.New("websocket: not a data opcode")
	ErrTooLong    = errors.New("websocket: control payload longer than 125 bytes")
	ErrBadMessage = errors.New("websocket: text message is not valid UTF-8")
)

// validCloseCode reports whether a status code may appear in a close frame.
func validCloseCode(c uint16) bool {
	switch {
	case c >= 1000 && c <= 1003, c >= 1007 && c <= 1014:
		return true
	case c >= 3000 && c <= 4999:
		return true
	}
	return false
}

// appendHeader appends the header of an unmasked (server to client) frame.
func appendHeader(dst []byte, op Opcode, n int) []byte {
	b0 := byte(op) | 0x80 // FIN: this implementation never fragments what it sends
	switch {
	case n < 126:
		return append(dst, b0, byte(n))
	case n <= 0xffff:
		return append(dst, b0, 126, byte(n>>8), byte(n))
	}
	dst = append(dst, b0, 127)
	return binary.BigEndian.AppendUint64(dst, uint64(n))
}

// unmask XORs b with the 4-byte masking key, where b starts pos bytes into the
// frame's payload. It works eight bytes at a time.
func unmask(b []byte, key [4]byte, pos int) {
	if len(b) == 0 {
		return
	}
	var k [4]byte
	for i := range k {
		k[i] = key[(pos+i)&3] // rotate so k[0] lines up with b[0]
	}
	k64 := uint64(binary.LittleEndian.Uint32(k[:]))
	k64 |= k64 << 32
	i := 0
	for ; i+8 <= len(b); i += 8 {
		binary.LittleEndian.PutUint64(b[i:], binary.LittleEndian.Uint64(b[i:])^k64)
	}
	for ; i < len(b); i++ {
		b[i] ^= k[i&3]
	}
}

// validPrefix returns how many leading bytes of b form complete, valid UTF-8,
// and whether those are valid at all. A rune cut off at the end of b is not
// counted (and not an error) unless final is set.
func validPrefix(b []byte, final bool) (n int, ok bool) {
	end := len(b)
	if !final {
		for i := 1; i <= 3 && i <= len(b); i++ {
			ch := b[len(b)-i]
			if ch < 0x80 {
				break
			}
			if ch >= 0xC0 { // a lead byte: complete only if all its continuation bytes are here
				if !utf8.FullRune(b[len(b)-i:]) {
					end = len(b) - i
				}
				break
			}
		}
	}
	if !utf8.Valid(b[:end]) {
		return 0, false
	}
	return end, true
}
