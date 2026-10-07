package http

import "bytes"

// Chunked request bodies (RFC 9112 §7.1) are decoded in place and incrementally:
// each call consumes what has arrived, moves the decoded data down over the
// framing, and remembers where it stopped, so a body costs time linear in its
// size however the client splits it (one byte at a time included).

type chunkState struct {
	phase   uint8 // ckSize, ckData, ckDataEnd, ckTrailer
	remain  int   // bytes of the current chunk still to come
	body    int   // decoded bytes so far; they sit right after the request head
	trailer int   // trailer bytes seen
}

const (
	ckSize uint8 = iota
	ckData
	ckDataEnd
	ckTrailer
)

const maxChunkLine = 1024 // chunk-size line, extensions included

// decode consumes raw, the bytes that follow the decoded prefix. It writes the
// decoded data at the front of raw and moves whatever it could not use yet (a
// partial chunk line, pipelined bytes after the body) down behind it, so the
// caller's buffer is raw[:w] + raw[i:] afterwards. i is how much of raw was
// consumed and w how much was written. done is true once the last chunk and the
// trailers are in. status is non-zero on a protocol error.
func (ck *chunkState) decode(raw []byte, max, maxTrailer int) (i, w int, done bool, status int) {
loop:
	for {
		switch ck.phase {
		case ckSize:
			j := bytes.Index(raw[i:], crlf)
			if j < 0 {
				if len(raw)-i > maxChunkLine {
					return 0, 0, false, 400
				}
				break loop
			}
			size, st := parseChunkSize(raw[i : i+j])
			if st != 0 {
				return 0, 0, false, st
			}
			if size > max-ck.body {
				return 0, 0, false, 413
			}
			i += j + 2
			if size == 0 {
				ck.phase = ckTrailer
			} else {
				ck.remain, ck.phase = size, ckData
			}
		case ckData:
			n := min(ck.remain, len(raw)-i)
			copy(raw[w:], raw[i:i+n]) // w <= i: the framing seen so far has opened a gap
			w, i, ck.body, ck.remain = w+n, i+n, ck.body+n, ck.remain-n
			if ck.remain > 0 {
				break loop
			}
			ck.phase = ckDataEnd
		case ckDataEnd:
			if len(raw)-i < 2 {
				break loop
			}
			if raw[i] != '\r' || raw[i+1] != '\n' {
				return 0, 0, false, 400
			}
			i += 2
			ck.phase = ckSize
		case ckTrailer:
			j := bytes.Index(raw[i:], crlf)
			if j < 0 {
				if ck.trailer+len(raw)-i > maxTrailer {
					return 0, 0, false, 431
				}
				break loop
			}
			if j == 0 {
				i += 2
				done = true
				break loop
			}
			line := raw[i : i+j]
			if ck.trailer += j + 2; ck.trailer > maxTrailer {
				return 0, 0, false, 431
			}
			if colon := bytes.IndexByte(line, ':'); colon <= 0 || !isToken(line[:colon]) {
				return 0, 0, false, 400
			}
			for _, c := range line {
				if c == 0 || c == '\r' || c == '\n' {
					return 0, 0, false, 400
				}
			}
			i += j + 2
		}
	}
	copy(raw[w:], raw[i:]) // compact: the undecoded rest follows the decoded data
	return i, w, done, 0
}

var crlf = []byte("\r\n")

// parseChunkSize reads "HEXDIGITS[;extension]". Trailing garbage, whitespace
// before the ';', an empty size and sizes of more than 8 hex digits are refused.
func parseChunkSize(line []byte) (size, status int) {
	if semi := bytes.IndexByte(line, ';'); semi >= 0 {
		for _, c := range line[semi:] {
			if c == 0 || c == '\r' || c == '\n' {
				return 0, 400
			}
		}
		line = line[:semi]
	}
	if len(line) == 0 {
		return 0, 400
	}
	if len(line) > 8 {
		for _, c := range line {
			if hexVal(c) < 0 {
				return 0, 400
			}
		}
		return 0, 413
	}
	for _, c := range line {
		v := hexVal(c)
		if v < 0 {
			return 0, 400
		}
		size = size<<4 | v
	}
	return size, 0
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}
