// Package http is an HTTP/1.1 server framework built on Gina isolates: one
// listener isolate per shard (SO_REUSEPORT capable) and one isolate per
// connection. It starts no goroutines of its own and does not use net/http: its
// state is kept per shard, so it runs unchanged on one thread or on many shard
// threads.
//
// Handlers run synchronously inside the connection isolate's turn and may use
// Context.Gina() to message other isolates. Request bodies must carry
// Content-Length (chunked request bodies are rejected with 501).
package http

import "bytes"

// Header is one request header; both slices alias the connection's read buffer
// and are only valid during the handler call.
type Header struct{ Name, Value []byte }

// Request is a parsed request. All byte slices alias the connection's read
// buffer and are only valid during the handler call; copy what you keep.
type Request struct {
	Method        string
	Target        []byte // raw request target
	Path          []byte // target without the query string
	Query         []byte // raw query string without '?'
	Minor         int    // HTTP/1.<Minor>
	Headers       []Header
	Body          []byte
	ContentLength int
	KeepAlive     bool
	// Protocol is the :protocol pseudo-header of an HTTP/2 extended CONNECT
	// (RFC 8441), e.g. "websocket". Such a request is presented with Method "GET"
	// so that one route serves both HTTP versions. Nil on HTTP/1.1 and on
	// ordinary HTTP/2 requests.
	Protocol []byte
}

// Header returns the first value of a header (case-insensitive) or nil.
func (r *Request) Header(name string) []byte {
	for i := range r.Headers {
		if equalFold(r.Headers[i].Name, name) {
			return r.Headers[i].Value
		}
	}
	return nil
}

func equalFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c, d := b[i], s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if 'A' <= d && d <= 'Z' {
			d += 'a' - 'A'
		}
		if c != d {
			return false
		}
	}
	return true
}

type parseState uint8

const (
	psMore parseState = iota // need more bytes
	psOK
	psErr // status holds the HTTP error code to answer with
)

type limits struct{ maxHeaderBytes, maxURI, maxBody int }

var crlf2 = []byte("\r\n\r\n")

var tokenChar = func() (t [256]bool) {
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
		t[c-'a'+'A'] = true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		t[c] = true
	}
	return
}()

func isToken(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if !tokenChar[c] {
			return false
		}
	}
	return true
}

func parseMethod(b []byte) (string, bool) {
	switch string(b) {
	case "GET":
		return "GET", true
	case "HEAD":
		return "HEAD", true
	case "POST":
		return "POST", true
	case "PUT":
		return "PUT", true
	case "DELETE":
		return "DELETE", true
	case "PATCH":
		return "PATCH", true
	case "OPTIONS":
		return "OPTIONS", true
	}
	return "", false
}

func trimOWS(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}

func parseContentLength(v []byte) (int, bool) {
	if len(v) == 0 || len(v) > 15 {
		return 0, false
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// connectionTokens reports whether a Connection header value lists close / keep-alive.
func connectionTokens(v []byte) (closeTok, keepTok bool) {
	for len(v) > 0 {
		var tok []byte
		if i := bytes.IndexByte(v, ','); i >= 0 {
			tok, v = v[:i], v[i+1:]
		} else {
			tok, v = v, nil
		}
		tok = trimOWS(tok)
		if equalFold(tok, "close") {
			closeTok = true
		} else if equalFold(tok, "keep-alive") {
			keepTok = true
		}
	}
	return
}

// parseRequest parses one request from the front of buf. `from` is how many
// bytes earlier calls already searched for the header terminator (so a slow
// client cannot make the scan quadratic). hdrs provides the header storage.
//
// On psMore, need > 0 is the total byte count required (headers + body) and 0
// means the headers are still incomplete. On psErr, status is the response code.
func parseRequest(buf []byte, from int, lim *limits, req *Request, hdrs []Header) (st parseState, consumed, need, status int) {
	start := from - 3
	if start < 0 {
		start = 0
	}
	i := bytes.Index(buf[start:], crlf2)
	if i < 0 {
		if len(buf) >= lim.maxHeaderBytes {
			return psErr, 0, 0, 431
		}
		return psMore, 0, 0, 0
	}
	end := start + i
	headEnd := end + 4
	if headEnd > lim.maxHeaderBytes {
		return psErr, 0, 0, 431
	}
	head := buf[:end]
	for j, b := range head { // only CRLF line endings, no NUL
		switch b {
		case 0:
			return psErr, 0, 0, 400
		case '\n':
			if j == 0 || head[j-1] != '\r' {
				return psErr, 0, 0, 400
			}
		case '\r':
			if j+1 >= len(head) || head[j+1] != '\n' {
				return psErr, 0, 0, 400
			}
		}
	}

	lineEnd := bytes.IndexByte(head, '\r')
	if lineEnd < 0 {
		lineEnd = len(head)
	}
	line := head[:lineEnd]
	sp1 := bytes.IndexByte(line, ' ')
	if sp1 <= 0 {
		return psErr, 0, 0, 400
	}
	method, ok := parseMethod(line[:sp1])
	if !ok {
		if isToken(line[:sp1]) {
			return psErr, 0, 0, 501
		}
		return psErr, 0, 0, 400
	}
	rest := line[sp1+1:]
	sp2 := bytes.IndexByte(rest, ' ')
	if sp2 <= 0 {
		return psErr, 0, 0, 400
	}
	target, ver := rest[:sp2], rest[sp2+1:]
	if len(target) > lim.maxURI {
		return psErr, 0, 0, 414
	}
	for _, b := range target {
		if b <= 0x20 || b == 0x7f {
			return psErr, 0, 0, 400
		}
	}
	if target[0] != '/' && !(method == "OPTIONS" && len(target) == 1 && target[0] == '*') {
		return psErr, 0, 0, 400
	}
	minor := 1
	switch string(ver) {
	case "HTTP/1.1":
	case "HTTP/1.0":
		minor = 0
	default:
		if bytes.HasPrefix(ver, []byte("HTTP/")) {
			return psErr, 0, 0, 505
		}
		return psErr, 0, 0, 400
	}

	hdrs = hdrs[:0]
	pos := lineEnd + 2
	var (
		contentLength = -1
		hasTE         bool
		hosts         int
		closeTok      bool
		keepTok       bool
	)
	for pos < len(head) {
		le := bytes.IndexByte(head[pos:], '\r')
		var l []byte
		if le < 0 {
			l, pos = head[pos:], len(head)
		} else {
			l, pos = head[pos:pos+le], pos+le+2
		}
		if len(l) == 0 || l[0] == ' ' || l[0] == '\t' { // obsolete line folding
			return psErr, 0, 0, 400
		}
		colon := bytes.IndexByte(l, ':')
		if colon <= 0 || !isToken(l[:colon]) { // also rejects "Name : value"
			return psErr, 0, 0, 400
		}
		name, value := l[:colon], trimOWS(l[colon+1:])
		for _, c := range value {
			if (c < 0x20 && c != '\t') || c == 0x7f {
				return psErr, 0, 0, 400
			}
		}
		if len(hdrs) == cap(hdrs) {
			return psErr, 0, 0, 431
		}
		hdrs = append(hdrs, Header{name, value})
		switch {
		case equalFold(name, "content-length"):
			n, ok := parseContentLength(value)
			if !ok || (contentLength >= 0 && contentLength != n) {
				return psErr, 0, 0, 400
			}
			contentLength = n
		case equalFold(name, "transfer-encoding"):
			hasTE = true
		case equalFold(name, "host"):
			hosts++
		case equalFold(name, "connection"):
			c, k := connectionTokens(value)
			closeTok = closeTok || c
			keepTok = keepTok || k
		}
	}
	if hasTE {
		if contentLength >= 0 {
			return psErr, 0, 0, 400 // request smuggling shape
		}
		return psErr, 0, 0, 501
	}
	if hosts > 1 || (minor == 1 && hosts == 0) {
		return psErr, 0, 0, 400
	}
	if contentLength < 0 {
		contentLength = 0
	}
	if contentLength > lim.maxBody {
		return psErr, 0, 0, 413
	}
	if total := headEnd + contentLength; len(buf) < total {
		return psMore, 0, total, 0
	}
	*req = Request{
		Method: method, Target: target, Minor: minor, Headers: hdrs, ContentLength: contentLength,
		Body:      buf[headEnd : headEnd+contentLength],
		KeepAlive: !closeTok && (minor == 1 || keepTok),
	}
	if q := bytes.IndexByte(target, '?'); q >= 0 {
		req.Path, req.Query = target[:q], target[q+1:]
	} else {
		req.Path = target
	}
	return psOK, headEnd + contentLength, 0, 0
}
