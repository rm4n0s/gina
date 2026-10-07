package http

import (
	"io"
	"net/netip"
	"strconv"

	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// Context is a request/response pair handed to a Handler. It is reused for every
// request on a connection, so do not keep it (or slices from Req) after returning.
type Context struct {
	Req *Request

	g         *gina.Ctx
	params    [maxParams]param
	nparams   int
	status    int
	ctype     string
	hdr       []byte // extra response headers, "Name: value\r\n" each
	body      []byte
	close     bool
	stream    bool        // EventStream: keep the connection open for pushed events
	notify    gina.Handle // EventStream: told when the stream ends
	tls       *gtls.Conn
	fd        gina.FDHandle
	tunnel    Tunnel    // SetTunnel: the protocol that takes the connection over
	src       io.Reader // SendReader: the body, read as it is sent
	srcSize   int64
	onBody    BodyFunc // OnBody: takes the request body as it arrives
	stopped   bool     // StopBody: answered early, the rest of the request is not wanted
	bodyEnd   bool     // the last piece has been delivered
	aborted   bool     // the body ended because the exchange did (AbortBody)
	streaming bool     // the server is delivering the body itself (DispatchStream)
}

func (c *Context) reset(g *gina.Ctx, req *Request) {
	c.Req, c.g, c.nparams, c.tls, c.tunnel = req, g, 0, nil, nil
	c.status, c.ctype, c.close = 200, "", false
	c.stream, c.notify = false, 0
	c.onBody, c.stopped, c.bodyEnd, c.streaming, c.aborted = nil, false, false, false, false
	c.dropSource() // a source the server never took (a handler that gave up) must not leak
	c.hdr, c.body = c.hdr[:0], c.body[:0]
}

// TLSInfo describes the TLS session a request arrived on.
type TLSInfo struct {
	ServerName  string // SNI the client asked for ("" if none)
	ALPN        string // negotiated application protocol ("http/1.1")
	CipherSuite uint16
}

// CipherName is the IANA name of the negotiated cipher suite.
func (i TLSInfo) CipherName() string {
	switch i.CipherSuite {
	case gtls.TLS_AES_128_GCM_SHA256:
		return "TLS_AES_128_GCM_SHA256"
	case gtls.TLS_AES_256_GCM_SHA384:
		return "TLS_AES_256_GCM_SHA384"
	}
	return "unknown"
}

// TLS returns the session details when the request came over HTTPS (always TLS 1.3).
func (c *Context) TLS() (TLSInfo, bool) {
	if c.tls == nil {
		return TLSInfo{}, false
	}
	return TLSInfo{ServerName: c.tls.ServerName(), ALPN: c.tls.ALPN(), CipherSuite: c.tls.CipherSuite()}, true
}

// RemoteAddr is the client's address (IPv4-mapped IPv6 is reported as IPv4). It
// is the TCP peer: behind a proxy that is the proxy, so read X-Forwarded-For
// yourself in that case. ok is false if the connection no longer exists.
func (c *Context) RemoteAddr() (netip.AddrPort, bool) { return c.g.PeerAddr(c.fd) }

// Gina exposes the isolate context, e.g. to message other isolates.
func (c *Context) Gina() *gina.Ctx { return c.g }

// Param returns a path parameter (":name" or "*"); "" if absent.
func (c *Context) Param(name string) string { return string(c.ParamBytes(name)) }

func (c *Context) ParamBytes(name string) []byte {
	for i := 0; i < c.nparams; i++ {
		if c.params[i].name == name {
			return c.params[i].value
		}
	}
	return nil
}

// Query returns the first value of a query parameter, percent-decoded.
func (c *Context) Query(name string) string {
	q := c.Req.Query
	for len(q) > 0 {
		var pair []byte
		if i := indexByte(q, '&'); i >= 0 {
			pair, q = q[:i], q[i+1:]
		} else {
			pair, q = q, nil
		}
		k, v := pair, []byte(nil)
		if i := indexByte(pair, '='); i >= 0 {
			k, v = pair[:i], pair[i+1:]
		}
		if string(unescape(k)) == name {
			return string(unescape(v))
		}
	}
	return ""
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

func unhex(c byte) int {
	switch {
	case '0' <= c && c <= '9':
		return int(c - '0')
	case 'a' <= c && c <= 'f':
		return int(c-'a') + 10
	case 'A' <= c && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

func unescape(b []byte) []byte {
	if indexByte(b, '%') < 0 && indexByte(b, '+') < 0 {
		return b
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		switch {
		case b[i] == '+':
			out = append(out, ' ')
		case b[i] == '%' && i+2 < len(b) && unhex(b[i+1]) >= 0 && unhex(b[i+2]) >= 0:
			out = append(out, byte(unhex(b[i+1])<<4|unhex(b[i+2])))
			i += 2
		default:
			out = append(out, b[i])
		}
	}
	return out
}

// Status sets the response status code (default 200).
func (c *Context) Status(code int) *Context { c.status = code; return c }

// SetHeader adds a response header. CR/LF in name or value panic (header
// injection); the connection answers 500 for that request.
func (c *Context) SetHeader(name, value string) {
	for i := 0; i < len(name); i++ {
		if name[i] == '\r' || name[i] == '\n' || name[i] == ':' {
			panic("http: invalid header name")
		}
	}
	for i := 0; i < len(value); i++ {
		if value[i] == '\r' || value[i] == '\n' {
			panic("http: invalid header value")
		}
	}
	c.hdr = append(c.hdr, name...)
	c.hdr = append(c.hdr, ": "...)
	c.hdr = append(c.hdr, value...)
	c.hdr = append(c.hdr, '\r', '\n')
}

// Write appends to the response body (io.Writer compatible).
func (c *Context) Write(p []byte) (int, error) { c.body = append(c.body, p...); return len(p), nil }

func (c *Context) WriteString(s string) { c.body = append(c.body, s...) }

// String replies with a text/plain body.
func (c *Context) String(code int, s string) {
	c.status, c.ctype = code, "text/plain; charset=utf-8"
	c.body = append(c.body[:0], s...)
}

// Bytes replies with an arbitrary body and content type.
func (c *Context) Bytes(code int, contentType string, b []byte) {
	c.status, c.ctype = code, contentType
	c.body = append(c.body[:0], b...)
}

// JSON replies with an already-encoded JSON body (the framework does not marshal).
func (c *Context) JSON(code int, raw string) {
	c.status, c.ctype = code, "application/json"
	c.body = append(c.body[:0], raw...)
}

func (c *Context) Redirect(code int, location string) {
	c.SetHeader("Location", location)
	c.status = code
	c.body = c.body[:0]
}

// Close asks the server to close the connection after this response.
func (c *Context) Close() { c.close = true }

func statusText(code int) string {
	switch code {
	case 100:
		return "Continue"
	case 101:
		return "Switching Protocols"
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 202:
		return "Accepted"
	case 204:
		return "No Content"
	case 206:
		return "Partial Content"
	case 301:
		return "Moved Permanently"
	case 302:
		return "Found"
	case 303:
		return "See Other"
	case 304:
		return "Not Modified"
	case 307:
		return "Temporary Redirect"
	case 308:
		return "Permanent Redirect"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 408:
		return "Request Timeout"
	case 409:
		return "Conflict"
	case 411:
		return "Length Required"
	case 413:
		return "Content Too Large"
	case 414:
		return "URI Too Long"
	case 415:
		return "Unsupported Media Type"
	case 416:
		return "Range Not Satisfiable"
	case 422:
		return "Unprocessable Content"
	case 426:
		return "Upgrade Required"
	case 429:
		return "Too Many Requests"
	case 431:
		return "Request Header Fields Too Large"
	case 500:
		return "Internal Server Error"
	case 501:
		return "Not Implemented"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	case 505:
		return "HTTP Version Not Supported"
	}
	return "Status " + strconv.Itoa(code)
}

func bodyAllowed(code int) bool { return code >= 200 && code != 204 && code != 304 }

// SendReader replies with a body that is read as it is sent, so a large response
// is never held in memory: only one buffer (32 KiB) per streaming response. size is
// the number of bytes r will deliver; the Content-Length of the reply is set
// from it, and a reader that delivers fewer ends the connection. A negative size
// means unknown: HTTP/1.1 sends the body chunked, HTTP/2 simply ends the stream
// at io.EOF. If r is an io.Closer it is closed when the response is finished, or
// abandoned (the peer went away, the connection or the server shut down).
//
// The server calls r.Read on the shard thread, between turns of other
// connections, so a Read must not block for long: files in the page cache and
// memory are fine, a slow network source is not (feed those from another
// isolate instead). The handler may not touch r after SendReader returns.
func (c *Context) SendReader(code int, contentType string, size int64, r io.Reader) {
	c.dropSource()
	c.status, c.ctype = code, contentType
	c.body = c.body[:0]
	if size == 0 { // nothing to read: an ordinary empty body
		CloseSource(r)
		return
	}
	c.src, c.srcSize = r, size
}

func (c *Context) dropSource() {
	if c.src != nil {
		CloseSource(c.src)
		c.src = nil
	}
}

// CloseSource closes r if it is an io.Closer (the servers use it for sources they
// abandon).
func CloseSource(r io.Reader) {
	if cl, ok := r.(io.Closer); ok {
		cl.Close()
	}
}

// BodyFunc receives a request body in pieces (see Route.StreamBody). chunk is
// valid only during the call; copy what you keep. last is true for the final call,
// whose chunk may be empty. After the final call the response is whatever the
// Context holds, as after an ordinary handler.
type BodyFunc func(c *Context, chunk []byte, last bool)

// OnBody registers fn to take the request body. On a route made with
// Route.StreamBody the server calls it for every piece as it arrives, on the
// shard thread: do not block in it (a write to a local file is fine, waiting for
// another isolate is not; hand the piece to one with ctx.SendRaw instead). On other
// routes the buffered body is delivered in one call once the handler returns.
func (c *Context) OnBody(fn BodyFunc) { c.onBody = fn }

// StopBody ends the exchange early with a plain-text answer: the rest of the body
// is not wanted (too large, wrong content type, quota exceeded). HTTP/1.1 closes
// the connection after the answer, since the unread body cannot be skipped
// cheaply; HTTP/2 resets just that stream. Call it from the handler or from an
// OnBody callback.
func (c *Context) StopBody(code int, msg string) {
	c.String(code, msg)
	c.stopped = true
	c.close = true
}

// BodyAborted reports, inside an OnBody callback, that the body will never be
// complete: the client went away, timed out or reset the stream. The call it is
// reported in is the last one (last is true, chunk empty), so a handler that
// holds a file or a buffer can release it. No response can be sent anymore.
func (c *Context) BodyAborted() bool { return c.aborted }
