package http

import (
	"strconv"

	"gina"
	gtls "gina/extensions/tls"
)

// Context is a request/response pair handed to a Handler. It is reused for every
// request on a connection, so do not keep it (or slices from Req) after returning.
type Context struct {
	Req *Request

	g       *gina.Ctx
	params  [maxParams]param
	nparams int
	status  int
	ctype   string
	hdr     []byte // extra response headers, "Name: value\r\n" each
	body    []byte
	close   bool
	stream  bool        // EventStream: keep the connection open for pushed events
	notify  gina.Handle // EventStream: told when the stream ends
	tls     *gtls.Conn
}

func (c *Context) reset(g *gina.Ctx, req *Request) {
	c.Req, c.g, c.nparams, c.tls = req, g, 0, nil
	c.status, c.ctype, c.close = 200, "", false
	c.stream, c.notify = false, 0
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
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 202:
		return "Accepted"
	case 204:
		return "No Content"
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
	case 422:
		return "Unprocessable Content"
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
