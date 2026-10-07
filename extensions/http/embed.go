package http

import (
	"gina"
	gtls "gina/extensions/tls"
)

// Hooks for protocol front ends built on this package's Router and Context:
// extensions/http2 parses HTTP/2 frames into a Request, runs the very same
// handlers, and reads the response back out. Applications do not need these.

// Dispatch runs the matching handler for c.Req. A panicking handler becomes a
// 500 response and Dispatch reports it.
func (r *Router) Dispatch(c *Context) (panicked bool) {
	defer func() {
		if p := recover(); p != nil {
			panicked = true
			c.status, c.ctype, c.close = 500, "text/plain; charset=utf-8", true
			c.hdr = c.hdr[:0]
			c.body = append(c.body[:0], "internal server error\n"...)
		}
	}()
	r.serve(c)
	return false
}

// Begin prepares c for a new request, discarding the previous response. conn is
// the connection's TLS state, or nil on a plain connection.
func (c *Context) Begin(g *gina.Ctx, req *Request, conn *gtls.Conn) {
	c.reset(g, req)
	c.tls = conn
}

// Result is the response a handler built. Its slices alias the Context and are
// valid until the next Begin.
type Result struct {
	Status      int
	ContentType string // "" when the handler set none
	Headers     []byte // extra headers, "Name: value\r\n" each
	Body        []byte
	Stream      bool   // the handler asked for an event stream (Context.EventStream)
	Tunnel      Tunnel // the handler asked to hand the connection or stream over (Context.SetTunnel)
}

// Result returns the response built so far.
func (c *Context) Result() Result {
	return Result{Status: c.status, ContentType: c.ctype, Headers: c.hdr, Body: c.body, Stream: c.stream, Tunnel: c.tunnel}
}

// EachHeader calls fn for every extra header in r.Headers, in order.
func (r Result) EachHeader(fn func(name, value []byte)) {
	h := r.Headers
	for len(h) > 0 {
		end := indexByte(h, '\r')
		if end < 0 {
			return
		}
		line := h[:end]
		if colon := indexByte(line, ':'); colon > 0 && colon+2 <= len(line) {
			fn(line[:colon], line[colon+2:])
		}
		h = h[end+2:]
	}
}

// BodyAllowed reports whether a response with this status may carry a body.
func BodyAllowed(status int) bool { return bodyAllowed(status) }

// StatusText is the reason phrase of an HTTP status code.
func StatusText(code int) string { return statusText(code) }
