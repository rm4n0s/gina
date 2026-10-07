package http

import (
	"io"

	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
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
			c.dropSource()
			c.hdr = c.hdr[:0]
			c.body = append(c.body[:0], "internal server error\n"...)
		}
	}()
	r.serve(c)
	if c.onBody != nil && !c.streaming { // a buffered request: the whole body is already here
		c.DeliverBody(c.Req.Body, true)
	}
	return false
}

// DispatchStream runs the handler of a route made with StreamBody as soon as the
// request headers are in. The body is not delivered by this call.
func (r *Router) DispatchStream(c *Context) (panicked bool) {
	c.streaming = true
	return r.Dispatch(c)
}

// HasBodyHandler reports whether the handler asked for the body (OnBody) and has
// not already answered with StopBody.
func (c *Context) HasBodyHandler() bool { return c.onBody != nil && !c.stopped }

// Stopped reports whether the exchange was ended early (StopBody, or a panic).
func (c *Context) Stopped() bool { return c.stopped }

// DeliverBody hands the next piece of the request body to the handler's OnBody
// callback (last = true for the final piece, which may be empty) and reports
// whether the exchange is over early (StopBody or a panic in the callback).
func (c *Context) DeliverBody(chunk []byte, last bool) (stop bool) {
	if c.onBody == nil || c.stopped || c.bodyEnd {
		return c.stopped
	}
	c.bodyEnd = last
	defer func() {
		if p := recover(); p != nil {
			c.status, c.ctype, c.close, c.stopped = 500, "text/plain; charset=utf-8", true, true
			c.dropSource()
			c.hdr = c.hdr[:0]
			c.body = append(c.body[:0], "internal server error\n"...)
			stop = true
		}
	}()
	c.onBody(c, chunk, last)
	return c.stopped
}

// AbortBody tells the handler's OnBody callback that the body will not arrive
// (see Context.BodyAborted), if it is still waiting for it.
func (c *Context) AbortBody() {
	if c.onBody != nil && !c.bodyEnd && !c.stopped {
		c.aborted = true
		c.DeliverBody(nil, true)
	}
}

// Begin prepares c for a new request, discarding the previous response. conn is
// the connection's TLS state, or nil on a plain connection; fd is its socket
// (for Context.RemoteAddr).
func (c *Context) Begin(g *gina.Ctx, req *Request, conn *gtls.Conn, fd gina.FDHandle) {
	c.reset(g, req)
	c.tls, c.fd = conn, fd
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

	// Source, when set, is the body (Context.SendReader), SourceSize its length or
	// -1. The caller owns it from then on and must close it with CloseSource
	// whenever it stops reading, finished or not.
	Source     io.Reader
	SourceSize int64
}

// Result returns the response built so far.
func (c *Context) Result() Result {
	r := Result{Status: c.status, ContentType: c.ctype, Headers: c.hdr, Body: c.body, Stream: c.stream, Tunnel: c.tunnel}
	r.Source, r.SourceSize = c.src, c.srcSize
	c.src = nil // handed over
	return r
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
