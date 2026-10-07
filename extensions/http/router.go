package http

import (
	"bytes"
	"time"
)

// Handler serves one request. It runs synchronously inside the connection's turn.
type Handler func(c *Context)

const maxParams = 4

type param struct {
	name  string
	value []byte
}

type segment struct {
	lit   string
	param string // ":name"
	wild  bool   // trailing "*"
}

type route struct {
	method string
	segs   []segment
	h      Handler

	maxBody     int           // 0 = Config.MaxBodyBytes (no limit for a streaming route)
	readTimeout time.Duration // 0 = Config.ReadTimeout; negative disables
	stream      bool          // the body is delivered to Context.OnBody as it arrives
}

// Route is returned by Router.Handle and the method helpers so that a route can
// carry limits of its own.
type Route struct {
	rt *route
	r  *Router
}

// MaxBody sets the largest request body this route accepts (Content-Length or
// the total of a chunked body), overriding Config.MaxBodyBytes, up or down: a
// file upload can take 250 MB while everything else stays at the default. The
// body is still buffered in memory, one buffer of that size per connection that
// is uploading, so size MaxConns accordingly.
func (o *Route) MaxBody(n int) *Route { o.rt.maxBody = n; o.r.limited = true; return o }

// StreamBody makes the route receive its request body piece by piece instead of
// buffered: the handler runs as soon as the headers are in (Request.Body is
// empty, ContentLength says what the client announced, -1 if chunked) and calls
// Context.OnBody to take the pieces as they arrive. Memory per upload stays at
// one read buffer, however large the body. With StreamBody a route has no body
// limit unless MaxBody sets one, and ReadTimeout becomes an inactivity timeout (it
// restarts with every read) instead of a deadline for the whole request. A request
// without a body still reaches the handler, and its OnBody callback is called once
// with an empty last piece, so one handler serves both.
func (o *Route) StreamBody() *Route { o.rt.stream = true; o.r.limited = true; return o }

// ReadTimeout sets how long this route's request may take from its first byte
// to its last (default Config.ReadTimeout; negative disables).
func (o *Route) ReadTimeout(d time.Duration) *Route {
	o.rt.readTimeout = d
	o.r.limited = true
	return o
}

// Router matches method + path. Patterns are literal segments, ":name" path
// parameters and a trailing "*" wildcard, e.g. "/users/:id" or "/static/*".
// HEAD falls back to the GET route. A path that matches under another method
// answers 405 with an Allow header.
type Router struct {
	routes   []*route
	limited  bool    // some route has its own limits: the parser must look the route up early
	NotFound Handler // default: 404 text
}

func NewRouter() *Router { return &Router{} }

func (r *Router) Handle(method, pattern string, h Handler) *Route {
	if len(pattern) == 0 || pattern[0] != '/' {
		panic("http: pattern must start with '/': " + pattern)
	}
	var segs []segment
	start := 1
	for i := 1; i <= len(pattern); i++ {
		if i < len(pattern) && pattern[i] != '/' {
			continue
		}
		part := pattern[start:i]
		start = i + 1
		if part == "" && len(pattern) == 1 {
			break
		}
		switch {
		case part == "*":
			if i != len(pattern) {
				panic("http: '*' must be the last segment: " + pattern)
			}
			segs = append(segs, segment{wild: true, param: "*"})
		case len(part) > 1 && part[0] == ':':
			segs = append(segs, segment{param: part[1:]})
		default:
			segs = append(segs, segment{lit: part})
		}
	}
	rt := &route{method: method, segs: segs, h: h}
	r.routes = append(r.routes, rt)
	return &Route{rt, r}
}

func (r *Router) GET(p string, h Handler) *Route    { return r.Handle("GET", p, h) }
func (r *Router) POST(p string, h Handler) *Route   { return r.Handle("POST", p, h) }
func (r *Router) PUT(p string, h Handler) *Route    { return r.Handle("PUT", p, h) }
func (r *Router) DELETE(p string, h Handler) *Route { return r.Handle("DELETE", p, h) }
func (r *Router) PATCH(p string, h Handler) *Route  { return r.Handle("PATCH", p, h) }

// matchPath reports whether path matches segs, recording parameters in c (which
// may be nil when only the answer matters).
func matchPath(segs []segment, path []byte, c *Context) bool {
	if c != nil {
		c.nparams = 0
	}
	if len(segs) == 0 {
		return len(path) == 1
	}
	p := 1
	for _, sg := range segs {
		if sg.wild {
			if c != nil && c.nparams < maxParams {
				c.params[c.nparams] = param{"*", path[min(p, len(path)):]}
				c.nparams++
			}
			return true
		}
		if p > len(path) {
			return false
		}
		var seg []byte
		if j := bytes.IndexByte(path[p:], '/'); j < 0 {
			seg, p = path[p:], len(path)+1
		} else {
			seg, p = path[p:p+j], p+j+1
		}
		if sg.param != "" {
			if len(seg) == 0 {
				return false
			}
			if c != nil && c.nparams < maxParams {
				c.params[c.nparams] = param{sg.param, seg}
				c.nparams++
			}
		} else if string(seg) != sg.lit {
			return false
		}
	}
	return p > len(path)
}

func (r *Router) serve(c *Context) {
	method := c.Req.Method
	pathMatched := false
	for i := range r.routes {
		rt := r.routes[i]
		if !matchPath(rt.segs, c.Req.Path, c) {
			continue
		}
		pathMatched = true
		if rt.method == method || (method == "HEAD" && rt.method == "GET") {
			rt.h(c)
			return
		}
	}
	if pathMatched {
		allow := ""
		for i := range r.routes {
			if matchPath(r.routes[i].segs, c.Req.Path, c) {
				if allow != "" {
					allow += ", "
				}
				allow += r.routes[i].method
			}
		}
		c.SetHeader("Allow", allow)
		c.String(405, "method not allowed\n")
		return
	}
	if r.NotFound != nil {
		r.NotFound(c)
		return
	}
	c.String(404, "not found\n")
}

// find returns the route that would serve method and path, or nil.
func (r *Router) find(method string, path []byte) *route {
	for _, rt := range r.routes {
		if (rt.method == method || (method == "HEAD" && rt.method == "GET")) && matchPath(rt.segs, path, nil) {
			return rt
		}
	}
	return nil
}

// HasRouteLimits reports whether any route has limits of its own (see Route).
func (r *Router) HasRouteLimits() bool { return r.limited }

// BodyLimits reports the limits set on the route that would serve method and
// path (stream: Route.StreamBody): maxBody and readTimeout are 0 when the route has none of its own (use the
// server's defaults), ok is false when no route matches. HTTP/2 uses it to apply
// the same per-route limits as HTTP/1.1.
func (r *Router) BodyLimits(method string, path []byte) (maxBody int, readTimeout time.Duration, stream, ok bool) {
	if !r.limited {
		return 0, 0, false, true
	}
	rt := r.find(method, path)
	if rt == nil {
		return 0, 0, false, false
	}
	return rt.maxBody, rt.readTimeout, rt.stream, true
}

// StreamsBody reports whether the route that would serve method and path takes
// its body as a stream (Route.StreamBody).
func (r *Router) StreamsBody(method string, path []byte) bool {
	if !r.limited {
		return false
	}
	rt := r.find(method, path)
	return rt != nil && rt.stream
}
