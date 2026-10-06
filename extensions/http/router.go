package http

import "bytes"

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
}

// Router matches method + path. Patterns are literal segments, ":name" path
// parameters and a trailing "*" wildcard, e.g. "/users/:id" or "/static/*".
// HEAD falls back to the GET route. A path that matches under another method
// answers 405 with an Allow header.
type Router struct {
	routes   []route
	NotFound Handler // default: 404 text
}

func NewRouter() *Router { return &Router{} }

func (r *Router) Handle(method, pattern string, h Handler) {
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
	r.routes = append(r.routes, route{method: method, segs: segs, h: h})
}

func (r *Router) GET(p string, h Handler)    { r.Handle("GET", p, h) }
func (r *Router) POST(p string, h Handler)   { r.Handle("POST", p, h) }
func (r *Router) PUT(p string, h Handler)    { r.Handle("PUT", p, h) }
func (r *Router) DELETE(p string, h Handler) { r.Handle("DELETE", p, h) }
func (r *Router) PATCH(p string, h Handler)  { r.Handle("PATCH", p, h) }

func matchPath(segs []segment, path []byte, c *Context) bool {
	c.nparams = 0
	if len(segs) == 0 {
		return len(path) == 1
	}
	p := 1
	for _, sg := range segs {
		if sg.wild {
			if c.nparams < maxParams {
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
			if c.nparams < maxParams {
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
		rt := &r.routes[i]
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
