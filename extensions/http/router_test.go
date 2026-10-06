package http

import "testing"

func serveOne(r *Router, method, path string) *Context {
	c := &Context{}
	c.reset(nil, &Request{Method: method, Path: []byte(path)})
	r.serve(c)
	return c
}

func TestRouterMatching(t *testing.T) {
	r := NewRouter()
	hit := ""
	mk := func(name string) Handler {
		return func(c *Context) { hit = name; c.String(200, name) }
	}
	r.GET("/", mk("root"))
	r.GET("/users/:id", mk("user"))
	r.GET("/users/:id/posts/:pid", mk("post"))
	r.POST("/users", mk("create"))
	r.GET("/static/*", mk("static"))

	for _, c := range []struct {
		method, path, want string
		status             int
	}{
		{"GET", "/", "root", 200},
		{"GET", "/users/42", "user", 200},
		{"GET", "/users/42/posts/7", "post", 200},
		{"POST", "/users", "create", 200},
		{"GET", "/static/css/app.css", "static", 200},
		{"GET", "/static/", "static", 200},
		{"HEAD", "/users/42", "user", 200},
		{"GET", "/nope", "", 404},
		{"GET", "/users/", "", 404},
		{"GET", "/users//posts/1", "", 404},
		{"GET", "/users/42/", "", 404},
		{"DELETE", "/users/42", "", 405},
	} {
		hit = ""
		got := serveOne(r, c.method, c.path)
		if got.status != c.status || hit != c.want {
			t.Errorf("%s %s: status=%d hit=%q, want %d %q", c.method, c.path, got.status, hit, c.status, c.want)
		}
	}
}

func TestRouterParamsAndAllow(t *testing.T) {
	r := NewRouter()
	var id, pid, rest string
	r.GET("/users/:id/posts/:pid", func(c *Context) { id, pid = c.Param("id"), c.Param("pid") })
	r.GET("/files/*", func(c *Context) { rest = c.Param("*") })
	r.POST("/users/:id/posts/:pid", func(c *Context) {})
	serveOne(r, "GET", "/users/42/posts/7")
	serveOne(r, "GET", "/files/a/b.txt")
	if id != "42" || pid != "7" || rest != "a/b.txt" {
		t.Fatalf("params: %q %q %q", id, pid, rest)
	}
	c := serveOne(r, "PUT", "/users/1/posts/2")
	if c.status != 405 || string(c.hdr) != "Allow: GET, POST\r\n" {
		t.Fatalf("405: status=%d hdr=%q", c.status, c.hdr)
	}
}

func TestQueryDecoding(t *testing.T) {
	c := &Context{}
	c.reset(nil, &Request{Query: []byte("a=1&name=J%C3%BCrgen+Z&flag&empty=")})
	if c.Query("a") != "1" || c.Query("name") != "Jürgen Z" || c.Query("missing") != "" || c.Query("empty") != "" {
		t.Fatalf("query: %q %q", c.Query("a"), c.Query("name"))
	}
}

func TestHeaderInjectionPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("CRLF in a header value was accepted")
		}
	}()
	c := &Context{}
	c.reset(nil, &Request{})
	c.SetHeader("X-Evil", "a\r\nSet-Cookie: pwned")
}
