package http

import (
	"strings"
	"testing"

	"gina/internal/prng"
)

func parse(raw string) (parseState, int, int, int, *Request) {
	req := &Request{}
	lim := &limits{maxHeaderBytes: 1024, maxURI: 128, maxBody: 64}
	st, consumed, need, status := parseRequest([]byte(raw), 0, lim, req, make([]Header, 0, 8))
	return st, consumed, need, status, req
}

func TestParseValid(t *testing.T) {
	st, consumed, _, _, r := parse("GET /a/b?x=1&y=2 HTTP/1.1\r\nHost: h\r\nUser-Agent:  t \r\n\r\n")
	if st != psOK || consumed == 0 {
		t.Fatalf("st=%v", st)
	}
	if r.Method != "GET" || string(r.Path) != "/a/b" || string(r.Query) != "x=1&y=2" || !r.KeepAlive || r.Minor != 1 {
		t.Fatalf("bad request: %+v", r)
	}
	if string(r.Header("user-agent")) != "t" || string(r.Header("HOST")) != "h" {
		t.Fatalf("headers: %q %q", r.Header("user-agent"), r.Header("HOST"))
	}
}

func TestParseBodyAndPipelining(t *testing.T) {
	raw := "POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhelloGET /next HTTP/1.1\r\nHost: h\r\n\r\n"
	st, consumed, _, _, r := parse(raw)
	if st != psOK || string(r.Body) != "hello" || !strings.HasPrefix(raw[consumed:], "GET /next") {
		t.Fatalf("st=%v body=%q consumed=%d", st, r.Body, consumed)
	}
	// body not fully arrived: ask for exactly the missing size
	st, _, need, _, _ := parse("POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhe")
	if st != psMore || need != len("POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\n")+5 {
		t.Fatalf("st=%v need=%d", st, need)
	}
	if st, _, need, _, _ := parse("GET / HTTP/1.1\r\nHo"); st != psMore || need != 0 {
		t.Fatalf("incomplete headers: st=%v need=%d", st, need)
	}
}

func TestParseKeepAliveRules(t *testing.T) {
	for _, c := range []struct {
		raw  string
		keep bool
	}{
		{"GET / HTTP/1.1\r\nHost: h\r\n\r\n", true},
		{"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n", false},
		{"GET / HTTP/1.1\r\nHost: h\r\nConnection: Keep-Alive, Close\r\n\r\n", false},
		{"GET / HTTP/1.0\r\n\r\n", false},
		{"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n", true},
	} {
		st, _, _, status, r := parse(c.raw)
		if st != psOK || r.KeepAlive != c.keep {
			t.Errorf("%q: st=%v status=%d keep=%v want %v", c.raw, st, status, r.KeepAlive, c.keep)
		}
	}
}

func TestParseRejects(t *testing.T) {
	long := strings.Repeat("a", 200)
	for _, c := range []struct {
		name, raw string
		want      int
	}{
		{"no version", "GET /\r\nHost: h\r\n\r\n", 400},
		{"bad method token", "G@T / HTTP/1.1\r\nHost: h\r\n\r\n", 400},
		{"unknown method", "BREW / HTTP/1.1\r\nHost: h\r\n\r\n", 501},
		{"http/2", "GET / HTTP/2.0\r\nHost: h\r\n\r\n", 505},
		{"garbage version", "GET / FTP/1.1\r\nHost: h\r\n\r\n", 400},
		{"missing host", "GET / HTTP/1.1\r\n\r\n", 400},
		{"duplicate host", "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", 400},
		{"bare LF", "GET / HTTP/1.1\nHost: h\n\n\r\n\r\n", 400},
		{"bare CR", "GET / HTTP/1.1\r\nHost: h\rX: y\r\n\r\n", 400},
		{"NUL", "GET / HTTP/1.1\r\nHost: h\x00\r\n\r\n", 400},
		{"obs-fold", "GET / HTTP/1.1\r\nHost: h\r\n  folded\r\n\r\n", 400},
		{"space before colon", "GET / HTTP/1.1\r\nHost : h\r\n\r\n", 400},
		{"control char in value", "GET / HTTP/1.1\r\nHost: h\x01\r\n\r\n", 400},
		{"TE + CL", "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n", 400},
		{"chunked unsupported", "POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n", 501},
		{"conflicting CL", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\n", 400},
		{"CL list", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 3, 3\r\n\r\n", 400},
		{"CL sign", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: -1\r\n\r\n", 400},
		{"CL too big", "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 65\r\n\r\n", 413},
		{"uri too long", "GET /" + long + " HTTP/1.1\r\nHost: h\r\n\r\n", 414},
		{"relative target", "GET foo HTTP/1.1\r\nHost: h\r\n\r\n", 400},
		{"header section too big", "GET / HTTP/1.1\r\nHost: h\r\nX: " + strings.Repeat("v", 1100) + "\r\n\r\n", 431},
	} {
		st, _, _, status, _ := parse(c.raw)
		if st != psErr || status != c.want {
			t.Errorf("%s: st=%v status=%d, want error %d", c.name, st, status, c.want)
		}
	}
}

func TestParseTooManyHeaders(t *testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: h\r\n" + strings.Repeat("X: y\r\n", 20) + "\r\n"
	if st, _, _, status, _ := parse(raw); st != psErr || status != 431 {
		t.Fatalf("st=%v status=%d", st, status)
	}
}

func TestParseUnterminatedHugeHeadersAnswer431(t *testing.T) {
	if st, _, _, status, _ := parse("GET / HTTP/1.1\r\nHost: h\r\nX: " + strings.Repeat("v", 1100)); st != psErr || status != 431 {
		t.Fatalf("st=%v status=%d", st, status)
	}
}

func TestParseOptionsStar(t *testing.T) {
	if st, _, _, _, _ := parse("OPTIONS * HTTP/1.1\r\nHost: h\r\n\r\n"); st != psOK {
		t.Fatal("OPTIONS * rejected")
	}
	if st, _, _, _, _ := parse("GET * HTTP/1.1\r\nHost: h\r\n\r\n"); st != psErr {
		t.Fatal("GET * accepted")
	}
}

func TestParseIsAllocationFree(t *testing.T) {
	raw := []byte("GET /a/b?x=1 HTTP/1.1\r\nHost: h\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n")
	req := &Request{}
	hdrs := make([]Header, 0, 16)
	lim := &limits{maxHeaderBytes: 4096, maxURI: 1024, maxBody: 1024}
	if n := testing.AllocsPerRun(1000, func() { parseRequest(raw, 0, lim, req, hdrs) }); n != 0 {
		t.Fatalf("parseRequest allocated %v times", n)
	}
}

func FuzzParseRequest(f *testing.F) {
	for _, s := range []string{
		"GET / HTTP/1.1\r\nHost: h\r\n\r\n",
		"POST /x?y=1 HTTP/1.1\r\nHost: h\r\nContent-Length: 3\r\n\r\nabc",
		"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n",
	} {
		f.Add([]byte(s))
	}
	lim := &limits{maxHeaderBytes: 2048, maxURI: 512, maxBody: 256}
	hdrs := make([]Header, 0, 16)
	f.Fuzz(func(t *testing.T, b []byte) {
		var req Request
		st, consumed, need, status := parseRequest(b, 0, lim, &req, hdrs)
		switch st {
		case psOK:
			if consumed <= 0 || consumed > len(b) || len(req.Body) != req.ContentLength || req.ContentLength > lim.maxBody {
				t.Fatalf("inconsistent OK: consumed=%d len=%d body=%d", consumed, len(b), len(req.Body))
			}
		case psMore:
			if need != 0 && (need <= len(b) || need > lim.maxHeaderBytes+lim.maxBody) {
				t.Fatalf("bad need %d for len %d", need, len(b))
			}
		case psErr:
			if status < 400 || status > 505 {
				t.Fatalf("bad status %d", status)
			}
		}
	})
}

// TestParseRandomMutations is a deterministic stand-in for fuzzing: it mutates
// valid requests with the repo's seeded PRNG and checks the parser's invariants,
// so the same inputs run on every `go test`.
func TestParseRandomMutations(t *testing.T) {
	seeds := []string{
		"GET /a/b?x=1 HTTP/1.1\r\nHost: h\r\nAccept: */*\r\n\r\n",
		"POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\nhello world",
		"GET / HTTP/1.0\r\nConnection: close\r\n\r\n",
	}
	r := prng.New(20261006)
	lim := &limits{maxHeaderBytes: 512, maxURI: 128, maxBody: 64}
	hdrs := make([]Header, 0, 16)
	ok, more, bad := 0, 0, 0
	for i := 0; i < 300_000; i++ {
		b := []byte(seeds[r.Intn(len(seeds))])
		for m := r.Intn(6); m >= 0; m-- {
			switch r.Intn(5) {
			case 0: // flip a byte
				b[r.Intn(len(b))] = byte(r.Uint64())
			case 1: // truncate
				b = b[:r.Intn(len(b)+1)]
			case 2: // insert a random byte
				p := r.Intn(len(b) + 1)
				b = append(b[:p], append([]byte{byte(r.Uint64())}, b[p:]...)...)
			case 3: // duplicate a slice
				p, q := r.Intn(len(b)+1), r.Intn(len(b)+1)
				if p > q {
					p, q = q, p
				}
				b = append(b[:q], append(append([]byte{}, b[p:q]...), b[q:]...)...)
			case 4: // splice in an interesting token
				toks := []string{"\r\n", "\n", "\r", ": ", "\x00", "Transfer-Encoding: chunked\r\n", "Content-Length: 99999999999999999999\r\n", " ", "%", "HTTP/1.1"}
				p := r.Intn(len(b) + 1)
				b = append(b[:p], append([]byte(toks[r.Intn(len(toks))]), b[p:]...)...)
			}
			if len(b) == 0 {
				b = []byte("x")
			}
		}
		var req Request
		st, consumed, need, status := parseRequest(b, 0, lim, &req, hdrs)
		switch st {
		case psOK:
			ok++
			if consumed <= 0 || consumed > len(b) || len(req.Body) != req.ContentLength || req.ContentLength > lim.maxBody ||
				len(req.Path) == 0 || req.Path[0] != '/' && req.Method != "OPTIONS" {
				t.Fatalf("inconsistent OK for %q: consumed=%d body=%d path=%q", b, consumed, len(req.Body), req.Path)
			}
		case psMore:
			more++
			if need != 0 && (need <= len(b) || need > lim.maxHeaderBytes+lim.maxBody) {
				t.Fatalf("bad need %d for len %d: %q", need, len(b), b)
			}
		case psErr:
			bad++
			if status < 400 || status > 505 {
				t.Fatalf("bad status %d for %q", status, b)
			}
		}
	}
	t.Logf("300k mutations: %d accepted, %d need more, %d rejected", ok, more, bad)
	if ok == 0 || bad == 0 {
		t.Fatal("mutations did not exercise both outcomes")
	}
}
