package sql

import (
	"bufio"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakePG is a PostgreSQL server for tests that need to do what a real one will
// not: ask for MD5 or cleartext passwords, hang up in the middle of a result, send
// garbage, ignore a cancel request, speak TLS to a client we choose. It answers
// statements from a script.

type fakeResult struct {
	cols              []column
	rows              [][][]byte // nil value = NULL
	tag               string
	err               *Error
	delay             time.Duration // before answering
	silent            bool          // never answer (the statement "runs forever")
	hangupAt          int           // close the socket after this many rows (-1: never)
	badRow            bool          // send a DataRow with the wrong number of columns
	copyOut           bool          // answer with a CopyOutResponse
	copyIn            bool
	hangupBeforeReply bool
}

func rowsOf(cols []string, rows ...[]string) fakeResult {
	r := fakeResult{hangupAt: -1, tag: "SELECT " + strconv.Itoa(len(rows))}
	for _, c := range cols {
		r.cols = append(r.cols, column{c, OIDText})
	}
	for _, row := range rows {
		var rr [][]byte
		for _, v := range row {
			rr = append(rr, []byte(v))
		}
		r.rows = append(r.rows, rr)
	}
	return r
}

type fakePG struct {
	t         *testing.T
	ln        net.Listener
	auth      string // trust, cleartext, md5, scram, scram-forged
	user      string
	password  string
	tlsCfg    *tls.Config // non-nil: SSLRequest is answered 'S'
	refuseTLS bool        // answer SSLRequest 'N'
	script    func(sql string, args []string) fakeResult

	mu         sync.Mutex
	conns      int
	live       int
	statements []string
	cancels    []struct {
		pid int32
		key []byte
	}
	startups []map[string]string
	sslSeen  int
	closed   chan struct{}
	open     []net.Conn
	pidSeq   atomic.Int32
}

func newFakePG(t *testing.T, auth string) *fakePG {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakePG{t: t, ln: ln, auth: auth, user: "app", password: "secret", closed: make(chan struct{})}
	f.pidSeq.Store(1000)
	f.script = func(sql string, args []string) fakeResult {
		return fakeResult{hangupAt: -1, tag: "SELECT 0"}
	}
	go f.serve()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakePG) addr() netip.AddrPort { return netip.MustParseAddrPort(f.ln.Addr().String()) }

func (f *fakePG) config() Config {
	return Config{Addr: f.addr(), User: f.user, Password: f.password, Database: "app", Shard: 1}
}

func (f *fakePG) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(c)
	}
}

func (f *fakePG) statementsSeen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.statements...)
}

func (f *fakePG) liveConns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.live
}

type fakeConn struct {
	f   *fakePG
	c   net.Conn
	r   *bufio.Reader
	w   []byte
	pid int32
	key []byte
}

func (c *fakeConn) begin(t byte) int {
	c.w = append(c.w, t, 0, 0, 0, 0)
	return len(c.w) - 4
}
func (c *fakeConn) end(m int)     { binary.BigEndian.PutUint32(c.w[m:], uint32(len(c.w)-m)) }
func (c *fakeConn) cstr(s string) { c.w = append(append(c.w, s...), 0) }
func (c *fakeConn) i32(v int32)   { c.w = binary.BigEndian.AppendUint32(c.w, uint32(v)) }
func (c *fakeConn) i16(v int)     { c.w = binary.BigEndian.AppendUint16(c.w, uint16(v)) }
func (c *fakeConn) flush() error  { _, err := c.c.Write(c.w); c.w = c.w[:0]; return err }
func (c *fakeConn) readMsg() (byte, []byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(c.r, h[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(h[1:])) - 4
	b := make([]byte, n)
	_, err := io.ReadFull(c.r, b)
	return h[0], b, err
}

func (c *fakeConn) errorResponse(e *Error) {
	m := c.begin('E')
	for _, kv := range [][2]string{{"S", e.Severity}, {"V", e.Severity}, {"C", e.Code}, {"M", e.Message}} {
		c.w = append(c.w, kv[0][0])
		c.cstr(kv[1])
	}
	c.w = append(c.w, 0)
	c.end(m)
}

func (c *fakeConn) ready() {
	m := c.begin('Z')
	c.w = append(c.w, 'I')
	c.end(m)
}

func (f *fakePG) handle(nc net.Conn) {
	c := &fakeConn{f: f, c: nc, r: bufio.NewReader(nc)}
	f.mu.Lock()
	f.conns++
	f.live++
	f.open = append(f.open, nc)
	f.mu.Unlock()
	defer func() {
		nc.Close()
		f.mu.Lock()
		f.live--
		f.mu.Unlock()
	}()
	// startup packet(s)
	for {
		var lb [4]byte
		if _, err := io.ReadFull(c.r, lb[:]); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(lb[:])-4)
		if _, err := io.ReadFull(c.r, body); err != nil {
			return
		}
		code := binary.BigEndian.Uint32(body)
		switch code {
		case sslRequest:
			f.mu.Lock()
			f.sslSeen++
			f.mu.Unlock()
			if f.tlsCfg == nil || f.refuseTLS {
				nc.Write([]byte{'N'})
				continue
			}
			nc.Write([]byte{'S'})
			tc := tls.Server(nc, f.tlsCfg)
			if err := tc.Handshake(); err != nil {
				return
			}
			c.c, c.r = tc, bufio.NewReader(tc)
			continue
		case cancelRequest:
			f.mu.Lock()
			f.cancels = append(f.cancels, struct {
				pid int32
				key []byte
			}{int32(binary.BigEndian.Uint32(body[4:])), append([]byte(nil), body[8:]...)})
			f.mu.Unlock()
			return
		case protoVersion3:
			params := map[string]string{}
			kv := strings.Split(string(body[4:]), "\x00")
			for i := 0; i+1 < len(kv); i += 2 {
				params[kv[i]] = kv[i+1]
			}
			f.mu.Lock()
			f.startups = append(f.startups, params)
			f.mu.Unlock()
			if !f.login(c, params) {
				return
			}
			f.session(c)
			return
		default:
			return
		}
	}
}

func (f *fakePG) fail(c *fakeConn, code, msg string) bool {
	c.errorResponse(&Error{Severity: "FATAL", Code: code, Message: msg})
	c.flush()
	return false
}

func (f *fakePG) login(c *fakeConn, params map[string]string) bool {
	if params["user"] != f.user {
		return f.fail(c, "28000", "no such role")
	}
	authReq := func(code int32, extra ...byte) {
		m := c.begin('R')
		c.i32(code)
		c.w = append(c.w, extra...)
		c.end(m)
		c.flush()
	}
	switch f.auth {
	case "trust":
	case "cleartext":
		authReq(3)
		t, b, err := c.readMsg()
		if err != nil || t != 'p' || string(b) != f.password+"\x00" {
			return f.fail(c, "28P01", "password authentication failed")
		}
	case "md5":
		salt := []byte{1, 2, 3, 4}
		authReq(5, salt...)
		t, b, err := c.readMsg()
		if err != nil || t != 'p' || string(b) != md5Password(f.user, f.password, salt)+"\x00" {
			return f.fail(c, "28P01", "password authentication failed")
		}
	case "scram", "scram-forged":
		if !f.scram(c) {
			return false
		}
	}
	m := c.begin('R')
	c.i32(0)
	c.end(m)
	for _, kv := range [][2]string{{"server_version", "17.0"}, {"client_encoding", "UTF8"}} {
		m = c.begin('S')
		c.cstr(kv[0])
		c.cstr(kv[1])
		c.end(m)
	}
	c.pid = f.pidSeq.Add(1)
	c.key = make([]byte, 4)
	rand.Read(c.key)
	m = c.begin('K')
	c.i32(c.pid)
	c.w = append(c.w, c.key...)
	c.end(m)
	c.ready()
	return c.flush() == nil
}

// scram is the server side of SCRAM-SHA-256, written from the RFC separately from
// the client.
func (f *fakePG) scram(c *fakeConn) bool {
	m := c.begin('R')
	c.i32(10)
	c.cstr("SCRAM-SHA-256")
	c.w = append(c.w, 0)
	c.end(m)
	c.flush()
	t, b, err := c.readMsg()
	if err != nil || t != 'p' {
		return false
	}
	mech, rest, _ := strings.Cut(string(b), "\x00")
	if mech != "SCRAM-SHA-256" {
		return f.fail(c, "28000", "bad mechanism")
	}
	clientFirst := string(rest[4:])
	bare, ok := strings.CutPrefix(clientFirst, "n,,")
	if !ok {
		return f.fail(c, "28000", "bad gs2 header")
	}
	var cnonce string
	for _, p := range strings.Split(bare, ",") {
		if v, ok := strings.CutPrefix(p, "r="); ok {
			cnonce = v
		}
	}
	salt := []byte("0123456789abcdef")
	nonce := cnonce + "srvnonce"
	serverFirst := "r=" + nonce + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
	m = c.begin('R')
	c.i32(11)
	c.w = append(c.w, serverFirst...)
	c.end(m)
	c.flush()
	t, b, err = c.readMsg()
	if err != nil || t != 'p' {
		return false
	}
	final := string(b)
	withoutProof, proofPart, _ := strings.Cut(final, ",p=")
	proof, _ := base64.StdEncoding.DecodeString(proofPart)
	salted, _ := pbkdf2.Key(sha256.New, f.password, salt, 4096, 32)
	mac := func(key []byte, s string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(s))
		return h.Sum(nil)
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	authMsg := bare + "," + serverFirst + "," + withoutProof
	sig := mac(stored[:], authMsg)
	want := make([]byte, 32)
	for i := range want {
		want[i] = clientKey[i] ^ sig[i]
	}
	if string(proof) != string(want) {
		return f.fail(c, "28P01", "password authentication failed")
	}
	serverSig := mac(mac(salted, "Server Key"), authMsg)
	if f.auth == "scram-forged" {
		serverSig[0] ^= 1
	}
	m = c.begin('R')
	c.i32(12)
	c.w = append(c.w, "v="+base64.StdEncoding.EncodeToString(serverSig)...)
	c.end(m)
	return c.flush() == nil
}

// session answers statements until the client goes.
func (f *fakePG) session(c *fakeConn) {
	var sql string
	var args []string
	for {
		t, b, err := c.readMsg()
		if err != nil {
			return
		}
		switch t {
		case 'X':
			return
		case 'Q':
			sql, args = strings.TrimSuffix(string(b), "\x00"), nil
			f.record(sql)
			if !f.answer(c, sql, args, false) {
				return
			}
		case 'P':
			name, rest, _ := strings.Cut(string(b), "\x00")
			_ = name
			sql, _, _ = strings.Cut(rest, "\x00")
			args = nil
		case 'B':
			args = parseBindArgs(b)
		case 'D', 'E', 'f':
		case 'S':
			f.record(sql)
			if !f.answer(c, sql, args, true) {
				return
			}
		}
	}
}

func (f *fakePG) record(sql string) {
	f.mu.Lock()
	f.statements = append(f.statements, sql)
	f.mu.Unlock()
}

func parseBindArgs(b []byte) []string {
	r := reader{b: b}
	r.cstr()
	r.cstr()
	nf := r.i16()
	for i := 0; i < nf; i++ {
		r.i16()
	}
	n := r.i16()
	var out []string
	for i := 0; i < n; i++ {
		l := r.i32()
		if l < 0 {
			out = append(out, "<NULL>")
			continue
		}
		out = append(out, string(r.take(int(l))))
	}
	return out
}

// answer sends the scripted result. It returns false when the script ends the connection.
func (f *fakePG) answer(c *fakeConn, sql string, args []string, extended bool) bool {
	res := f.script(sql, args)
	if res.silent {
		// Ignore everything until the client goes away.
		io.Copy(io.Discard, c.r)
		return false
	}
	if res.delay > 0 {
		time.Sleep(res.delay)
	}
	if res.hangupBeforeReply {
		return false
	}
	if extended {
		m := c.begin('1')
		c.end(m)
		m = c.begin('2')
		c.end(m)
	}
	switch {
	case res.err != nil:
		c.errorResponse(res.err)
	case res.copyOut:
		m := c.begin('H')
		c.w = append(c.w, 0)
		c.i16(0)
		c.end(m)
		m = c.begin('d')
		c.w = append(c.w, "a\tb\n"...)
		c.end(m)
		m = c.begin('c')
		c.end(m)
		m = c.begin('C')
		c.cstr("COPY 1")
		c.end(m)
	case res.copyIn:
		m := c.begin('G')
		c.w = append(c.w, 0)
		c.i16(0)
		c.end(m)
		c.flush()
		// the client answers CopyFail; PostgreSQL then sends an error
		c.readMsg()
		c.errorResponse(&Error{Severity: "ERROR", Code: "57014", Message: "COPY from stdin failed: client says no"})
	default:
		if res.cols != nil {
			m := c.begin('T')
			c.i16(len(res.cols))
			for _, col := range res.cols {
				c.cstr(col.name)
				c.i32(0)
				c.i16(0)
				c.i32(int32(col.oid))
				c.i16(-1)
				c.i32(-1)
				c.i16(0)
			}
			c.end(m)
		} else if extended {
			m := c.begin('n')
			c.end(m)
		}
		for i, row := range res.rows {
			if res.hangupAt >= 0 && i == res.hangupAt {
				c.flush()
				return false
			}
			m := c.begin('D')
			if res.badRow {
				c.i16(len(row) + 1)
			} else {
				c.i16(len(row))
			}
			for _, v := range row {
				if v == nil {
					c.i32(-1)
				} else {
					c.i32(int32(len(v)))
					c.w = append(c.w, v...)
				}
			}
			c.end(m)
		}
		if res.hangupAt >= 0 {
			c.flush()
			return false
		}
		m := c.begin('C')
		c.cstr(res.tag)
		c.end(m)
	}
	c.ready()
	return c.flush() == nil
}

func closeAllFake(f *fakePG) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.open {
		c.Close()
	}
}
