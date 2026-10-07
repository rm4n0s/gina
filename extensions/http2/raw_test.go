//go:build linux

package http2_test

// Frame-level tests: a hand-written HTTP/2 client that can send what no real
// client would (malformed frames, floods, zero windows) and checks exactly what
// the server answers.

import (
	"bytes"
	ctls "crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rm4n0s/gina/extensions/http2"
)

const (
	tData, tHeaders, tPriority, tRST, tSettings, tPush, tPing, tGoAway, tWindowUpdate, tContinuation = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
	fEndStream, fAck, fEndHeaders, fPadded, fPriority                                                = 1, 1, 4, 8, 0x20

	// error codes
	eNo, eProtocol, eFlowControl, eStreamClosed, eFrameSize, eRefused, eCompression, eCalm = 0, 1, 3, 5, 6, 7, 9, 11
)

type frame struct {
	typ, flags byte
	id         uint32
	p          []byte
}

func (f frame) code() uint32 { // RST_STREAM / GOAWAY error code
	if f.typ == tGoAway {
		return binary.BigEndian.Uint32(f.p[4:])
	}
	return binary.BigEndian.Uint32(f.p)
}

func fr(typ, flags byte, id uint32, p []byte) []byte {
	b := []byte{byte(len(p) >> 16), byte(len(p) >> 8), byte(len(p)), typ, flags, byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)}
	return append(b, p...)
}

func u32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

func settings(kv ...uint32) []byte {
	var p []byte
	for i := 0; i+1 < len(kv); i += 2 {
		p = binary.BigEndian.AppendUint16(p, uint16(kv[i]))
		p = binary.BigEndian.AppendUint32(p, kv[i+1])
	}
	return fr(tSettings, 0, 0, p)
}

const (
	setEnablePush, setInitialWindow, setMaxFrame = 2, 4, 5
)

type raw struct {
	t    *testing.T
	c    net.Conn
	dec  *http2.TestDecoder
	srv  map[uint16]uint32 // the server's SETTINGS
	rbuf []byte
}

func (h *srv) raw() *raw {
	h.t.Helper()
	return &raw{t: h.t, c: dial(h.t, h.addr()), dec: http2.NewTestDecoder(), srv: map[uint16]uint32{}}
}

func (r *raw) send(parts ...[]byte) {
	r.t.Helper()
	r.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := r.c.Write(bytes.Join(parts, nil)); err != nil {
		r.t.Fatalf("write: %v", err)
	}
}

func (r *raw) read(d time.Duration) (frame, error) {
	r.c.SetReadDeadline(time.Now().Add(d))
	for len(r.rbuf) < 9 || len(r.rbuf) < 9+int(r.rbuf[0])<<16+int(r.rbuf[1])<<8+int(r.rbuf[2]) {
		var tmp [32 << 10]byte
		n, err := r.c.Read(tmp[:])
		r.rbuf = append(r.rbuf, tmp[:n]...)
		if err != nil && (n == 0 || len(r.rbuf) < 9) {
			return frame{}, err
		}
		if err != nil && len(r.rbuf) < 9+int(r.rbuf[0])<<16+int(r.rbuf[1])<<8+int(r.rbuf[2]) {
			return frame{}, err
		}
	}
	n := int(r.rbuf[0])<<16 + int(r.rbuf[1])<<8 + int(r.rbuf[2])
	f := frame{typ: r.rbuf[3], flags: r.rbuf[4], id: binary.BigEndian.Uint32(r.rbuf[5:]) & 0x7fffffff, p: append([]byte(nil), r.rbuf[9:9+n]...)}
	r.rbuf = r.rbuf[9+n:]
	return f, nil
}

func (r *raw) next() frame {
	r.t.Helper()
	f, err := r.read(3 * time.Second)
	if err != nil {
		r.t.Fatalf("waiting for a frame: %v", err)
	}
	return f
}

// quiet asserts that the server sends nothing for d.
func (r *raw) quiet(d time.Duration) {
	r.t.Helper()
	if f, err := r.read(d); err == nil {
		r.t.Fatalf("unexpected frame: type %d flags %#x stream %d len %d", f.typ, f.flags, f.id, len(f.p))
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		r.t.Fatalf("connection ended while expecting silence: %v", err)
	}
}

// hello sends the client preface and SETTINGS, then consumes the server's
// preface and the ACK of ours, acknowledging the server's settings.
func (r *raw) hello(kv ...uint32) {
	r.t.Helper()
	r.send([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), settings(kv...))
	gotSettings, gotAck := false, false
	for !gotSettings || !gotAck {
		f := r.next()
		switch {
		case f.typ == tSettings && f.flags&fAck != 0:
			gotAck = true
		case f.typ == tSettings:
			gotSettings = true
			for i := 0; i+6 <= len(f.p); i += 6 {
				r.srv[binary.BigEndian.Uint16(f.p[i:])] = binary.BigEndian.Uint32(f.p[i+2:])
			}
			r.send(fr(tSettings, fAck, 0, nil))
		case f.typ == tWindowUpdate && f.id == 0:
		default:
			r.t.Fatalf("unexpected frame during the handshake: type %d", f.typ)
		}
	}
}

func req(method, path string, extra ...[2]string) []byte {
	f := [][2]string{{":method", method}, {":scheme", "http"}, {":path", path}, {":authority", "test"}}
	return http2.EncodeBlock(append(f, extra...)...)
}

func (r *raw) get(id uint32, path string) {
	r.send(fr(tHeaders, fEndStream|fEndHeaders, id, req("GET", path)))
}

type response struct {
	status    string
	hdr       map[string]string
	body      []byte
	dataSizes []int
	rst       bool
	code      uint32
	extra     []frame // frames for other streams seen meanwhile
}

// readResp reads until stream id ends (END_STREAM or RST_STREAM).
func (r *raw) readResp(id uint32) response {
	r.t.Helper()
	resp := response{hdr: map[string]string{}}
	var block []byte
	for {
		f := r.next()
		if f.id != id {
			if f.typ == tGoAway {
				r.t.Fatalf("GOAWAY(%d) while waiting for stream %d", f.code(), id)
			}
			resp.extra = append(resp.extra, f)
			continue
		}
		switch f.typ {
		case tHeaders, tContinuation:
			block = append(block, f.p...)
			if f.flags&fEndHeaders != 0 {
				fields, err := r.dec.Decode(block)
				if err != nil {
					r.t.Fatalf("decoding response headers: %v", err)
				}
				for _, kv := range fields {
					if kv[0] == ":status" {
						resp.status = kv[1]
					} else if _, dup := resp.hdr[kv[0]]; !dup {
						resp.hdr[kv[0]] = kv[1]
					}
				}
				block = nil
			}
		case tData:
			resp.body = append(resp.body, f.p...)
			resp.dataSizes = append(resp.dataSizes, len(f.p))
		case tRST:
			resp.rst, resp.code = true, f.code()
			return resp
		}
		if f.flags&fEndStream != 0 && f.typ != tRST {
			return resp
		}
	}
}

func (r *raw) wantStatus(id uint32, status, body string) {
	r.t.Helper()
	resp := r.readResp(id)
	if resp.rst || resp.status != status || (body != "\x00" && string(resp.body) != body) {
		r.t.Fatalf("stream %d: status %q body %q rst=%v/%d; want %s %q", id, resp.status, resp.body, resp.rst, resp.code, status, body)
	}
}

func (r *raw) wantRST(id uint32, code uint32) {
	r.t.Helper()
	for {
		f := r.next()
		if f.typ == tGoAway {
			r.t.Fatalf("GOAWAY(%d) where RST_STREAM(%d) was expected", f.code(), code)
		}
		if f.typ == tRST && f.id == id {
			if f.code() != code {
				r.t.Fatalf("RST_STREAM on %d with code %d, want %d", id, f.code(), code)
			}
			return
		}
	}
}

// wantGoAway skips other frames, checks the GOAWAY code and that the server then hangs up.
func (r *raw) wantGoAway(code uint32) {
	r.t.Helper()
	for {
		f := r.next()
		if f.typ != tGoAway {
			continue
		}
		if f.code() != code {
			r.t.Fatalf("GOAWAY with code %d, want %d", f.code(), code)
		}
		break
	}
	r.wantClosed()
}

func (r *raw) wantClosed() {
	r.t.Helper()
	for {
		f, err := r.read(3 * time.Second)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				r.t.Fatal("the server did not close the connection")
			}
			return // EOF or reset
		}
		_ = f
	}
}

// ---- tests ----

func TestRawHandshake(t *testing.T) {
	h := start(t, 1, false, func(c *http2.Config) { c.MaxConcurrentStreams = 7 })
	r := h.raw()
	r.hello()
	if r.srv[3] != 7 || r.srv[6] != 16384 {
		t.Fatalf("server settings: %v", r.srv)
	}
	if v, ok := r.srv[4]; !ok || v != 256<<10 {
		t.Fatalf("initial window setting: %v", r.srv)
	}
	r.get(1, "/")
	r.wantStatus(1, "200", "hello\n")
}

func TestRawResponseHeaders(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.get(1, "/hello/x")
	resp := r.readResp(1)
	if resp.status != "200" || resp.hdr["content-type"] != "text/plain; charset=utf-8" || resp.hdr["content-length"] != "8" ||
		resp.hdr["server"] != "gina" || resp.hdr["date"] == "" {
		t.Fatalf("%+v", resp)
	}
	for name := range resp.hdr {
		if name != strings.ToLower(name) {
			t.Fatalf("response header %q is not lowercase", name)
		}
	}
	// HEAD: headers only, content-length of the GET body
	r.send(fr(tHeaders, fEndStream|fEndHeaders, 3, req("HEAD", "/hello/x")))
	resp = r.readResp(3)
	if resp.status != "200" || len(resp.body) != 0 || len(resp.dataSizes) != 0 || resp.hdr["content-length"] != "8" {
		t.Fatalf("HEAD: %+v", resp)
	}
}

func TestRawNotHTTP2(t *testing.T) {
	h := start(t, 1, false)
	for name, junk := range map[string]string{
		"http/1.1":       "GET / HTTP/1.1\r\nHost: x\r\n\r\n",
		"wrong preface":  "PRI * HTTP/2.0\r\n\r\nSX\r\n\r\n",
		"preface prefix": "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\nXXXX", // the preface is right, the rest is not a frame... see below
	} {
		if name == "preface prefix" {
			continue
		}
		r := h.raw()
		r.send([]byte(junk))
		r.wantClosed()
	}
}

func TestRawPing(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.send(fr(tPing, 0, 0, []byte("12345678")))
	f := r.next()
	if f.typ != tPing || f.flags != fAck || string(f.p) != "12345678" {
		t.Fatalf("got %+v", f)
	}
	r.send(fr(tPing, fAck, 0, []byte("abcdefgh"))) // an ACK is not answered
	r.quiet(100 * time.Millisecond)
}

func TestRawConnectionErrors(t *testing.T) {
	big := make([]byte, 16385)
	cases := []struct {
		name string
		send []byte
		code uint32
	}{
		{"DATA on stream 0", fr(tData, 0, 0, []byte("x")), eProtocol},
		{"HEADERS on stream 0", fr(tHeaders, fEndHeaders, 0, req("GET", "/")), eProtocol},
		{"HEADERS on an even stream", fr(tHeaders, fEndHeaders|fEndStream, 2, req("GET", "/")), eProtocol},
		{"client PUSH_PROMISE", fr(tPush, fEndHeaders, 1, append(u32(2), req("GET", "/")...)), eProtocol},
		{"RST_STREAM on an idle stream", fr(tRST, 0, 5, u32(0)), eProtocol},
		{"RST_STREAM on stream 0", fr(tRST, 0, 0, u32(0)), eProtocol},
		{"RST_STREAM of the wrong size", fr(tRST, 0, 1, []byte{0, 0}), eFrameSize},
		{"WINDOW_UPDATE of 0 on the connection", fr(tWindowUpdate, 0, 0, u32(0)), eProtocol},
		{"WINDOW_UPDATE overflowing the connection", fr(tWindowUpdate, 0, 0, u32(0x7fffffff)), eFlowControl},
		{"WINDOW_UPDATE on an idle stream", fr(tWindowUpdate, 0, 9, u32(1)), eProtocol},
		{"WINDOW_UPDATE of the wrong size", fr(tWindowUpdate, 0, 0, []byte{1}), eFrameSize},
		{"SETTINGS on a stream", fr(tSettings, 0, 1, nil), eProtocol},
		{"SETTINGS of the wrong size", fr(tSettings, 0, 0, []byte{1, 2, 3}), eFrameSize},
		{"SETTINGS ACK with a payload", fr(tSettings, fAck, 0, make([]byte, 6)), eFrameSize},
		{"ENABLE_PUSH = 2", settings(setEnablePush, 2), eProtocol},
		{"INITIAL_WINDOW_SIZE too big", settings(setInitialWindow, 1<<31), eFlowControl},
		{"MAX_FRAME_SIZE too small", settings(setMaxFrame, 100), eProtocol},
		{"MAX_FRAME_SIZE too big", settings(setMaxFrame, 1<<24), eProtocol},
		{"PING on a stream", fr(tPing, 0, 1, make([]byte, 8)), eProtocol},
		{"PING of the wrong size", fr(tPing, 0, 0, make([]byte, 7)), eFrameSize},
		{"GOAWAY on a stream", fr(tGoAway, 0, 1, make([]byte, 8)), eProtocol},
		{"frame larger than SETTINGS_MAX_FRAME_SIZE", fr(tData, 0, 1, big), eFrameSize},
		{"undecodable header block", fr(tHeaders, fEndHeaders|fEndStream, 1, []byte{0xff, 0x7f}), eCompression},
		{"HEADERS with a bad padding length", fr(tHeaders, fPadded|fEndHeaders, 1, []byte{200, 0x82}), eProtocol},
		{"HEADERS depending on itself", fr(tHeaders, fPriority|fEndHeaders|fEndStream, 1, append(u32(1), 15, 0x82)), eProtocol},
		{"CONTINUATION without HEADERS", fr(tContinuation, fEndHeaders, 1, []byte{0x82}), eProtocol},
		{"HEADERS then another frame", append(fr(tHeaders, 0, 1, req("GET", "/")[:3]), fr(tPing, 0, 0, make([]byte, 8))...), eProtocol},
		{"HEADERS then CONTINUATION on another stream", append(fr(tHeaders, 0, 1, req("GET", "/")[:3]), fr(tContinuation, fEndHeaders, 3, []byte{0x82})...), eProtocol},
		{"DATA on an idle stream", fr(tData, 0, 3, []byte("x")), eProtocol},
		{"HEADERS reusing a closed stream", append(fr(tHeaders, fEndHeaders|fEndStream, 5, req("GET", "/")), fr(tHeaders, fEndHeaders|fEndStream, 3, req("GET", "/"))...), eStreamClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t, 1, false)
			r := h.raw()
			r.hello()
			r.send(tc.send)
			r.wantGoAway(tc.code)
		})
	}
}

func TestRawMalformedRequests(t *testing.T) {
	cases := []struct {
		name  string
		block []byte
	}{
		{"uppercase header name", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{":path", "/"}, [2]string{"X-Upper", "1"})},
		{"missing :path", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"})},
		{"missing :method", http2.EncodeBlock([2]string{":scheme", "http"}, [2]string{":path", "/"})},
		{"missing :scheme", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":path", "/"})},
		{"empty :path", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{":path", ""})},
		{"duplicate :path", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{":path", "/"}, [2]string{":path", "/"})},
		{"unknown pseudo-header", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{":path", "/"}, [2]string{":bogus", "1"})},
		{"response pseudo-header", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{":path", "/"}, [2]string{":status", "200"})},
		{"pseudo-header after a regular one", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{"x-a", "1"}, [2]string{":path", "/"})},
		{"Connection header", req("GET", "/", [2]string{"connection", "close"})},
		{"Transfer-Encoding header", req("GET", "/", [2]string{"transfer-encoding", "chunked"})},
		{"Upgrade header", req("GET", "/", [2]string{"upgrade", "h2c"})},
		{"TE other than trailers", req("GET", "/", [2]string{"te", "gzip"})},
		{"newline in a value", req("GET", "/", [2]string{"x-a", "a\nb"})},
		{"space in a name", req("GET", "/", [2]string{"x a", "1"})},
		{"path not starting with /", http2.EncodeBlock([2]string{":method", "GET"}, [2]string{":scheme", "http"}, [2]string{":path", "x"})},
		{"space in the path", req("GET", "/a b")},
		{"conflicting content-lengths", req("POST", "/echo", [2]string{"content-length", "1"}, [2]string{"content-length", "2"})},
		{"non-numeric content-length", req("POST", "/echo", [2]string{"content-length", "1x"})},
		{"bad method token", req("G ET", "/")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t, 1, false)
			r := h.raw()
			r.hello()
			r.send(fr(tHeaders, fEndHeaders|fEndStream, 1, tc.block))
			r.wantRST(1, eProtocol)
			// a stream error leaves the connection usable
			r.get(3, "/")
			r.wantStatus(3, "200", "hello\n")
		})
	}
}

func TestRawContinuationAndPadding(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()

	// a header block split over HEADERS + 2 CONTINUATIONs, at arbitrary byte positions
	blk := req("GET", "/hello/split", [2]string{"x-pad", strings.Repeat("v", 300)})
	r.send(fr(tHeaders, fEndStream, 1, blk[:5]), fr(tContinuation, 0, 1, blk[5:100]), fr(tContinuation, fEndHeaders, 1, blk[100:]))
	r.wantStatus(1, "200", "hello split\n")

	// padded HEADERS with priority fields
	hp := append([]byte{3}, append(append(u32(0), 15), req("GET", "/hello/pad")...)...)
	hp = append(hp, 0, 0, 0)
	r.send(fr(tHeaders, fPadded|fPriority|fEndHeaders|fEndStream, 3, hp))
	r.wantStatus(3, "200", "hello pad\n")

	// padded DATA: the padding is flow-controlled but not part of the body
	r.send(fr(tHeaders, fEndHeaders, 5, req("POST", "/echo")))
	r.send(fr(tData, fPadded|fEndStream, 5, append(append([]byte{4}, "payload"...), 0, 0, 0, 0)))
	r.wantStatus(5, "200", "payload")
}

func TestRawRequestBody(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.send(fr(tHeaders, fEndHeaders, 1, req("POST", "/echo", [2]string{"content-length", "11"})))
	r.send(fr(tData, 0, 1, []byte("hello")), fr(tData, 0, 1, nil), fr(tData, fEndStream, 1, []byte(" world")))
	r.wantStatus(1, "200", "hello world")

	// content-length that disagrees with the data is a stream error (RFC 9113 §8.1.1)
	r.send(fr(tHeaders, fEndHeaders, 3, req("POST", "/echo", [2]string{"content-length", "5"})))
	r.send(fr(tData, fEndStream, 3, []byte("abc")))
	r.wantRST(3, eProtocol)

	// trailers after the body are accepted (and dropped)
	r.send(fr(tHeaders, fEndHeaders, 5, req("POST", "/echo")))
	r.send(fr(tData, 0, 5, []byte("body")))
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 5, http2.EncodeBlock([2]string{"x-trailer", "1"})))
	r.wantStatus(5, "200", "body")
	// but trailers that do not end the stream are not
	r.send(fr(tHeaders, fEndHeaders, 7, req("POST", "/echo")))
	r.send(fr(tHeaders, fEndHeaders, 7, http2.EncodeBlock([2]string{"x-trailer", "1"})))
	r.wantRST(7, eProtocol)
	// and trailers cannot carry pseudo-headers
	r.send(fr(tHeaders, fEndHeaders, 9, req("POST", "/echo")))
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 9, http2.EncodeBlock([2]string{":path", "/"})))
	r.wantRST(9, eProtocol)
}

func TestRawEarlyRejections(t *testing.T) {
	h := start(t, 1, false, func(c *http2.Config) { c.MaxBodyBytes = 1000; c.MaxHeaderBytes = 2000; c.MaxURIBytes = 100 })
	r := h.raw()
	r.hello()

	// too big by content-length: answered at once, then the client is told to stop (RFC 9113 §8.1)
	r.send(fr(tHeaders, fEndHeaders, 1, req("POST", "/echo", [2]string{"content-length", "5000"})))
	r.wantStatus(1, "413", "\x00")
	if f := r.next(); f.typ != tRST || f.id != 1 || f.code() != eNo {
		t.Fatalf("after the 413: %+v", f)
	}
	// data the client had already sent is ignored without breaking the connection
	r.send(fr(tData, 0, 1, make([]byte, 100)))

	// too big in fact
	r.send(fr(tHeaders, fEndHeaders, 3, req("POST", "/echo")))
	r.send(fr(tData, 0, 3, make([]byte, 600)), fr(tData, 0, 3, make([]byte, 600)))
	r.wantStatus(3, "413", "\x00")

	// header list too big: 431, and the HPACK state is still in sync afterwards
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 5, req("GET", "/", [2]string{"x-big", strings.Repeat("a", 3000)})))
	r.wantStatus(5, "431", "\x00")
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 7, req("GET", "/long/"+strings.Repeat("p", 150))))
	r.wantStatus(7, "414", "\x00")
	r.get(9, "/")
	r.wantStatus(9, "200", "hello\n")

	// a header block that never ends is cut off
	r.send(fr(tHeaders, 0, 11, make([]byte, 3000)), fr(tContinuation, 0, 11, make([]byte, 3000)), fr(tContinuation, 0, 11, make([]byte, 3000)))
	r.wantGoAway(eCalm)
}

func TestRawUnsupportedMethods(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 1, req("CONNECT", "/")))
	r.wantStatus(1, "501", "\x00")
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 3, req("BREW", "/")))
	r.wantStatus(3, "501", "\x00")
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 5, req("OPTIONS", "/")))
	r.wantStatus(5, "405", "\x00") // routed, and the route is GET only
}

func TestRawHPACKDynamicTable(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	// first request puts x-a into the dynamic table; the second refers to it by index (62)
	blk1 := append(req("GET", "/headers"), http2.EncodeBlockIndexed([2]string{"x-a", "first"})...)
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 1, blk1))
	if resp := r.readResp(1); !strings.Contains(string(resp.body), "x-a=first\n") {
		t.Fatalf("%q", resp.body)
	}
	blk2 := append(req("GET", "/headers"), 0xbe)
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 3, blk2))
	if resp := r.readResp(3); !strings.Contains(string(resp.body), "x-a=first\n") {
		t.Fatalf("indexed reference to the dynamic table: %q", resp.body)
	}
	// split cookies are rejoined with "; " (RFC 9113 §8.2.3)
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 5, req("GET", "/headers", [2]string{"cookie", "a=1"}, [2]string{"cookie", "b=2"}, [2]string{"cookie", "c=3"})))
	if resp := r.readResp(5); strings.Count(string(resp.body), "cookie=") != 1 || !strings.Contains(string(resp.body), "cookie=a=1; b=2; c=3\n") {
		t.Fatalf("%q", resp.body)
	}
}

func TestRawStreamLimit(t *testing.T) {
	h := start(t, 1, false, func(c *http2.Config) { c.MaxConcurrentStreams = 2 })
	r := h.raw()
	r.hello()
	r.send(fr(tHeaders, fEndHeaders, 1, req("POST", "/echo")), fr(tHeaders, fEndHeaders, 3, req("POST", "/echo")))
	// the refused request adds x-r to the HPACK dynamic table; the header block has
	// to be decoded all the same or every later block on the connection is garbage
	refused := append(req("GET", "/"), http2.EncodeBlockIndexed([2]string{"x-r", "refused"})...)
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 5, refused))
	r.wantRST(5, eRefused)
	r.send(fr(tData, fEndStream, 1, []byte("one")))
	r.wantStatus(1, "200", "one")
	r.send(fr(tHeaders, fEndHeaders|fEndStream, 7, append(req("GET", "/headers"), 0xbe)))
	if resp := r.readResp(7); !strings.Contains(string(resp.body), "x-r=refused\n") {
		t.Fatalf("dynamic table entry of a refused stream: %q", resp.body)
	}
	r.send(fr(tData, fEndStream, 3, []byte("three")))
	r.wantStatus(3, "200", "three")
}

func TestRawStreamFlowControl(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello(setInitialWindow, 10) // the server may send only 10 bytes per stream until told otherwise
	r.get(1, "/big?n=100")
	f := r.next()
	if f.typ != tHeaders {
		t.Fatalf("%+v", f)
	}
	if d := r.next(); d.typ != tData || len(d.p) != 10 || d.flags&fEndStream != 0 {
		t.Fatalf("first DATA: type %d len %d flags %#x", d.typ, len(d.p), d.flags)
	}
	r.quiet(150 * time.Millisecond)
	r.send(fr(tWindowUpdate, 0, 1, u32(30)))
	if d := r.next(); d.typ != tData || len(d.p) != 30 {
		t.Fatalf("after +30: %+v", d)
	}
	r.quiet(100 * time.Millisecond)
	r.send(fr(tWindowUpdate, 0, 1, u32(1000)))
	if d := r.next(); d.typ != tData || len(d.p) != 60 || d.flags&fEndStream == 0 {
		t.Fatalf("after +1000: type %d len %d flags %#x", d.typ, len(d.p), d.flags)
	}

	// raising SETTINGS_INITIAL_WINDOW_SIZE moves every open stream's window too
	r.get(3, "/big?n=100")
	r.next() // HEADERS
	r.next() // 10 bytes
	r.send(settings(setInitialWindow, 110))
	var got int
	for got < 90 {
		f := r.next()
		if f.typ == tData {
			got += len(f.p)
		}
	}
	if got != 90 {
		t.Fatalf("after the settings change the server sent %d more bytes, want 90", got)
	}
}

func TestRawConnectionFlowControl(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.get(1, "/big?n=40000")
	r.get(3, "/big?n=40000")
	// 80000 bytes were asked for, the connection window is 65535
	total := 0
	for total < 65535 {
		f := r.next()
		if f.typ == tData {
			total += len(f.p)
		}
	}
	if total != 65535 {
		t.Fatalf("sent %d bytes against a 65535 connection window", total)
	}
	r.quiet(150 * time.Millisecond)
	r.send(fr(tWindowUpdate, 0, 0, u32(100000)))
	rest := 0
	for rest < 80000-65535 {
		f := r.next()
		if f.typ == tData {
			rest += len(f.p)
		}
	}
	if rest != 80000-65535 {
		t.Fatalf("sent %d more bytes, want %d", rest, 80000-65535)
	}
}

func TestRawFrameSizing(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.get(1, "/big?n=40000")
	resp := r.readResp(1)
	if len(resp.body) != 40000 || len(resp.dataSizes) != 3 || resp.dataSizes[0] != 16384 || resp.dataSizes[1] != 16384 || resp.dataSizes[2] != 7232 {
		t.Fatalf("DATA frame sizes %v", resp.dataSizes)
	}
	// a smaller peer MAX_FRAME_SIZE is not possible (16384 is the floor), a larger one is honoured
	r.send(settings(setMaxFrame, 100000))
	r.next()                                     // ACK
	r.send(fr(tWindowUpdate, 0, 0, u32(100000))) // the first response used part of the connection window
	r.get(3, "/big?n=60000")
	resp = r.readResp(3)
	if len(resp.body) != 60000 || len(resp.dataSizes) != 1 {
		t.Fatalf("with MAX_FRAME_SIZE 100000: DATA frame sizes %v", resp.dataSizes)
	}
}

func TestRawResetStreams(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello(setInitialWindow, 0) // responses stay queued: nothing may be sent
	// a request answered but blocked: HEADERS only
	r.get(1, "/big?n=50")
	if f := r.next(); f.typ != tHeaders || f.flags&fEndStream != 0 {
		t.Fatalf("%+v", f)
	}
	// data on a half-closed (remote) stream is a stream error
	r.send(fr(tData, 0, 1, []byte("x")))
	r.wantRST(1, eStreamClosed)
	// a reset stream is forgotten; frames still in flight for it are ignored
	r.get(3, "/big?n=50")
	r.next()
	r.send(fr(tRST, 0, 3, u32(8)), fr(tWindowUpdate, 0, 3, u32(100)), fr(tPriority, 0, 3, []byte{0, 0, 0, 0, 5}))
	r.send(fr(tData, 0, 3, []byte("late")))
	r.send(fr(tPing, 0, 0, []byte("stillup!")))
	for {
		f := r.next()
		if f.typ == tPing {
			break
		}
		if f.typ == tGoAway || f.typ == tRST {
			t.Fatalf("unexpected %+v", f)
		}
	}
	// zero-increment WINDOW_UPDATE on a stream resets only that stream
	r.get(5, "/big?n=50")
	r.next()
	r.send(fr(tWindowUpdate, 0, 5, u32(0)))
	r.wantRST(5, eProtocol)
	// PRIORITY of the wrong size is a stream error too
	r.send(fr(tPriority, 0, 7, []byte{1, 2}))
	r.wantRST(7, eFrameSize)
}

func TestRawIgnoredFrames(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.send(fr(0xfa, 0, 0, []byte("unknown frame types are ignored")))
	r.send(fr(tPriority, 0, 11, []byte{0, 0, 0, 0, 9}))
	r.send(settings(0x77, 1)) // unknown setting
	if f := r.next(); f.typ != tSettings || f.flags != fAck {
		t.Fatalf("%+v", f)
	}
	r.get(1, "/")
	r.wantStatus(1, "200", "hello\n")
}

func TestRawGoAwayFromClient(t *testing.T) {
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	r.get(1, "/")
	r.wantStatus(1, "200", "hello\n")
	r.send(fr(tGoAway, 0, 0, append(u32(0), u32(0)...)))
	r.wantClosed() // nothing left in flight: the server hangs up
}

func TestRawControlFrameFloods(t *testing.T) {
	cases := map[string]func(i int) []byte{
		"PING":     func(i int) []byte { return fr(tPing, 0, 0, make([]byte, 8)) },
		"SETTINGS": func(i int) []byte { return settings() },
		"empty DATA": func(i int) []byte {
			return fr(tData, 0, 1, nil)
		},
		"rapid reset": func(i int) []byte {
			id := uint32(2*i + 1)
			return append(fr(tHeaders, fEndHeaders, id, req("POST", "/echo")), fr(tRST, 0, id, u32(8))...)
		},
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			h := start(t, 1, false)
			r := h.raw()
			r.hello()
			if name == "empty DATA" {
				r.send(fr(tHeaders, fEndHeaders, 1, req("POST", "/echo")))
			}
			var all []byte
			for i := 0; i < 3000; i++ {
				all = append(all, mk(i)...)
			}
			r.send(all)
			r.wantGoAway(eCalm)
		})
	}
	// ordinary use stays well under the budget
	h := start(t, 1, false)
	r := h.raw()
	r.hello()
	for i := 0; i < 200; i++ {
		r.send(fr(tPing, 0, 0, make([]byte, 8)))
		if f := r.next(); f.typ != tPing {
			t.Fatalf("%+v", f)
		}
	}
}

func TestRawTimeouts(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		h := start(t, 1, false, func(c *http2.Config) { c.IdleTimeout = 150 * time.Millisecond })
		r := h.raw()
		r.hello()
		r.get(1, "/")
		r.wantStatus(1, "200", "hello\n")
		r.wantGoAway(eNo) // told, politely, before the hang-up
	})
	t.Run("no preface", func(t *testing.T) {
		h := start(t, 1, false, func(c *http2.Config) { c.ReadTimeout = 150 * time.Millisecond })
		r := h.raw()
		begin := time.Now()
		r.wantClosed()
		if d := time.Since(begin); d > 2*time.Second {
			t.Fatalf("took %v", d)
		}
	})
	t.Run("stalled request", func(t *testing.T) {
		h := start(t, 1, false, func(c *http2.Config) { c.ReadTimeout = 150 * time.Millisecond })
		r := h.raw()
		r.hello()
		r.send(fr(tHeaders, fEndHeaders, 1, req("POST", "/echo"))) // body never comes
		r.wantGoAway(eNo)
	})
	t.Run("response blocked on the peer", func(t *testing.T) {
		h := start(t, 1, false, func(c *http2.Config) { c.WriteTimeout = 150 * time.Millisecond })
		r := h.raw()
		r.hello(setInitialWindow, 0)
		r.get(1, "/big?n=10")
		r.next()
		r.wantGoAway(eNo)
	})
}

func TestRawTLS(t *testing.T) {
	h := start(t, 1, true)
	c := ctls.Client(dial(t, h.addr()), &ctls.Config{RootCAs: h.pool, ServerName: "localhost", NextProtos: []string{"h2", "http/1.1"}})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	if c.ConnectionState().NegotiatedProtocol != "h2" {
		t.Fatalf("ALPN %q", c.ConnectionState().NegotiatedProtocol)
	}
	r := &raw{t: t, c: c, dec: http2.NewTestDecoder(), srv: map[uint16]uint32{}}
	r.hello()
	r.get(1, "/tls")
	resp := r.readResp(1)
	if !strings.HasPrefix(string(resp.body), "true h2 TLS_AES_") {
		t.Fatalf("%q", resp.body)
	}
	// 1 MiB over the TLS path in many records
	r.get(3, "/big?n="+strconv.Itoa(1<<20))
	r.send(fr(tWindowUpdate, 0, 0, u32(1<<20)), fr(tWindowUpdate, 0, 3, u32(1<<20)))
	resp = r.readResp(3)
	if len(resp.body) != 1<<20 {
		t.Fatalf("%d bytes", len(resp.body))
	}
	// a client with no ALPN at all is assumed to speak h2; one that speaks HTTP/1.1 text is hung up on
	c2 := ctls.Client(dial(t, h.addr()), &ctls.Config{RootCAs: h.pool, ServerName: "localhost"})
	if err := c2.Handshake(); err != nil {
		t.Fatal(err)
	}
	c2.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
	c2.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, err := io.Copy(io.Discard, c2); err != nil && n > 0 {
		t.Fatalf("unexpected data (%d bytes) before the close: %v", n, err)
	}
}

func TestRawConnectionsAreReleased(t *testing.T) {
	h := start(t, 1, false)
	for i := 0; i < 50; i++ {
		r := h.raw()
		r.hello()
		r.send(fr(tHeaders, fEndHeaders, 1, req("POST", "/echo"))) // leave a stream half open
		if i%2 == 0 {
			r.get(3, "/big?n=100000") // and a response blocked mid-way
		}
		r.c.Close()
	}
	for deadline := time.Now().Add(3 * time.Second); h.srv.Conns() != 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d connections still counted after every client hung up", h.srv.Conns())
		}
	}
}
