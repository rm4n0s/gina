package http2

import (
	"io"
	"strconv"

	"github.com/rm4n0s/gina"
	ghttp "github.com/rm4n0s/gina/extensions/http"
)

// ---- request headers (RFC 9113 §8.2, §8.3) ----

// h2NameChar marks bytes allowed in a field name: HTTP/2 names are lowercase
// tokens (an uppercase letter makes the request malformed, §8.2.1).
var h2NameChar = func() (t [256]bool) {
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		t[c] = true
	}
	return
}()

func validValue(v []byte) bool {
	for _, c := range v {
		if c == 0 || c == '\r' || c == '\n' {
			return false
		}
	}
	return true
}

func isMethodToken(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if !h2NameChar[c] && !('A' <= c && c <= 'Z') {
			return false
		}
	}
	return true
}

func equalFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		c := b[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

// onField receives one decoded header field of the block being processed.
func (s *Server) onField(cs *conn, name, value []byte) {
	cs.listSize += len(name) + len(value) + 32
	if cs.listSize > s.cfg.MaxHeaderBytes && cs.curErr == hdrOK {
		cs.curErr = hdrTooLarge
	}
	if cs.curErr != hdrOK {
		return // malformed or oversized: the rest of the block is only decoded
	}
	if len(name) == 0 {
		cs.curErr = hdrMalformed
		return
	}
	if cs.trailers { // trailers are dropped; pseudo-headers in them are an error
		if name[0] == ':' {
			cs.curErr = hdrMalformed
		}
		return
	}
	st := cs.cur
	if st == nil { // a refused stream: nothing to record
		return
	}
	if name[0] == ':' {
		var bit uint8
		var sp *span
		switch string(name) {
		case ":method":
			bit, sp = seenMethod, &st.method
		case ":scheme":
			bit, sp = seenScheme, &st.scheme
		case ":authority":
			bit, sp = seenAuthority, &st.auth
		case ":path":
			bit, sp = seenPath, &st.path
		case ":protocol":
			if s.cfg.ExtendedConnect {
				bit, sp = seenProtocol, &st.proto
			}
		}
		// unknown pseudo-headers (and response ones such as :status) are errors, as
		// are pseudo-headers after regular ones and duplicates
		if sp == nil || st.sawRegular || st.seen&bit != 0 || !validValue(value) {
			cs.curErr = hdrMalformed
			return
		}
		st.seen |= bit
		off := int32(len(st.hbuf))
		st.hbuf = append(st.hbuf, value...)
		*sp = span{off, int32(len(st.hbuf))}
		return
	}
	st.sawRegular = true
	for _, c := range name {
		if !h2NameChar[c] {
			cs.curErr = hdrMalformed
			return
		}
	}
	if !validValue(value) {
		cs.curErr = hdrMalformed
		return
	}
	switch string(name) {
	case "connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade":
		cs.curErr = hdrMalformed // connection-specific fields do not exist in HTTP/2
		return
	case "te":
		if !equalFold(value, "trailers") {
			cs.curErr = hdrMalformed
			return
		}
	case "content-length":
		n, ok := parseLen(value)
		if !ok || (st.clen >= 0 && st.clen != n) {
			cs.curErr = hdrMalformed
			return
		}
		st.clen = n
	}
	off := int32(len(st.hbuf))
	st.hbuf = append(st.hbuf, name...)
	voff := int32(len(st.hbuf))
	st.hbuf = append(st.hbuf, value...)
	st.hf = append(st.hf, fieldSpan{off, voff, int32(len(st.hbuf))})
}

func parseLen(v []byte) (int64, bool) {
	if len(v) == 0 || len(v) > 15 {
		return 0, false
	}
	var n int64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

func (st *stream) val(sp span) []byte { return st.hbuf[sp.off:sp.end] }

// validRequest applies the rules that need the whole header list (§8.3.1).
func (st *stream) validRequest() bool {
	if st.seen&seenMethod == 0 || !isMethodToken(st.val(st.method)) {
		return false
	}
	if string(st.val(st.method)) == "CONNECT" {
		if st.seen&seenProtocol == 0 {
			return true // plain CONNECT: answered 501 in complete
		}
		// extended CONNECT (RFC 8441 §4): :scheme, :path and :authority are required
		if st.seen&(seenScheme|seenPath|seenAuthority) != seenScheme|seenPath|seenAuthority || st.proto.end == st.proto.off {
			return false
		}
	} else if st.seen&seenProtocol != 0 {
		return false // :protocol belongs to CONNECT
	}
	if st.seen&seenScheme == 0 || st.seen&seenPath == 0 || st.scheme.end == st.scheme.off {
		return false
	}
	p := st.val(st.path)
	if len(p) == 0 {
		return false
	}
	for _, c := range p {
		if c <= 0x20 || c == 0x7f {
			return false
		}
	}
	return p[0] == '/' || (len(p) == 1 && p[0] == '*' && string(st.val(st.method)) == "OPTIONS")
}

func methodName(b []byte) (string, bool) {
	switch string(b) {
	case "GET":
		return "GET", true
	case "HEAD":
		return "HEAD", true
	case "POST":
		return "POST", true
	case "PUT":
		return "PUT", true
	case "DELETE":
		return "DELETE", true
	case "PATCH":
		return "PATCH", true
	case "OPTIONS":
		return "OPTIONS", true
	}
	return "", false
}

// ---- dispatch ----

// complete runs when a request has fully arrived: build the http.Request, run
// the router, send the response.
func (s *Server) complete(cs *conn, g *gina.Ctx, st *stream) {
	if st.streaming {
		s.endStreamed(cs, g, st)
		return
	}
	ext := st.seen&seenProtocol != 0 // extended CONNECT: answered as a GET, and the stream may become a tunnel
	if ext && st.recvEnded {
		s.reject(cs, g, st, 400) // nothing could ever be tunnelled
		return
	}
	if !ext && st.clen >= 0 && int64(len(st.body)) != st.clen {
		s.streamErr(cs, st, errProtocol) // body does not match content-length (§8.1.1)
		return
	}
	method, ok := methodName(st.val(st.method))
	if ext {
		method, ok = "GET", true
	}
	if !ok { // CONNECT, or a method this framework does not route
		s.reject(cs, g, st, 501)
		return
	}

	cs.hdrs, cs.cookie = fillRequest(&cs.req, st, method, ext, cs.hdrs[:0], cs.cookie)
	cs.c.Begin(g, &cs.req, cs.tls, cs.fd)
	s.router.Dispatch(&cs.c)
	s.respond(cs, g, st, cs.c.Result(), method == "HEAD")
}

// fillRequest builds the http.Request of a stream: its headers with the
// :authority as Host and split cookies joined (§8.2.3). hdrs and cookie are
// scratch space the request's slices live in; the grown versions come back.
func fillRequest(req *ghttp.Request, st *stream, method string, ext bool, hdrs []ghttp.Header, cookie []byte) ([]ghttp.Header, []byte) {
	hb := st.hbuf
	cookies, hasHost := 0, false
	for _, f := range st.hf {
		switch name := hb[f.nameOff:f.valOff]; string(name) {
		case "cookie":
			cookies++
		case "host":
			hasHost = true
		}
	}
	if cookies > 1 {
		cookie = cookie[:0]
	}
	for _, f := range st.hf {
		name, value := hb[f.nameOff:f.valOff], hb[f.valOff:f.end]
		if cookies > 1 && string(name) == "cookie" {
			if len(cookie) > 0 {
				cookie = append(cookie, "; "...)
			}
			cookie = append(cookie, value...)
			continue
		}
		hdrs = append(hdrs, ghttp.Header{Name: name, Value: value})
	}
	if cookies > 1 {
		hdrs = append(hdrs, ghttp.Header{Name: []byte("cookie"), Value: cookie})
	}
	if !hasHost && st.seen&seenAuthority != 0 {
		hdrs = append(hdrs, ghttp.Header{Name: []byte("host"), Value: st.val(st.auth)})
	}

	target := st.val(st.path)
	*req = ghttp.Request{
		Method: method, Target: target, Path: target, Minor: 1, Headers: hdrs,
		Body: st.body, ContentLength: len(st.body), KeepAlive: true,
	}
	if ext {
		req.Protocol = st.val(st.proto)
	}
	for i, c := range target {
		if c == '?' {
			req.Path, req.Query = target[:i], target[i+1:]
			break
		}
	}
	return hdrs, cookie
}

// beginStreamed starts a request whose route takes its body as a stream: the
// handler runs now, with the headers, and registers the callback that receives
// DATA as it arrives. The stream gets a Request and a Context of its own, since
// other streams of the connection run handlers while this one is still open.
func (s *Server) beginStreamed(cs *conn, g *gina.Ctx, st *stream) {
	method, ok := methodName(st.val(st.method))
	if !ok {
		s.reject(cs, g, st, 501)
		return
	}
	st.sreq, st.sctx = new(ghttp.Request), new(ghttp.Context)
	fillRequest(st.sreq, st, method, false, nil, nil)
	st.sreq.ContentLength = int(st.clen)
	st.sctx.Begin(g, st.sreq, cs.tls, cs.fd)
	s.router.DispatchStream(st.sctx)
	if !st.sctx.HasBodyHandler() { // answered without taking the body: ignore the rest of it
		st.discard = true
		s.respond(cs, g, st, st.sctx.Result(), method == "HEAD")
		return
	}
	st.streaming = true
}

// streamedData delivers one DATA frame of a streamed upload.
func (s *Server) streamedData(cs *conn, g *gina.Ctx, st *stream, flags byte, data []byte) {
	end := flags&flagEndStream != 0
	if st.bodyRecv += int64(len(data)); st.bodyRecv > int64(st.maxBody) {
		s.creditConn(cs)
		st.abortBody()
		s.reject(cs, g, st, 413)
		return
	}
	if end && st.clen >= 0 && st.bodyRecv != st.clen {
		st.abortBody()
		s.streamErr(cs, st, errProtocol) // body does not match content-length (§8.1.1)
		return
	}
	stop := st.sctx.DeliverBody(data, end)
	s.creditConn(cs)
	switch {
	case stop || end:
		st.streaming = false
		st.recvEnded = st.recvEnded || end
		st.discard = !end // answered early: the rest of the request is ignored
		s.respond(cs, g, st, st.sctx.Result(), false)
	default:
		s.creditStream(cs, st)
	}
}

// endStreamed finishes a streamed upload that ended with trailers instead of DATA.
func (s *Server) endStreamed(cs *conn, g *gina.Ctx, st *stream) {
	if st.clen >= 0 && st.bodyRecv != st.clen {
		st.abortBody()
		s.streamErr(cs, st, errProtocol)
		return
	}
	st.sctx.DeliverBody(nil, true)
	st.streaming = false
	s.respond(cs, g, st, st.sctx.Result(), false)
}

// reject answers a request with a plain-text error without running a handler,
// and ignores whatever remains of the request.
func (s *Server) reject(cs *conn, g *gina.Ctx, st *stream, code int) {
	st.discard, st.body = true, nil
	text := ghttp.StatusText(code) + "\n"
	s.respond(cs, g, st, ghttp.Result{Status: code, ContentType: "text/plain; charset=utf-8", Body: []byte(text)}, false)
}

// ---- responses ----

func (s *Server) respond(cs *conn, g *gina.Ctx, st *stream, res ghttp.Result, head bool) {
	ss := s.sh(g)
	ss.requests.Add(1)
	if res.Source != nil && (res.Tunnel != nil || res.Stream) { // contradictory: the tunnel or stream wins
		ghttp.CloseSource(res.Source)
		res.Source = nil
	}
	if res.Tunnel != nil && st.seen&seenProtocol != 0 && !st.recvEnded && res.Status/100 == 2 {
		s.openTunnel(cs, g, st, res)
		return
	}
	if res.Stream { // an SSE handler: events cannot be routed to one stream of many
		res = ghttp.Result{Status: 501, ContentType: "text/plain; charset=utf-8",
			Body: []byte("event streams are not supported over HTTP/2\n")}
	}
	allowed := ghttp.BodyAllowed(res.Status)
	body := res.Body
	src, streamed := res.Source, res.Source != nil
	if src != nil && (head || !allowed) { // nothing to send: do not read it
		ghttp.CloseSource(src)
		src = nil
	}

	blk := appendStatus(cs.hblk[:0], res.Status)
	if allowed {
		if len(body) > 0 || res.ContentType != "" {
			ct := res.ContentType
			if ct == "" {
				ct = "text/plain; charset=utf-8"
			}
			blk = appendLiteral(blk, []byte("content-type"), []byte(ct))
		}
		var num [20]byte
		switch {
		case !streamed:
			blk = appendLiteral(blk, []byte("content-length"), strconv.AppendInt(num[:0], int64(len(body)), 10))
		case res.SourceSize >= 0:
			blk = appendLiteral(blk, []byte("content-length"), strconv.AppendInt(num[:0], res.SourceSize, 10))
		} // streamed, length unknown: the stream's END_STREAM delimits the body
	}
	blk = appendLiteral(blk, []byte("date"), ss.date())
	blk = appendLiteral(blk, []byte("server"), []byte("gina"))
	blk = cs.appendExtra(blk, res, allowed)
	cs.hblk = blk[:0]

	endNow := !allowed || head || (len(body) == 0 && src == nil)
	cs.writeHeaders(st.id, blk, endNow)
	if endNow {
		s.finish(cs, st)
		return
	}
	st.resp = body
	if src != nil {
		st.src, st.srcLeft, st.resp = src, res.SourceSize, nil
	}
	if _, ok := cs.sendBody(st, 1<<30); !ok {
		s.abort(cs, st)
		return
	}
	if st.fin {
		s.finish(cs, st)
		return
	}
	// flow control stopped us: keep a private copy, the Context buffer is reused
	st.respBuf = append(st.respBuf[:0], st.resp...)
	st.resp = st.respBuf
	st.queued = true
	cs.sendq = append(cs.sendq, st)
}

// appendExtra encodes the headers a handler added, minus the ones that do not
// exist in HTTP/2 or are written by the server.
func (cs *conn) appendExtra(blk []byte, res ghttp.Result, bodyAllowed bool) []byte {
	res.EachHeader(func(name, value []byte) {
		cs.ntmp = append(cs.ntmp[:0], name...)
		for i, c := range cs.ntmp {
			if 'A' <= c && c <= 'Z' {
				cs.ntmp[i] = c + 'a' - 'A'
			}
		}
		switch string(cs.ntmp) {
		case "connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade",
			"content-length", "date", "server": // not valid in HTTP/2, or set by the server
			return
		case "content-type":
			if bodyAllowed {
				return
			}
		}
		blk = appendLiteral(blk, cs.ntmp, value)
	})
	return blk
}

// writeHeaders sends a header block as HEADERS plus as many CONTINUATIONs as the
// peer's frame size needs.
func (cs *conn) writeHeaders(id uint32, blk []byte, endStream bool) {
	first := true
	for {
		n := min(len(blk), cs.maxFrame)
		var flags byte
		typ := byte(frameContinuation)
		if first {
			typ = frameHeaders
			if endStream {
				flags |= flagEndStream
			}
		}
		if n == len(blk) {
			flags |= flagEndHeaders
		}
		var at int
		cs.out, at = beginFrame(cs.out, typ, flags, id)
		cs.out = append(cs.out, blk[:n]...)
		endFrame(cs.out, at)
		blk, first = blk[n:], false
		if len(blk) == 0 {
			return
		}
	}
}

// srcBuf is how much of a streamed body is read at a time.
const srcBuf = 32 << 10

// sendBody queues DATA frames for st.resp as far as both flow-control windows,
// the peer's frame size, the output high-water mark and limit allow, reading more
// of a streamed body when the buffer runs dry. It reports how many bytes it queued
// and false if the body's source failed (the stream must then be reset). When it
// queues the last frame, st.fin is set.
func (cs *conn) sendBody(st *stream, limit int) (sent int, ok bool) {
	for !st.fin && limit > 0 && len(cs.out) < outHigh {
		if len(st.resp) == 0 && st.src != nil && !cs.refill(st) {
			return sent, false
		}
		if len(st.resp) == 0 { // a streamed body that ended exactly at a refill boundary, or has no data left
			var at int
			cs.out, at = beginFrame(cs.out, frameData, flagEndStream, st.id)
			endFrame(cs.out, at)
			st.fin = true
			return sent, true
		}
		n := min(len(st.resp), cs.maxFrame, limit)
		if w := min(st.sendWin, cs.sendWin); w < int64(n) {
			if w <= 0 {
				return sent, true
			}
			n = int(w)
		}
		var flags byte
		if n == len(st.resp) && st.src == nil {
			flags, st.fin = flagEndStream, true
		}
		var at int
		cs.out, at = beginFrame(cs.out, frameData, flags, st.id)
		cs.out = append(cs.out, st.resp[:n]...)
		endFrame(cs.out, at)
		st.resp = st.resp[n:]
		st.sendWin -= int64(n)
		cs.sendWin -= int64(n)
		limit -= n
		sent += n
	}
	return sent, true
}

// refill reads the next piece of st's streamed body into st.resp. The source is
// closed and cleared when the last piece is in hand; false means it failed or
// ended before the length it promised.
func (cs *conn) refill(st *stream) bool {
	if cap(st.respBuf) < srcBuf {
		st.respBuf = make([]byte, srcBuf)
	}
	buf := st.respBuf[:cap(st.respBuf)]
	if st.srcLeft >= 0 && int64(len(buf)) > st.srcLeft {
		buf = buf[:st.srcLeft]
	}
	var n int
	var err error
	for tries := 0; n == 0 && err == nil && tries < 8; tries++ {
		n, err = st.src.Read(buf)
	}
	if n == 0 && err == nil {
		err = io.ErrNoProgress
	}
	end := err != nil
	if st.srcLeft >= 0 {
		st.srcLeft -= int64(n)
		if st.srcLeft == 0 {
			end, err = true, nil
		}
	}
	if err != nil && (err != io.EOF || st.srcLeft > 0) {
		return false
	}
	st.resp = buf[:n]
	if end {
		st.closeSource()
	}
	return true
}

// abort resets a stream whose response cannot be completed.
func (s *Server) abort(cs *conn, st *stream) {
	s.streamErr(cs, st, errInternal)
}

// produce sends queued response bodies, round-robin in quantum-sized slices,
// until they are done, blocked on a window, or enough output is queued.
func (s *Server) produce(cs *conn, g *gina.Ctx) {
	s.pumpTunnels(cs, g)
	for progress := len(cs.sendq) > 0; progress && len(cs.out) < outHigh; {
		progress = false
		for i := 0; i < len(cs.sendq) && len(cs.out) < outHigh; {
			st := cs.sendq[i]
			if st.closed { // reset by the peer while queued
				cs.dequeue(i)
				cs.release(st)
				continue
			}
			sent, ok := cs.sendBody(st, quantum)
			progress = progress || sent > 0 || st.fin
			switch {
			case !ok:
				cs.dequeue(i)
				s.abort(cs, st)
				continue
			case st.fin:
				cs.dequeue(i)
				s.finish(cs, st)
				continue
			}
			i++
		}
	}
}

func (cs *conn) dequeue(i int) {
	cs.sendq[i].queued = false
	copy(cs.sendq[i:], cs.sendq[i+1:])
	cs.sendq[len(cs.sendq)-1] = nil
	cs.sendq = cs.sendq[:len(cs.sendq)-1]
}

// finish ends a stream whose response has been queued completely. If the request
// is still arriving the peer is told to stop sending it (§8.1, RST_STREAM with
// NO_ERROR after a complete response).
func (s *Server) finish(cs *conn, st *stream) {
	if !st.recvEnded {
		cs.out = appendRST(cs.out, st.id, errNo)
	}
	cs.drop(st)
}
