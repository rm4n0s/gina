package http2

import (
	"time"

	"gina"
	ghttp "gina/extensions/http"
)

// Tunnels over HTTP/2: a stream opened by an extended CONNECT (RFC 8441) and
// accepted with a 2xx answer carries an http.Tunnel. Its DATA frames in are the
// tunnel's input; what the tunnel queues goes out as DATA frames, within the
// peer's flow-control windows. The stream ends when the tunnel is Done and
// drained, when the peer ends or resets it, or when the connection goes.

// openTunnel answers an extended CONNECT with res (a 2xx) and hands the stream to
// res.Tunnel. The response has no body and does not end the stream.
func (s *Server) openTunnel(cs *conn, g *gina.Ctx, st *stream, res ghttp.Result) {
	blk := appendStatus(cs.hblk[:0], res.Status)
	blk = appendLiteral(blk, []byte("date"), s.sh(g).date())
	blk = appendLiteral(blk, []byte("server"), []byte("gina"))
	blk = cs.appendExtra(blk, res, false)
	cs.hblk = blk[:0]
	cs.writeHeaders(st.id, blk, false)
	st.tun = res.Tunnel
	cs.tuns = append(cs.tuns, st)
	st.tun.Open(g, st.id)
}

// untunnel removes st from the connection's tunnel list and tells its tunnel.
func (cs *conn) untunnel(st *stream) {
	for i, x := range cs.tuns {
		if x == st {
			copy(cs.tuns[i:], cs.tuns[i+1:])
			cs.tuns[len(cs.tuns)-1] = nil
			cs.tuns = cs.tuns[:len(cs.tuns)-1]
			break
		}
	}
	t := st.tun
	st.tun = nil
	if !st.tunClosed {
		st.tunClosed = true
		t.Closed(cs.g)
	}
}

// closeTunnel tells the tunnel its peer has gone; the stream is ended by
// pumpTunnels once whatever the tunnel still queued has been written.
func (s *Server) closeTunnel(cs *conn, g *gina.Ctx, st *stream) {
	if !st.tunClosed {
		st.tunClosed = true
		st.tun.Closed(g)
	}
}

// pumpTunnels writes tunnel output as DATA frames, as far as the windows, the
// peer's frame size and the output high-water mark allow, and ends the streams of
// tunnels that are finished.
func (s *Server) pumpTunnels(cs *conn, g *gina.Ctx) {
	for i := 0; i < len(cs.tuns); {
		st := cs.tuns[i]
		t := st.tun
		sent := 0
		for len(cs.out) < outHigh && sent < quantum {
			out := t.Outgoing()
			if len(out) == 0 {
				break
			}
			n := min(len(out), cs.maxFrame, quantum-sent)
			w := min(st.sendWin, cs.sendWin)
			if w <= 0 {
				break // blocked on the peer's window: a WINDOW_UPDATE will bring us back
			}
			n = int(min(int64(n), w))
			var at int
			cs.out, at = beginFrame(cs.out, frameData, 0, st.id)
			cs.out = append(cs.out, out[:n]...)
			endFrame(cs.out, at)
			t.Sent(n)
			st.sendWin -= int64(n)
			cs.sendWin -= int64(n)
			sent += n
		}
		if (t.Done() || st.tunClosed) && len(t.Outgoing()) == 0 {
			var at int
			cs.out, at = beginFrame(cs.out, frameData, flagEndStream, st.id)
			endFrame(cs.out, at)
			s.finish(cs, st) // drops the stream, which removes it from cs.tuns
			continue
		}
		i++
	}
}

// tunPending reports whether a tunnel has output the peer's window is holding back.
func (cs *conn) tunPending() bool {
	for _, st := range cs.tuns {
		if len(st.tun.Outgoing()) > 0 {
			return true
		}
	}
	return false
}

// tunIdle is the soonest any tunnel wants a Tick.
func (cs *conn) tunIdle(now uint64) (min time.Duration, ok bool) {
	for _, st := range cs.tuns {
		if d, has := st.tun.Idle(now); has && (!ok || d < min) {
			min, ok = d, true
		}
	}
	return
}

func (s *Server) tickTunnels(cs *conn, g *gina.Ctx) {
	for i := 0; i < len(cs.tuns); i++ { // a tick may not end a stream, but be safe against it
		cs.tuns[i].tun.Tick(g)
	}
}

// shutdownTunnels tells every tunnel the server is stopping and queues what they
// answer, so it goes out before the connection closes.
func (s *Server) shutdownTunnels(cs *conn, g *gina.Ctx) {
	if len(cs.tuns) == 0 {
		return
	}
	for _, st := range cs.tuns {
		st.tun.Shutdown(g)
	}
	s.pumpTunnels(cs, g)
}
