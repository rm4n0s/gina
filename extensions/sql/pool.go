package sql

// The pool isolate: the part of database/sql's DB that decides which connection
// runs a request. It never touches a socket. A request arrives as a message; the
// pool gives it to an idle connection, or queues it and opens a connection if the
// limit allows; connections tell the pool when they are logged in, free again, or
// gone. Replies go from the connection straight to the caller.

import (
	"encoding/binary"
	"time"

	"github.com/rm4n0s/gina"
)

type lease struct {
	owner   gina.Handle // who sent the request: may cancel, continue and use a transaction
	replyTo gina.Handle
	id      uint32
	w       *waiter // kept so a request the connection never started can run elsewhere
}

type pconn struct {
	ready bool   // logged in
	idle  bool   // in the idle list
	lease *lease // the request or transaction it serves
}

// waiter is a request that has not found a connection yet.
type waiter struct {
	owner, replyTo gina.Handle
	id             uint32
	op             opCode
	data           []byte
	deadline       uint64 // Ctx.Now; 0 = none
}

type pool struct {
	conns    map[gina.Handle]*pconn
	idle     []gina.Handle // most recently freed last
	queue    []*waiter
	opening  int // connections not yet logged in
	tickLive bool
}

func (d *DB) poolInit(p *pool, g *gina.Ctx, _ []byte) gina.Effect {
	p.conns = map[gina.Handle]*pconn{}
	d.pool.Store(uint64(g.Self())) // after a restart the handle has a new generation
	return gina.WaitMessage()
}

func (d *DB) poolHandler(p *pool, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case TagRequest:
		p.request(d, g, m)
	case TagCancel:
		p.cancel(d, g, m)
	case tagReady:
		if pc := p.conns[m.Source]; pc != nil && !pc.ready {
			pc.ready = true
			p.opening--
			d.opened.Add(1)
			p.release(d, g, m.Source, pc)
		}
	case tagFree:
		if pc := p.conns[m.Source]; pc != nil {
			l := pc.lease
			pc.lease = nil
			if m.PayloadSize > 0 && m.Payload[0] == 1 {
				p.unidle(m.Source)
				p.forget(d, m.Source, pc)
				if l != nil && m.PayloadSize > 1 && m.Payload[1] == 1 {
					// It died before it began the request it was given (it was idle when the
					// server hung up): nothing has happened yet, so run it on another.
					p.queue = append([]*waiter{l.w}, p.queue...)
					d.requeued.Add(1)
				}
			} else {
				p.release(d, g, m.Source, pc)
			}
		}
	case tagRetire:
		// An idle connection asks to go. If it was leased in the meantime the request
		// is already on its way to it, and it will ask again when that is done.
		if pc := p.conns[m.Source]; pc != nil && pc.idle {
			p.unidle(m.Source)
			p.forget(d, m.Source, pc)
			g.SendRaw(m.Source, tagClose, nil)
		}
	case tagFailed:
		if pc := p.conns[m.Source]; pc != nil {
			p.forget(d, m.Source, pc)
			d.failed.Add(1)
			p.failOldest(g, g.Data(), "")
		}
	case gina.TagChildExit:
		ce := gina.PayloadAs[gina.ChildExit](m)
		if pc := p.conns[ce.Old]; pc != nil { // it died without saying so
			p.unidle(ce.Old)
			p.forget(d, ce.Old, pc)
			if pc.lease != nil {
				reply(g, pc.lease.replyTo, pc.lease.id, encodeError(clientErr(ClientConnLost, "the connection stopped unexpectedly")))
			} else if !pc.ready {
				d.failed.Add(1)
				p.failOldest(g, nil, "the connection stopped unexpectedly")
			}
		}
	case tagTimerQueue:
		p.tickLive = false
		p.expire(d, g)
	case gina.TagShutdown:
		return gina.Done()
	}
	p.open(d, g)
	p.sync(d)
	return gina.WaitMessage()
}

// reply sends a terminal reply for request id to whoever waits for it.
func reply(g *gina.Ctx, to gina.Handle, id uint32, payload []byte) {
	if to != 0 {
		g.SendCorr(to, TagReply, id, payload)
	}
}

func (p *pool) request(d *DB, g *gina.Ctx, m *gina.Message) {
	data := g.Data()
	q, err := parseRequest(data)
	replyTo := q.replyTo
	if replyTo == 0 {
		replyTo = m.Source
	}
	if err != nil || (q.op != opQuery && q.op != opExec && q.op != opBegin) || q.id != m.Correlation {
		reply(g, replyTo, m.Correlation, encodeError(clientErr(ClientBadRequest, "malformed request")))
		return
	}
	w := &waiter{owner: m.Source, replyTo: replyTo, id: q.id, op: q.op, data: append([]byte(nil), data...)}
	if p.dispatch(d, g, w) {
		return
	}
	if len(p.queue) >= d.cfg.Queue {
		reply(g, replyTo, q.id, encodeError(clientErr(ClientOverloaded, "too many requests are waiting for a connection")))
		return
	}
	if d.cfg.QueueTimeout > 0 {
		w.deadline = g.Now() + uint64(d.cfg.QueueTimeout)
		if !p.tickLive {
			p.tickLive = g.RegisterTimer(min(d.cfg.QueueTimeout, time.Second), tagTimerQueue) != 0
		}
	}
	d.waitCount.Add(1)
	p.queue = append(p.queue, w)
}

// dispatch gives w to an idle connection. False means there is none.
func (p *pool) dispatch(d *DB, g *gina.Ctx, w *waiter) bool {
	for len(p.idle) > 0 {
		h := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		pc := p.conns[h]
		if pc == nil {
			continue
		}
		pc.idle = false
		pc.lease = &lease{owner: w.owner, replyTo: w.replyTo, id: w.id, w: w}
		buf := make([]byte, 8, 8+len(w.data))
		binary.BigEndian.PutUint64(buf, uint64(w.owner))
		if g.SendCorr(h, tagRun, w.id, append(buf, w.data...)) == gina.SendOK {
			return true
		}
		// The connection is gone or cannot take mail: forget it and try the next.
		p.forget(d, h, pc)
		pc.lease = nil
		g.SendRaw(h, tagClose, nil)
	}
	return false
}

// release puts a connection that is free to use: the oldest waiting request gets
// it, else it goes on the idle list, or away when there are enough idle already.
func (p *pool) release(d *DB, g *gina.Ctx, h gina.Handle, pc *pconn) {
	for len(p.queue) > 0 {
		w := p.queue[0]
		p.queue = p.queue[1:]
		pc.lease = &lease{owner: w.owner, replyTo: w.replyTo, id: w.id, w: w}
		buf := make([]byte, 8, 8+len(w.data))
		binary.BigEndian.PutUint64(buf, uint64(w.owner))
		if g.SendCorr(h, tagRun, w.id, append(buf, w.data...)) == gina.SendOK {
			return
		}
		pc.lease = nil
		reply(g, w.replyTo, w.id, encodeError(clientErr(ClientConnLost, "the connection could not take the request")))
		p.forget(d, h, pc)
		g.SendRaw(h, tagClose, nil)
		return
	}
	if len(p.idle) >= d.cfg.MaxIdleConns {
		p.forget(d, h, pc)
		g.SendRaw(h, tagClose, nil)
		return
	}
	pc.idle = true
	p.idle = append(p.idle, h)
}

// forget drops a connection from the books.
func (p *pool) forget(d *DB, h gina.Handle, pc *pconn) {
	if _, ok := p.conns[h]; !ok {
		return
	}
	delete(p.conns, h)
	if !pc.ready {
		p.opening--
	}
	d.closed.Add(1)
}

func (p *pool) unidle(h gina.Handle) {
	for i, x := range p.idle {
		if x == h {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			return
		}
	}
}

// open starts connections for the requests that wait, within MaxOpenConns.
func (p *pool) open(d *DB, g *gina.Ctx) {
	for len(p.queue) > p.opening && len(p.conns) < d.cfg.MaxOpenConns {
		h, err := g.Spawn(gina.SpawnSpec{Type: d.connType(), Group: gina.GroupNone, Restart: gina.RestartTemporary})
		if err != gina.SpawnErrNone {
			return
		}
		p.conns[h] = &pconn{}
		p.opening++
		if g.SendRaw(h, tagOpen, nil) != gina.SendOK {
			g.SendRaw(h, gina.TagShutdown, nil)
			return
		}
	}
}

// failOldest answers the request that has waited longest with a connection error.
// A connect failure is the pool's to report because the caller never had a
// connection to hear it from.
func (p *pool) failOldest(g *gina.Ctx, encoded []byte, msg string) {
	if len(p.queue) == 0 {
		return
	}
	w := p.queue[0]
	p.queue = p.queue[1:]
	if encoded == nil {
		encoded = encodeError(clientErr(ClientConnect, msg))
	}
	reply(g, w.replyTo, w.id, encoded)
}

// expire answers the queued requests whose time has run out and keeps the timer
// going while any remain.
func (p *pool) expire(d *DB, g *gina.Ctx) {
	now := g.Now()
	kept := p.queue[:0]
	for _, w := range p.queue {
		if w.deadline != 0 && now >= w.deadline {
			d.timedOut.Add(1)
			reply(g, w.replyTo, w.id, encodeError(clientErr(ClientTimeout, "no connection became free in time")))
			continue
		}
		kept = append(kept, w)
	}
	clear(p.queue[len(kept):])
	p.queue = kept
	if len(p.queue) > 0 && d.cfg.QueueTimeout > 0 {
		p.tickLive = g.RegisterTimer(min(d.cfg.QueueTimeout, time.Second), tagTimerQueue) != 0
	}
}

// cancel handles DB.Cancel: a request still in the queue is dropped; one that a
// connection is running is passed on to it.
func (p *pool) cancel(d *DB, g *gina.Ctx, m *gina.Message) {
	id := m.Correlation
	for i, w := range p.queue {
		if w.id == id && (w.owner == m.Source || w.replyTo == m.Source) {
			p.queue = append(p.queue[:i], p.queue[i+1:]...)
			reply(g, w.replyTo, w.id, encodeError(clientErr(ClientCanceled, "canceled while waiting for a connection")))
			return
		}
	}
	for h, pc := range p.conns {
		if l := pc.lease; l != nil && l.id == id && (l.owner == m.Source || l.replyTo == m.Source) {
			buf := binary.BigEndian.AppendUint64(nil, uint64(l.owner))
			g.SendCorr(h, tagCancelRun, id, buf)
			return
		}
	}
}

// sync publishes the counts for DB.Stats.
func (p *pool) sync(d *DB) {
	d.open.Store(int64(len(p.conns)))
	d.idle.Store(int64(len(p.idle)))
	d.inUse.Store(int64(len(p.conns) - len(p.idle) - p.opening))
	d.waiting.Store(int64(len(p.queue)))
}
