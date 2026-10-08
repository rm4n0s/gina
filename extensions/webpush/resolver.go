package webpush

// Name resolution as isolates, because Gina's reactor connects to addresses, not
// names. Two types:
//
//   - resolver: one per WebPush. Keeps a cache, merges concurrent requests for the
//     same name, and starts a lookup for each name it does not know.
//   - lookup: one per name being resolved. Owns a UDP socket, asks the configured
//     nameservers for A and AAAA records, reports to the resolver and exits.
//
// Deliveries ask the resolver with tagResolve and get tagResolved back; an empty
// answer means the name could not be resolved.

import (
	"crypto/rand"
	"encoding/binary"
	"net/netip"
	"strings"
	"time"

	"github.com/rm4n0s/gina"
)

const (
	tagResolve  gina.Tag = gina.TagUserBase + 0x30 // delivery -> resolver: the host name; Correlation comes back
	tagResolved gina.Tag = gina.TagUserBase + 0x31 // resolver -> delivery: encodeAddrs, empty on failure
	tagLookup   gina.Tag = gina.TagUserBase + 0x32 // resolver -> lookup: the host name
	tagLookedUp gina.Tag = gina.TagUserBase + 0x33 // lookup -> resolver: ttl (u32), then encodeAddrs

	dnsRounds   = 2 // passes over the nameserver list
	minCacheTTL = 10 * time.Second
	maxCacheTTL = 5 * time.Minute
	maxCached   = 256
)

type cached struct {
	addrs   []netip.Addr
	expires uint64 // Ctx.Now
}

type waiter struct {
	to   gina.Handle
	corr uint32
}

type pendingLookup struct {
	child   gina.Handle
	waiters []waiter
}

type resolver struct {
	cache   map[string]cached
	pending map[string]*pendingLookup
	byChild map[gina.Handle]string
}

func (w *WebPush) resolverInit(r *resolver, g *gina.Ctx, _ []byte) gina.Effect {
	r.cache = map[string]cached{}
	r.pending = map[string]*pendingLookup{}
	r.byChild = map[gina.Handle]string{}
	w.resolver.Store(uint64(g.Self())) // after a restart the handle has a new generation
	return gina.WaitMessage()
}

func (w *WebPush) resolverHandler(r *resolver, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagResolve:
		name := strings.ToLower(string(g.Data()))
		to := waiter{m.Source, m.Correlation}
		if addrs, ok := w.cfg.Hosts[name]; ok {
			answerTo(g, to, addrs)
		} else if c, ok := r.cache[name]; ok && g.Now() < c.expires {
			answerTo(g, to, c.addrs)
		} else if p := r.pending[name]; p != nil {
			p.waiters = append(p.waiters, to)
		} else {
			r.startLookup(w, g, name, to)
		}
	case tagLookedUp:
		name, ok := r.byChild[m.Source]
		d := g.Data()
		if !ok || len(d) < 4 {
			break
		}
		delete(r.byChild, m.Source)
		ttl := time.Duration(binary.BigEndian.Uint32(d)) * time.Second
		addrs := decodeAddrs(d[4:])
		if len(addrs) > 0 {
			if len(r.cache) >= maxCached {
				clear(r.cache) // crude, and a cache this size only matters to a few push services
			}
			r.cache[name] = cached{addrs, g.Now() + uint64(min(max(ttl, minCacheTTL), maxCacheTTL))}
		}
		r.finish(g, name, addrs)
	case gina.TagChildExit:
		ce := gina.PayloadAs[gina.ChildExit](m)
		if name, ok := r.byChild[ce.Old]; ok { // it ended without reporting
			delete(r.byChild, ce.Old)
			r.finish(g, name, nil)
		}
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

func (r *resolver) startLookup(w *WebPush, g *gina.Ctx, name string, first waiter) {
	self := g.Self()
	sp := gina.WithArgs(gina.SpawnSpec{Type: w.cfg.TypeID + typeLookupOffset, Group: gina.GroupNone, Restart: gina.RestartTemporary}, &self)
	child, err := g.Spawn(sp)
	if err != gina.SpawnErrNone {
		answerFail(g, first)
		return
	}
	r.pending[name] = &pendingLookup{child: child, waiters: []waiter{first}}
	r.byChild[child] = name
	if g.SendRaw(child, tagLookup, []byte(name)) != gina.SendOK {
		r.finish(g, name, nil)
	}
}

// finish answers everyone waiting for name.
func (r *resolver) finish(g *gina.Ctx, name string, addrs []netip.Addr) {
	p := r.pending[name]
	if p == nil {
		return
	}
	delete(r.pending, name)
	delete(r.byChild, p.child)
	for _, wt := range p.waiters {
		if len(addrs) == 0 {
			answerFail(g, wt)
		} else {
			answerTo(g, wt, addrs)
		}
	}
}

func answerTo(g *gina.Ctx, to waiter, addrs []netip.Addr) {
	g.SendCorr(to.to, tagResolved, to.corr, encodeAddrs(nil, addrs))
}

func answerFail(g *gina.Ctx, to waiter) { g.SendCorr(to.to, tagResolved, to.corr, nil) }

// ---- the lookup isolate: DNS over UDP ----

const (
	lkSendA uint8 = iota + 1
	lkSend6
	lkRecv
)

type lookupIso struct {
	parent    gina.Handle
	name      string
	round, si int
	fd        gina.FDHandle
	phase     uint8
	idA, id6  uint16
	gotA      bool
	got6      bool
	addrs     []netip.Addr
	ttl       uint32
	deadline  uint64
	qA, q6    []byte
	buf       []byte
}

func (w *WebPush) lookupInit(l *lookupIso, _ *gina.Ctx, args []byte) gina.Effect {
	l.parent = gina.ArgsAs[gina.Handle](args)
	return gina.WaitMessage()
}

func (w *WebPush) lookupHandler(l *lookupIso, g *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagLookup:
		l.name = string(g.Data())
		l.ttl = ^uint32(0)
		l.buf = make([]byte, 1500)
		return l.nextServer(w, g)
	case gina.TagIOSend:
		if gina.PayloadAs[gina.IOResult](m).Result < 0 {
			return l.nextServer(w, g)
		}
		if l.phase == lkSendA {
			l.phase = lkSend6
			g.IOSend(l.fd, l.q6, 0)
			return gina.WaitIO()
		}
		return l.recv(w, g)
	case gina.TagIORecv:
		r := gina.PayloadAs[gina.IOResult](m).Result
		switch {
		case r < 0: // timeout, or the server's port is closed
			if l.gotA || l.got6 {
				return l.finish(g)
			}
			return l.nextServer(w, g)
		case r == 0:
			return l.recv(w, g)
		}
		return l.onDatagram(w, g, l.buf[:r])
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// nextServer moves on to the next nameserver (or ends, when the passes are used up).
func (l *lookupIso) nextServer(w *WebPush, g *gina.Ctx) gina.Effect {
	if l.fd != 0 {
		g.CloseFD(l.fd)
		l.fd = 0
	}
	servers := w.cfg.Resolvers
	for {
		if l.round >= dnsRounds {
			l.addrs = nil
			return l.finish(g)
		}
		if l.si >= len(servers) {
			l.si, l.round = 0, l.round+1
			continue
		}
		srv := servers[l.si]
		l.si++
		fd, err := g.Dial(gina.DialSpec{IP: srv.Addr(), Port: srv.Port(), UDP: true})
		if err != nil {
			continue
		}
		l.fd = fd
		l.gotA, l.got6, l.addrs, l.ttl = false, false, nil, ^uint32(0)
		var id [4]byte
		rand.Read(id[:])
		l.idA, l.id6 = binary.BigEndian.Uint16(id[:2]), binary.BigEndian.Uint16(id[2:])
		var e1, e2 error
		l.qA, e1 = dnsQuery(l.idA, l.name, dnsA)
		l.q6, e2 = dnsQuery(l.id6, l.name, dnsAAAA)
		if e1 != nil || e2 != nil { // a name that cannot be asked for cannot be resolved
			l.round = dnsRounds
			continue
		}
		l.deadline = g.Now() + uint64(w.cfg.DNSTimeout)
		l.phase = lkSendA
		g.IOSend(fd, l.qA, 0)
		return gina.WaitIO()
	}
}

func (l *lookupIso) recv(w *WebPush, g *gina.Ctx) gina.Effect {
	rem := int64(l.deadline) - int64(g.Now())
	if rem <= 0 {
		if l.gotA || l.got6 {
			return l.finish(g)
		}
		return l.nextServer(w, g)
	}
	l.phase = lkRecv
	g.IORecv(l.fd, l.buf, time.Duration(rem))
	return gina.WaitIO()
}

func (l *lookupIso) onDatagram(w *WebPush, g *gina.Ctx, msg []byte) gina.Effect {
	qtype := uint16(dnsA)
	ans, err := dnsParse(msg, l.idA, l.name, dnsA)
	if err != nil {
		qtype = dnsAAAA
		ans, err = dnsParse(msg, l.id6, l.name, dnsAAAA)
	}
	if err != nil { // not ours (or garbage): keep waiting
		return l.recv(w, g)
	}
	switch {
	case ans.rcode == 3: // NXDOMAIN: the name does not exist, asking again will not change that
		l.addrs = nil
		return l.finish(g)
	case ans.rcode != 0, ans.truncated && len(ans.addrs) == 0: // SERVFAIL, REFUSED, ...: another server may do better
		return l.nextServer(w, g)
	}
	if qtype == dnsA {
		l.gotA = true
	} else {
		l.got6 = true
	}
	l.addrs = append(l.addrs, ans.addrs...)
	l.ttl = min(l.ttl, ans.ttl)
	if l.gotA && l.got6 {
		return l.finish(g)
	}
	return l.recv(w, g)
}

func (l *lookupIso) finish(g *gina.Ctx) gina.Effect {
	ttl := l.ttl
	if len(l.addrs) == 0 || ttl == ^uint32(0) {
		ttl = 0
	}
	msg := binary.BigEndian.AppendUint32(nil, ttl)
	msg = encodeAddrs(msg, l.addrs)
	g.SendRaw(l.parent, tagLookedUp, msg)
	return gina.Done()
}
