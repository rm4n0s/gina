package gina

import (
	"sync"
	"sync/atomic"
)

type atomicInt32 = atomic.Int32

// External injection: the one way for code that is not an isolate (a goroutine
// running a webhook, a signal handler, a job runner) to reach a running system.
//
// Each shard has an inbox that any number of threads may append to under a
// mutex. The shard looks at it once per tick through a single atomic counter, so
// a system nobody injects into pays one load of an unshared cache line per tick.
// The shard drains the inbox itself and allocates the envelopes from its own
// pool, so nothing else about the engine becomes concurrent.

type extMsg struct {
	to  Handle
	m   Message
	att any
}

type extInbox struct {
	n     paddedInt32 // queued messages; the shard reads it lock-free
	mu    sync.Mutex
	q     []extMsg
	spare []extMsg
}

// paddedInt32 keeps the counter on its own cache lines: producers write it, and
// the shard's hot fields must not share the line.
type paddedInt32 struct {
	_ [cacheLine]byte
	v atomicInt32
	_ [cacheLine - 4]byte
}

// SendExternal delivers a message to an isolate from any goroutine, while the
// system runs or not. It never blocks on the shard and wakes it if it sleeps.
//
// The result reports what the caller can know at once: SendOK means the message
// was queued for the shard (it can still be dropped there if the mailbox is full
// or the handle went stale, which counts in Stats().Dropped, as for messages from
// other shards). SendRingFull means the shard's external queue (PoolSlots long)
// is full; SendStaleHandle means the shard does not exist or is quarantined.
// Messages from one goroutine to one shard arrive in order.
func (sys *System) SendExternal(to Handle, tag Tag, payload []byte) SendResult {
	if len(payload) > sys.spec.MaxMessageBytes {
		return SendPayloadTooLarge
	}
	if int(to.Shard()) >= len(sys.shards) {
		return SendStaleHandle
	}
	sh := sys.shards[to.Shard()]
	if sh.quarantined.Load() {
		return SendStaleHandle
	}
	e := extMsg{to: to}
	e.m.Tag = tag
	if len(payload) > MaxPayload {
		e.m.Flags, e.m.PayloadSize = FlagLarge, MaxPayload
		copy(e.m.Payload[:], payload)
		e.att = &Blob{b: append([]byte(nil), payload...)}
	} else {
		e.m.PayloadSize = uint16(copy(e.m.Payload[:], payload))
	}
	ib := &sh.ext
	ib.mu.Lock()
	if len(ib.q) >= len(sh.pool) {
		ib.mu.Unlock()
		return SendRingFull
	}
	ib.q = append(ib.q, e)
	ib.n.v.Store(int32(len(ib.q)))
	ib.mu.Unlock()
	if sys.run == nil {
		sh.io.wake() // cooperative driver: it may be blocked in the master epoll, which watches this eventfd
	} else {
		sh.wake() // after the counter is visible; pairs with the sleeper's announce-then-recheck
	}
	return SendOK
}

// SendExternalTo is SendExternal for a typed payload.
func SendExternalTo[P any](sys *System, to Handle, tag Tag, p *P) SendResult {
	checkPOD[P](MaxPayload)
	return sys.SendExternal(to, tag, BytesOf(p))
}

func (s *Shard) externalPending() bool { return s.ext.n.v.Load() != 0 }

// drainExternal moves the inbox into mailboxes (tick phase 1).
func (s *Shard) drainExternal() bool {
	ib := &s.ext
	ib.mu.Lock()
	q := ib.q
	ib.q, ib.spare = ib.spare[:0], nil
	ib.n.v.Store(0)
	ib.mu.Unlock()
	for i := range q {
		e := &q[i]
		if s.enqueue(e.to, &e.m, e.att, false) != SendOK {
			s.stats.Dropped++
		}
		*e = extMsg{}
	}
	ib.mu.Lock()
	ib.spare = q[:0]
	ib.mu.Unlock()
	return len(q) > 0
}

// discardExternal drops the inbox (a quarantined shard).
func (s *Shard) discardExternal() {
	ib := &s.ext
	ib.mu.Lock()
	clear(ib.q)
	ib.q = ib.q[:0]
	ib.n.v.Store(0)
	ib.mu.Unlock()
}
