package gina

import "sync/atomic"

const cacheLine = 128 // two cache lines: also keeps the adjacent-line prefetcher from pairing the cursors

// ring is a single-producer single-consumer ring of 128-byte messages, one per
// ordered shard pair (Tina's cross-shard channel). Each side owns one cache-line
// pair holding its atomic cursor and a cached copy of the other side's cursor, so
// the hot path touches no shared line. The producer stages slots with plain
// stores and publishes a whole tick's batch with ONE atomic store; the consumer
// does one atomic load to see it and one atomic store to give the slots back.
// Go's atomics are sequentially consistent, which is stronger than the
// release/acquire pair this needs.
//
// Only the producing shard's thread may call stage/publish/producerPending and
// only the consuming shard's thread may call peek/advance/commit/discard/
// consumerPending.
type ring struct {
	// producer side
	write      atomic.Uint64 // published cursor (producer stores, consumer loads)
	staged     uint64        // next slot to write (not yet visible)
	published  uint64        // producer's copy of what write holds
	cachedRead uint64        // last value of read the producer saw
	_          [cacheLine - 32]byte

	// consumer side
	read        atomic.Uint64 // slots the consumer has finished with
	localRead   uint64        // next slot to read
	cachedWrite uint64        // last value of write the consumer saw
	_           [cacheLine - 24]byte

	// cold, read-only after construction
	buf  []Message
	att  []any // att[i] is the large-message buffer of buf[i], if it has one
	mask uint64
}

func newRing(n int) *ring {
	return &ring{buf: make([]Message, n), att: make([]any, n), mask: uint64(n - 1)}
}

// stage writes m into the next slot; false means the ring is full (the sender
// learns immediately, nothing overflows).
func (r *ring) stage(m *Message, att any) bool {
	if r.staged-r.cachedRead >= uint64(len(r.buf)) {
		r.cachedRead = r.read.Load() // only now touch the consumer's line
		if r.staged-r.cachedRead >= uint64(len(r.buf)) {
			return false
		}
	}
	r.buf[r.staged&r.mask] = *m
	r.att[r.staged&r.mask] = att // visible to the consumer through the same publish as the message
	r.staged++
	return true
}

// publish makes every staged message visible to the consumer and reports
// whether there was anything new.
func (r *ring) publish() bool {
	if r.published == r.staged {
		return false
	}
	r.write.Store(r.staged)
	r.published = r.staged
	return true
}

func (r *ring) producerPending() bool { return r.published != r.staged }

// peek returns the next published message without consuming it.
func (r *ring) peek() (*Message, any, bool) {
	if r.localRead == r.cachedWrite {
		r.cachedWrite = r.write.Load() // only now touch the producer's line
		if r.localRead == r.cachedWrite {
			return nil, nil, false
		}
	}
	i := r.localRead & r.mask
	return &r.buf[i], r.att[i], true
}

// advance consumes the message peek returned; its buffer reference is dropped
// here, before commit hands the slot back to the producer.
func (r *ring) advance() {
	r.att[r.localRead&r.mask] = nil
	r.localRead++
}

// commit returns the consumed slots to the producer (once per drain).
func (r *ring) commit() { r.read.Store(r.localRead) }

// discard drops everything published so far (a quarantined shard).
func (r *ring) discard() {
	end := r.write.Load()
	for i := r.localRead; i != end; i++ {
		r.att[i&r.mask] = nil
	}
	r.localRead = end
	r.cachedWrite = r.localRead
	r.commit()
}

// consumerPending reports whether published messages are waiting. It always
// reads the shared cursor, so it is safe to use after announcing sleep.
func (r *ring) consumerPending() bool { return r.localRead != r.write.Load() }
