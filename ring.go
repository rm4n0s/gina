package gina

// ring is a single-producer single-consumer ring of 128-byte messages, one per
// ordered shard pair. The producer stages writes and publishes a whole tick's
// batch at once (mirrors the one-store-release-per-tick design in SPEC §6.1);
// the consumer only sees published slots. This MVP runs on one thread so no
// atomics are needed, but the discipline is the same one a shared-memory ring
// would use.
type ring struct {
	buf    []Message
	mask   uint64
	head   uint64 // consumer cursor
	tail   uint64 // published cursor
	staged uint64 // producer cursor
}

func newRing(n int) *ring { return &ring{buf: make([]Message, n), mask: uint64(n - 1)} }

func (r *ring) stage(m *Message) bool {
	if r.staged-r.head >= uint64(len(r.buf)) {
		return false
	}
	r.buf[r.staged&r.mask] = *m
	r.staged++
	return true
}

func (r *ring) publish() { r.tail = r.staged }

func (r *ring) peek() (*Message, bool) {
	if r.head == r.tail {
		return nil, false
	}
	return &r.buf[r.head&r.mask], true
}

func (r *ring) advance() { r.head++ }

func (r *ring) pending() bool { return r.head != r.tail || r.staged != r.tail }
