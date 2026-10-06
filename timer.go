package gina

// timerHeap is a fixed-capacity indexed min-heap ordered by (due, seq), so timers
// with equal deadlines fire in registration order (deterministic) and any timer
// can be cancelled in O(log n).
type timerEntry struct {
	due, seq uint64
	target   Handle
	tag      Tag
	op       int32 // >= 0: this is an I/O timeout for reactor op `op` (incarnation opSeq)
	opSeq    uint32
	pos      int32 // position in the heap, -1 when free
}

type timerHeap struct {
	ents []timerEntry
	heap []int32 // entry indices
	free []int32
	seq  uint64
}

func newTimerHeap(capacity int) timerHeap {
	h := timerHeap{ents: make([]timerEntry, capacity), heap: make([]int32, 0, capacity), free: make([]int32, 0, capacity)}
	h.clear()
	return h
}

func (h *timerHeap) clear() {
	h.heap = h.heap[:0]
	h.free = h.free[:0]
	for i := len(h.ents) - 1; i >= 0; i-- {
		h.ents[i].pos = -1
		h.free = append(h.free, int32(i))
	}
}

func (h *timerHeap) less(i, j int) bool {
	a, b := &h.ents[h.heap[i]], &h.ents[h.heap[j]]
	if a.due != b.due {
		return a.due < b.due
	}
	return a.seq < b.seq
}

func (h *timerHeap) swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
	h.ents[h.heap[i]].pos = int32(i)
	h.ents[h.heap[j]].pos = int32(j)
}

func (h *timerHeap) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(i, p) {
			return
		}
		h.swap(i, p)
		i = p
	}
}

func (h *timerHeap) down(i int) {
	n := len(h.heap)
	for {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && h.less(l, m) {
			m = l
		}
		if r < n && h.less(r, m) {
			m = r
		}
		if m == i {
			return
		}
		h.swap(i, m)
		i = m
	}
}

// push adds a timer and returns its id, or 0 when the heap is full.
func (h *timerHeap) push(e timerEntry) TimerID {
	n := len(h.free)
	if n == 0 {
		return 0
	}
	idx := h.free[n-1]
	h.free = h.free[:n-1]
	h.seq++
	e.seq = h.seq
	e.pos = int32(len(h.heap))
	h.ents[idx] = e
	h.heap = append(h.heap, idx)
	h.up(len(h.heap) - 1)
	return TimerID(uint64(idx+1)<<32 | uint64(uint32(e.seq)))
}

func (h *timerHeap) remove(pos int) {
	idx := h.heap[pos]
	last := len(h.heap) - 1
	if pos != last {
		h.swap(pos, last)
	}
	h.heap = h.heap[:last]
	if pos != last {
		h.down(pos)
		h.up(pos)
	}
	h.ents[idx].pos = -1
	h.free = append(h.free, idx)
}

// cancel removes a pending timer; it reports false if it already fired.
func (h *timerHeap) cancel(id TimerID) bool {
	idx := int(id>>32) - 1
	if idx < 0 || idx >= len(h.ents) {
		return false
	}
	e := &h.ents[idx]
	if e.pos < 0 || uint32(e.seq) != uint32(id) {
		return false
	}
	h.remove(int(e.pos))
	return true
}

func (h *timerHeap) peek() (uint64, bool) {
	if len(h.heap) == 0 {
		return 0, false
	}
	return h.ents[h.heap[0]].due, true
}

func (h *timerHeap) pop() timerEntry {
	idx := h.heap[0]
	e := h.ents[idx]
	h.remove(0)
	return e
}
