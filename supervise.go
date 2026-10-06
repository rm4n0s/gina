package gina

// budget allows max events within window ticks (sliding window, integer ticks).
type budget struct {
	times  []uint64
	n, pos int
	window uint64
}

func newBudget(max int, window uint64) budget {
	return budget{times: make([]uint64, max), window: window}
}

// exceeded records an event at now and reports whether it pushed the count past
// the budget. When it reports true nothing is recorded.
func (b *budget) exceeded(now uint64) bool {
	max := len(b.times)
	if max == 0 {
		return true
	}
	if b.n == max && now-b.times[b.pos] < b.window {
		return true
	}
	b.times[b.pos] = now
	b.pos = (b.pos + 1) % max
	if b.n < max {
		b.n++
	}
	return false
}

type child struct {
	typ    TypeID
	slot   uint32
	live   bool
	spawn  SpawnSpec
	parent Handle
	last   Handle // handle of the current (or most recent) incarnation
	seq    uint64
}

type group struct {
	spec     GroupSpec
	children []child // in start order, capacity MaxChildren
	seq      uint64
	b        budget
}

type childRef struct{ g, c int32 }

var noRef = childRef{-1, -1}

func newGroups(specs []GroupSpec) []group {
	gs := make([]group, len(specs))
	for i, sp := range specs {
		gs[i] = group{spec: sp, children: make([]child, 0, sp.MaxChildren), b: newBudget(sp.RestartMax, sp.WindowTicks)}
	}
	return gs
}

func (s *Shard) groupIndex(id GroupID) int32 {
	for i := range s.groups {
		if s.groups[i].spec.ID == id {
			return int32(i)
		}
	}
	return -1
}

func (s *Shard) register(gi int32, h Handle, sp *SpawnSpec, parent Handle) {
	g := &s.groups[gi]
	g.seq++
	g.children = append(g.children, child{typ: h.Type(), slot: h.Slot(), live: true, spawn: *sp, parent: parent, last: h, seq: g.seq})
	s.types[h.Type()].cref[h.Slot()] = childRef{gi, int32(len(g.children) - 1)}
}

func (s *Shard) childIndex(gi int, seq uint64) int {
	cs := s.groups[gi].children
	for k := range cs {
		if cs[k].seq == seq {
			return k
		}
	}
	return -1
}

func (s *Shard) removeChild(gi, ci int) {
	g := &s.groups[gi]
	copy(g.children[ci:], g.children[ci+1:])
	g.children = g.children[:len(g.children)-1]
	for k := ci; k < len(g.children); k++ {
		if c := &g.children[k]; c.live {
			s.types[c.typ].cref[c.slot] = childRef{int32(gi), int32(k)}
		}
	}
}

func shouldRestart(r RestartType, k ExitKind) bool {
	switch r {
	case RestartPermanent:
		return k != ExitShutdown
	case RestartTransient:
		return k == ExitCrashed
	}
	return false
}

func (s *Shard) notifyParent(parent, old, new Handle, kind ExitKind) {
	if parent == 0 || parent.Shard() != s.id {
		return
	}
	var m Message
	m.Source = old
	m.Tag = TagChildExit
	ce := ChildExit{Old: old, New: new, Kind: uint64(kind)}
	m.PayloadSize = uint16(copy(m.Payload[:], BytesOf(&ce)))
	s.enqueue(parent, &m, nil, true)
}

// terminate ends a running isolate: free its slot, then apply supervision.
func (s *Shard) terminate(t *isoType, slot uint32, kind ExitKind) {
	old := s.handleOf(t, slot)
	ref := t.cref[slot]
	parent := t.parent[slot]
	s.freeSlot(t, slot)
	if ref.g < 0 {
		s.notifyParent(parent, old, 0, kind)
		return
	}
	s.groups[ref.g].children[ref.c].live = false
	s.childExited(int(ref.g), int(ref.c), kind)
}

func (s *Shard) childExited(gi, ci int, kind ExitKind) {
	g := &s.groups[gi]
	c := &g.children[ci]
	if s.shutting || !shouldRestart(c.spawn.Restart, kind) {
		s.notifyParent(c.parent, c.last, 0, kind)
		s.removeChild(gi, ci)
		return
	}
	if g.b.exceeded(s.tick) {
		s.stats.BudgetExceeded++
		s.level2Reset()
		return
	}
	crashed := c.seq
	switch g.spec.Strategy {
	case OneForOne:
		s.restartChild(gi, crashed, kind)
	case OneForAll, RestForOne:
		lo := 0
		if g.spec.Strategy == RestForOne {
			lo = ci
		}
		seqs := make([]uint64, 0, len(g.children)-lo)
		for k := lo; k < len(g.children); k++ {
			seqs = append(seqs, g.children[k].seq)
		}
		for k := len(g.children) - 1; k >= lo; k-- { // tear down in reverse start order
			if cc := &g.children[k]; cc.live {
				cc.live = false
				s.freeSlot(s.types[cc.typ], cc.slot)
			}
		}
		epoch := s.epoch
		for _, sq := range seqs {
			if s.epoch != epoch {
				return // a Level-2 reset rebuilt everything
			}
			k := s.childIndex(gi, sq)
			if k < 0 {
				continue
			}
			if sq != crashed && g.children[k].spawn.Restart == RestartTemporary {
				s.notifyParent(g.children[k].parent, g.children[k].last, 0, ExitShutdown)
				s.removeChild(gi, k)
				continue
			}
			s.restartChild(gi, sq, kind)
		}
	}
}

func (s *Shard) restartChild(gi int, seq uint64, kind ExitKind) {
	k := s.childIndex(gi, seq)
	if k < 0 {
		return
	}
	c := s.groups[gi].children[k] // copy: the slice may shift while init runs
	sp := c.spawn
	h, eff, err := s.startIsolate(&sp, c.parent)
	k = s.childIndex(gi, seq)
	if k < 0 {
		return
	}
	cc := &s.groups[gi].children[k]
	if err != SpawnErrNone { // init failed (or no slot): counts as another crash
		cc.live = false
		s.childExited(gi, k, ExitCrashed)
		return
	}
	cc.slot, cc.last = h.Slot(), h
	if eff.Kind == effDone { // finished inside init
		cc.live = false
		s.childExited(gi, k, ExitNormal)
		return
	}
	cc.live = true
	s.types[h.Type()].cref[h.Slot()] = childRef{int32(gi), int32(k)}
	s.stats.Restarts++
	s.notifyParent(cc.parent, c.last, h, kind)
}
