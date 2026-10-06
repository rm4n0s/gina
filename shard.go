package gina

import (
	"fmt"
	"syscall"
	"time"
)

const (
	stFree uint8 = iota
	stIdle
	stReady
	stRunning
	stWaitIO // parked until an I/O completion arrives
)

// isoType is one isolate type on one shard: struct-of-arrays metadata plus the
// type-erased operations over its typed slab.
type isoType struct {
	id      TypeID
	ops     typeOps
	slots   int
	mboxCap int
	stride  int // mboxCap+1: one extra slot so an I/O completion can always be pushed to the front
	ioop    []int32
	ownfd   []FDHandle
	gen     []uint32
	state   []uint8
	mbuf    []uint32 // slots*mboxCap pool indices
	mhead   []uint32
	mlen    []uint32
	free    []uint32 // LIFO stack of free slots
	cref    []childRef
	parent  []Handle
}

func newIsoType(d TypeDesc) *isoType {
	o := d.opts
	t := &isoType{
		id: d.id, ops: d.build(o), slots: o.SlotCount, mboxCap: o.MailboxCapacity, stride: o.MailboxCapacity + 1,
		ioop: make([]int32, o.SlotCount), ownfd: make([]FDHandle, o.SlotCount),
		gen: make([]uint32, o.SlotCount), state: make([]uint8, o.SlotCount),
		mbuf:  make([]uint32, o.SlotCount*(o.MailboxCapacity+1)),
		mhead: make([]uint32, o.SlotCount), mlen: make([]uint32, o.SlotCount),
		free: make([]uint32, 0, o.SlotCount), cref: make([]childRef, o.SlotCount),
		parent: make([]Handle, o.SlotCount),
	}
	for i := range t.gen {
		t.gen[i] = 1
		t.cref[i] = noRef
		t.ioop[i] = -1
	}
	t.refill()
	return t
}

func (t *isoType) refill() {
	t.free = t.free[:0]
	for i := t.slots - 1; i >= 0; i-- {
		t.free = append(t.free, uint32(i))
	}
}

// Stats are cumulative per-shard counters.
type Stats struct {
	Turns, Crashes, Panics, Restarts, BudgetExceeded, Resets, Quarantines, Dropped uint64
}

// Shard owns isolates, a message pool, timers and its ring endpoints. It is
// driven by Tick and never touched from more than one thread.
type Shard struct {
	sys   *System
	id    uint8
	spec  ShardSpec
	types [256]*isoType
	list  []*isoType

	pool     []Message
	poolAtt  []any
	poolFree []uint32
	reserve  int

	readyBuf  []uint32
	rhead, rl int
	timers    timerHeap

	out []*ring // indexed by destination shard (nil for self)
	in  []*ring // indexed by source shard (nil for self)

	groups      []group
	io          *reactor
	resetBudget budget
	bootHandles []Handle

	ctx       Ctx
	yieldMsg  Message
	lastPanic any

	tick        uint64
	epoch       uint64
	live        int
	shutting    bool
	quarantined bool
	stats       Stats
}

func newShard(sys *System, id uint8, spec SystemSpec) *Shard {
	s := &Shard{sys: sys, id: id, spec: spec.Shards[id], reserve: spec.SystemReserve}
	total := 0
	for _, d := range spec.Types {
		t := newIsoType(d)
		s.types[d.id] = t
		s.list = append(s.list, t)
		total += t.slots
	}
	s.pool = make([]Message, spec.PoolSlots)
	s.poolAtt = make([]any, spec.PoolSlots)
	s.poolFree = make([]uint32, 0, spec.PoolSlots)
	s.refillPool()
	n := 16
	for n < 4*total {
		n <<= 1
	}
	s.readyBuf = make([]uint32, n)
	s.timers = newTimerHeap(spec.TimerEntries)
	s.io = newReactor(s, sys.iop.epfd, spec.MaxFDs, total)
	s.groups = newGroups(s.spec.Groups)
	s.resetBudget = newBudget(spec.ResetMax, spec.ResetWindow)
	return s
}

func (s *Shard) refillPool() {
	s.poolFree = s.poolFree[:0]
	for i := len(s.pool) - 1; i >= 0; i-- {
		s.poolFree = append(s.poolFree, uint32(i))
		s.poolAtt[i] = nil
	}
}

func (s *Shard) Stats() Stats      { return s.stats }
func (s *Shard) Quarantined() bool { return s.quarantined }
func (s *Shard) Live() int         { return s.live }

func (s *Shard) handleOf(t *isoType, slot uint32) Handle {
	return MakeHandle(s.id, t.id, slot, t.gen[slot])
}

// ---- pool, mailboxes, ready queue ----

func (s *Shard) poolAlloc(system bool) (uint32, bool) {
	n := len(s.poolFree)
	if n == 0 || (!system && n <= s.reserve) {
		return 0, false
	}
	idx := s.poolFree[n-1]
	s.poolFree = s.poolFree[:n-1]
	return idx, true
}

func (s *Shard) poolRelease(idx uint32) {
	s.poolAtt[idx] = nil
	s.poolFree = append(s.poolFree, idx)
}

func (s *Shard) mboxPop(t *isoType, slot uint32) (uint32, bool) {
	if t.mlen[slot] == 0 {
		return 0, false
	}
	c := uint32(t.stride)
	idx := t.mbuf[int(slot)*t.stride+int(t.mhead[slot])]
	t.mhead[slot] = (t.mhead[slot] + 1) % c
	t.mlen[slot]--
	return idx, true
}

func (s *Shard) readyPush(t *isoType, slot uint32) {
	if s.rl == len(s.readyBuf) {
		panic("gina: ready queue overflow (engine bug)")
	}
	s.readyBuf[(s.rhead+s.rl)&(len(s.readyBuf)-1)] = uint32(t.id)<<20 | slot
	s.rl++
}

func (s *Shard) readyPop() (TypeID, uint32) {
	e := s.readyBuf[s.rhead]
	s.rhead = (s.rhead + 1) & (len(s.readyBuf) - 1)
	s.rl--
	return TypeID(e >> 20), e & 0xFFFFF
}

func (s *Shard) makeReady(t *isoType, slot uint32) {
	t.state[slot] = stReady
	s.readyPush(t, slot)
}

func (s *Shard) alloc(t *isoType) (uint32, bool) {
	n := len(t.free)
	if n == 0 {
		return 0, false
	}
	slot := t.free[n-1]
	t.free = t.free[:n-1]
	t.state[slot] = stIdle
	s.live++
	return slot, true
}

// freeSlot releases a slot: pending mail is returned to the pool, the isolate
// state is zeroed (so the GC can reclaim what it referenced) and the generation
// is bumped (so every old Handle goes stale).
func (s *Shard) freeSlot(t *isoType, slot uint32) {
	if op := t.ioop[slot]; op >= 0 {
		s.io.cancelOp(op)
		t.ioop[slot] = -1
	}
	if fd := t.ownfd[slot]; fd != 0 {
		t.ownfd[slot] = 0
		s.io.closeFD(fd)
	}
	for {
		idx, ok := s.mboxPop(t, slot)
		if !ok {
			break
		}
		s.poolRelease(idx)
	}
	t.mhead[slot] = 0
	t.ops.zero(slot)
	g := (t.gen[slot] + 1) & genMask
	if g == 0 {
		g = 1
	}
	t.gen[slot] = g
	t.state[slot] = stFree
	t.cref[slot] = noRef
	t.parent[slot] = 0
	t.free = append(t.free, slot)
	s.live--
}

// ---- messaging ----

// enqueue delivers m to a local isolate. system messages may use the reserved
// part of the pool so timers and supervision survive data-plane saturation.
func (s *Shard) enqueue(to Handle, m *Message, att any, system bool) SendResult {
	t := s.types[to.Type()]
	if t == nil {
		return SendStaleHandle
	}
	slot := to.Slot()
	if int(slot) >= t.slots || t.state[slot] == stFree || t.gen[slot] != to.Gen() {
		return SendStaleHandle
	}
	if t.mlen[slot] >= uint32(t.mboxCap) {
		return SendMailboxFull
	}
	idx, ok := s.poolAlloc(system)
	if !ok {
		return SendPoolExhausted
	}
	p := &s.pool[idx]
	*p = *m
	p.Dest = to
	s.poolAtt[idx] = att
	pos := (t.mhead[slot] + t.mlen[slot]) % uint32(t.stride)
	t.mbuf[int(slot)*t.stride+int(pos)] = idx
	t.mlen[slot]++
	if t.state[slot] == stIdle {
		s.makeReady(t, slot)
	}
	return SendOK
}

func (s *Shard) route(to Handle, m *Message, att any) SendResult {
	if to.Shard() == s.id {
		return s.enqueue(to, m, att, false)
	}
	if att != nil {
		return SendAttachNotLocal
	}
	if int(to.Shard()) >= len(s.sys.shards) {
		return SendStaleHandle
	}
	dst := s.sys.shards[to.Shard()]
	if dst.quarantined {
		return SendStaleHandle
	}
	if f := s.sys.faults; f != nil && f.dropRemote(s.id, to.Shard()) {
		return SendOK // lost in transit: the sender cannot tell
	}
	m.Dest = to
	if !s.out[to.Shard()].stage(m) {
		return SendRingFull
	}
	return SendOK
}

// ---- tick ----

// Tick runs one scheduler round in the fixed phase order of SPEC §6.1 and
// reports whether anything happened.
func (s *Shard) Tick() bool {
	s.tick++
	if s.quarantined {
		for _, r := range s.in {
			if r != nil {
				r.head = r.tail
			}
		}
		return false
	}
	s.io.flush()
	p := s.drainInbound()     // 1. cross-shard messages
	p = s.fireTimers() || p   // 2. timers
	p = s.dispatch() || p     // 3. isolate turns
	for _, r := range s.out { // 4. publish outbound batches
		if r != nil {
			r.publish()
		}
	}
	return p
}

func (s *Shard) drainInbound() bool {
	any := false
	for _, r := range s.in {
		if r == nil {
			continue
		}
		for {
			m, ok := r.peek()
			if !ok {
				break
			}
			any = true
			if s.enqueue(m.Dest, m, nil, false) != SendOK {
				s.stats.Dropped++
			}
			r.advance()
		}
	}
	return any
}

func (s *Shard) fireTimers() bool {
	now := s.sys.clock.Now()
	fired := false
	for {
		due, ok := s.timers.peek()
		if !ok || due > now {
			return fired
		}
		e := s.timers.pop()
		fired = true
		if e.op >= 0 {
			s.io.timeoutOp(e.op, e.opSeq)
			continue
		}
		var m Message
		m.Tag = e.tag
		s.enqueue(e.target, &m, nil, true)
	}
}

func (s *Shard) dispatch() bool {
	n := s.rl
	ran := false
	for i := 0; i < n && s.rl > 0; i++ {
		tid, slot := s.readyPop()
		t := s.types[tid]
		if t == nil || t.state[slot] != stReady {
			continue // stale entry: the slot was freed or reused since it was queued
		}
		s.turn(t, slot)
		ran = true
	}
	return ran
}

func (s *Shard) turn(t *isoType, slot uint32) {
	self := s.handleOf(t, slot)
	t.state[slot] = stRunning
	idx, has := s.mboxPop(t, slot)
	var msg *Message
	var att any
	if has {
		msg, att = &s.pool[idx], s.poolAtt[idx]
	} else {
		s.yieldMsg = Message{Dest: self, Tag: TagYield}
		msg = &s.yieldMsg
	}
	s.ctx = Ctx{s: s, self: self, t: t, slot: slot, att: att}
	eff := s.runTurn(t, slot, msg)
	if tr := s.sys.trace; tr != nil {
		tr.add(s.tick, s.id, t.id, slot, msg.Tag, eff)
	}
	if has {
		s.poolRelease(idx)
	}
	staged := s.ctx.staged
	s.ctx = Ctx{}
	s.stats.Turns++
	switch eff.Kind {
	case effDone:
		s.terminate(t, slot, ExitNormal)
	case effYield:
		s.makeReady(t, slot)
	case effWaitMessage:
		if t.mlen[slot] > 0 {
			s.makeReady(t, slot)
		} else {
			t.state[slot] = stIdle
		}
	case effWaitIO:
		if staged.kind == 0 { // WaitIO without a staged operation
			s.stats.Crashes++
			s.terminate(t, slot, ExitCrashed)
		} else {
			t.state[slot] = stWaitIO
			s.io.submit(self, t, slot, &staged)
		}
	default: // effCrash, or a zero Effect (contract violation)
		s.stats.Crashes++
		s.terminate(t, slot, ExitCrashed)
	}
}

// runTurn is the trap boundary: a panic (or, with SetPanicOnFault, a memory
// fault) inside a handler becomes Crash(FaultPanic) for that isolate only.
func (s *Shard) runTurn(t *isoType, slot uint32, msg *Message) (eff Effect) {
	defer func() {
		if r := recover(); r != nil {
			s.stats.Panics++
			s.lastPanic = r
			eff = Crash(FaultPanic)
		}
	}()
	if f := s.sys.faults; f != nil && f.rollCrash() {
		panic("gina: injected crash")
	}
	return t.ops.run(slot, &s.ctx, msg)
}

func (s *Shard) runInit(t *isoType, slot uint32, args []byte) (eff Effect) {
	defer func() {
		if r := recover(); r != nil {
			s.stats.Panics++
			s.lastPanic = r
			eff = Crash(FaultPanic)
		}
	}()
	return t.ops.init(slot, &s.ctx, args)
}

// ---- spawning ----

func (s *Shard) spawn(sp *SpawnSpec, parent Handle) (Handle, SpawnError) {
	gi := int32(-1)
	if sp.Group != GroupNone {
		if gi = s.groupIndex(sp.Group); gi < 0 {
			return 0, SpawnErrGroupNotAllocated
		}
		if g := &s.groups[gi]; len(g.children) == cap(g.children) {
			return 0, SpawnErrGroupFull
		}
	}
	h, eff, err := s.startIsolate(sp, parent)
	if err != SpawnErrNone {
		return 0, err
	}
	if gi >= 0 && eff.Kind != effDone {
		s.register(gi, h, sp, parent)
	}
	return h, SpawnErrNone
}

// startIsolate allocates a slot and runs the init handler synchronously.
func (s *Shard) startIsolate(sp *SpawnSpec, parent Handle) (Handle, Effect, SpawnError) {
	t := s.types[sp.Type]
	if t == nil {
		return 0, Effect{}, SpawnErrTypeNotAllocated
	}
	slot, ok := s.alloc(t)
	if !ok {
		return 0, Effect{}, SpawnErrSlotsFull
	}
	t.parent[slot] = parent
	h := s.handleOf(t, slot)
	if sp.HandoffFD != 0 {
		if !s.io.valid(sp.HandoffFD) {
			s.freeSlot(t, slot)
			return 0, Effect{}, SpawnErrBadFD
		}
		t.ownfd[slot] = sp.HandoffFD
	}
	t.state[slot] = stRunning
	saved := s.ctx
	s.ctx = Ctx{s: s, self: h, t: t, slot: slot}
	eff := s.runInit(t, slot, sp.Args[:sp.ArgsSize])
	staged := s.ctx.staged
	s.ctx = saved
	switch eff.Kind {
	case effDone:
		s.freeSlot(t, slot)
	case effWaitIO:
		if staged.kind == 0 {
			s.stats.Crashes++
			s.freeSlot(t, slot)
			return 0, eff, SpawnErrInitFailed
		}
		t.state[slot] = stWaitIO
		s.io.submit(h, t, slot, &staged)
	case effYield:
		s.makeReady(t, slot)
	case effWaitMessage:
		if t.mlen[slot] > 0 {
			s.makeReady(t, slot)
		} else {
			t.state[slot] = stIdle
		}
	default:
		s.stats.Crashes++
		s.freeSlot(t, slot)
		return 0, eff, SpawnErrInitFailed
	}
	return h, eff, SpawnErrNone
}

func (s *Shard) bootSpawns() error {
	s.bootHandles = s.bootHandles[:0]
	for i := range s.spec.Boot {
		h, err := s.spawn(&s.spec.Boot[i], 0)
		if err != SpawnErrNone {
			return fmt.Errorf("gina: shard %d boot spawn %d: %s", s.id, i, err)
		}
		s.bootHandles = append(s.bootHandles, h)
	}
	return nil
}

// ---- Level-2 reset, quarantine, shutdown ----

// wipe drops every isolate, message, timer and supervision record on the shard.
func (s *Shard) wipe() {
	for _, t := range s.list {
		for slot := 0; slot < t.slots; slot++ {
			if t.state[slot] != stFree {
				t.ops.zero(uint32(slot))
				g := (t.gen[slot] + 1) & genMask
				if g == 0 {
					g = 1
				}
				t.gen[slot] = g
			}
			t.state[slot] = stFree
			t.mhead[slot], t.mlen[slot] = 0, 0
			t.cref[slot] = noRef
			t.parent[slot] = 0
			t.ioop[slot], t.ownfd[slot] = -1, 0
		}
		t.refill()
	}
	s.live = 0
	s.io.reset()
	s.refillPool()
	s.rhead, s.rl = 0, 0
	s.timers.clear()
	s.groups = newGroups(s.spec.Groups)
}

// level2Reset tears the shard down and rebuilds it from the boot spec. Too many
// resets in a window quarantine the shard instead.
func (s *Shard) level2Reset() {
	s.epoch++
	s.stats.Resets++
	s.wipe()
	if s.resetBudget.exceeded(s.tick) {
		s.quarantined = true
		s.stats.Quarantines++
		return
	}
	if err := s.bootSpawns(); err != nil {
		s.quarantined = true
		s.stats.Quarantines++
	}
}

func (s *Shard) beginShutdown() {
	s.shutting = true
	for _, t := range s.list {
		for slot := 0; slot < t.slots; slot++ {
			if t.state[slot] == stFree {
				continue
			}
			m := Message{Tag: TagShutdown}
			s.enqueue(s.handleOf(t, uint32(slot)), &m, nil, true)
			if t.state[slot] == stWaitIO && t.ioop[slot] >= 0 {
				// wake it with -ECANCELED ahead of the shutdown message
				s.io.finish(t.ioop[slot], -int64(syscall.ECANCELED))
			}
		}
	}
}

// forceStop frees every remaining isolate after the shutdown grace period.
func (s *Shard) forceStop() int {
	n := 0
	for _, t := range s.list {
		for slot := 0; slot < t.slots; slot++ {
			if t.state[slot] != stFree {
				s.freeSlot(t, uint32(slot))
				n++
			}
		}
	}
	s.io.reset()
	s.groups = newGroups(s.spec.Groups)
	return n
}

func (s *Shard) pending() bool {
	if s.quarantined {
		return false
	}
	if s.rl > 0 {
		return true
	}
	for _, r := range s.in {
		if r != nil && r.pending() {
			return true
		}
	}
	for _, r := range s.out {
		if r != nil && r.pending() {
			return true
		}
	}
	return false
}

// check verifies the shard's structural invariants (pool conservation, mailbox
// bounds, live count, supervision records pointing at live isolates).
func (s *Shard) check() error {
	inbox, live := 0, 0
	for _, t := range s.list {
		for slot := 0; slot < t.slots; slot++ {
			if t.mlen[slot] > uint32(t.stride) {
				return fmt.Errorf("shard %d type %d slot %d: mailbox over capacity", s.id, t.id, slot)
			}
			if t.state[slot] == stFree {
				if t.mlen[slot] != 0 {
					return fmt.Errorf("shard %d type %d slot %d: free slot has mail", s.id, t.id, slot)
				}
				continue
			}
			live++
			inbox += int(t.mlen[slot])
		}
	}
	if live != s.live {
		return fmt.Errorf("shard %d: live count %d, counted %d", s.id, s.live, live)
	}
	if inUse := len(s.pool) - len(s.poolFree); inUse != inbox {
		return fmt.Errorf("shard %d: pool conservation: %d in use, %d in mailboxes", s.id, inUse, inbox)
	}
	for gi := range s.groups {
		for ci, c := range s.groups[gi].children {
			if !c.live {
				continue
			}
			t := s.types[c.typ]
			if t.state[c.slot] == stFree || t.gen[c.slot] != c.last.Gen() || t.cref[c.slot] != (childRef{int32(gi), int32(ci)}) {
				return fmt.Errorf("shard %d group %d child %d: supervision record out of sync", s.id, s.groups[gi].spec.ID, ci)
			}
		}
	}
	return nil
}

// ---- Ctx ----

// Ctx is the handler's window onto its shard. It is only valid during the turn.
type Ctx struct {
	s      *Shard
	self   Handle
	t      *isoType
	slot   uint32
	att    any
	staged ioStage
}

func (c *Ctx) Self() Handle         { return c.self }
func (c *Ctx) ShardID() uint8       { return c.s.id }
func (c *Ctx) IsShuttingDown() bool { return c.s.shutting }
func (c *Ctx) Now() uint64          { return c.s.sys.clock.Now() }

// Attachment returns the Go value sent with SendAttach (nil otherwise). It is
// valid for the current turn only.
func (c *Ctx) Attachment() any { return c.att }

func (c *Ctx) SendRaw(to Handle, tag Tag, payload []byte) SendResult {
	if len(payload) > MaxPayload {
		return SendPayloadTooLarge
	}
	var m Message
	m.Source, m.Tag = c.self, tag
	m.PayloadSize = uint16(copy(m.Payload[:], payload))
	return c.s.route(to, &m, nil)
}

// Send sends a pointer-free payload value (<= 96 bytes, no padding).
func Send[P any](c *Ctx, to Handle, tag Tag, p *P) SendResult {
	checkPOD[P](MaxPayload)
	var m Message
	m.Source, m.Tag = c.self, tag
	m.PayloadSize = uint16(copy(m.Payload[:], BytesOf(p)))
	return c.s.route(to, &m, nil)
}

// SendAttach sends an arbitrary Go value to a same-shard isolate. Ownership
// moves to the receiver: the sender must drop its reference.
func (c *Ctx) SendAttach(to Handle, tag Tag, payload []byte, v any) SendResult {
	if len(payload) > MaxPayload {
		return SendPayloadTooLarge
	}
	var m Message
	m.Source, m.Tag = c.self, tag
	m.PayloadSize = uint16(copy(m.Payload[:], payload))
	return c.s.route(to, &m, v)
}

// Spawn starts an isolate on this shard; its init handler runs before Spawn returns.
func (c *Ctx) Spawn(sp SpawnSpec) (Handle, SpawnError) { return c.s.spawn(&sp, c.self) }

// RegisterTimer delivers an empty message with tag to this isolate after d.
func (c *Ctx) RegisterTimer(d time.Duration, tag Tag) TimerID {
	return c.s.timers.push(timerEntry{due: c.s.sys.clock.Now() + uint64(d), target: c.self, tag: tag, op: -1})
}

// CancelTimer cancels a timer from RegisterTimer; false if it already fired.
func (c *Ctx) CancelTimer(id TimerID) bool { return c.s.timers.cancel(id) }

// enqueueFront delivers a system message ahead of everything already in the
// target's mailbox (I/O completions must be seen before older user mail). The
// mailbox has one spare slot for this, so it cannot be full while an I/O
// operation is outstanding.
func (s *Shard) enqueueFront(to Handle, m *Message) SendResult {
	t := s.types[to.Type()]
	if t == nil {
		return SendStaleHandle
	}
	slot := to.Slot()
	if int(slot) >= t.slots || t.state[slot] == stFree || t.gen[slot] != to.Gen() {
		return SendStaleHandle
	}
	if t.mlen[slot] >= uint32(t.stride) {
		return SendMailboxFull
	}
	idx, ok := s.poolAlloc(true)
	if !ok {
		return SendPoolExhausted
	}
	p := &s.pool[idx]
	*p = *m
	p.Dest = to
	s.poolAtt[idx] = nil
	st := uint32(t.stride)
	t.mhead[slot] = (t.mhead[slot] + st - 1) % st
	t.mbuf[int(slot)*t.stride+int(t.mhead[slot])] = idx
	t.mlen[slot]++
	if t.state[slot] == stIdle || t.state[slot] == stWaitIO {
		s.makeReady(t, slot)
	}
	return SendOK
}
