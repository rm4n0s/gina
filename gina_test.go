package gina_test

import (
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

const (
	tagStart = gina.TagUserBase + iota
	tagBall
	tagBoom
	tagDone
	tagWork
	tagTick
	tagTickB
	tagArm
)

type startMsg struct {
	Peer         gina.Handle
	Limit, Serve uint64
}
type ballMsg struct{ N uint64 }

func opts(slots int) gina.TypeOptions { return gina.TypeOptions{SlotCount: slots, MailboxCapacity: 8} }

func newSim(t *testing.T, spec gina.SystemSpec, seed uint64, cfg gina.SimConfig) *gina.Sim {
	t.Helper()
	s, err := gina.NewSim(spec, seed, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func run(t *testing.T, s *gina.Sim, rounds int) {
	t.Helper()
	if _, err := s.Run(rounds); err != nil {
		t.Fatal(err)
	}
}

func TestHandlePacking(t *testing.T) {
	for _, c := range []struct {
		shard uint8
		typ   gina.TypeID
		slot  uint32
		gen   uint32
	}{{0, 0, 0, 1}, {255, 254, 1<<20 - 1, 1<<28 - 1}, {7, 3, 12345, 99}} {
		h := gina.MakeHandle(c.shard, c.typ, c.slot, c.gen)
		if h.Shard() != c.shard || h.Type() != c.typ || h.Slot() != c.slot || h.Gen() != c.gen {
			t.Fatalf("round trip failed for %+v: %#x", c, uint64(h))
		}
	}
}

func TestPayloadValidation(t *testing.T) {
	type ok struct {
		A uint64
		B uint32
		C uint32
	}
	type padded struct {
		A uint8
		B uint64
	}
	type withPtr struct{ P *int }
	type withString struct{ S string }
	type withSlice struct{ S []byte }
	type tooBig [97]byte
	if err := gina.ValidatePayloadType[ok](); err != nil {
		t.Fatalf("ok rejected: %v", err)
	}
	if err := gina.ValidatePayloadType[[96]byte](); err != nil {
		t.Fatalf("96 bytes rejected: %v", err)
	}
	if gina.ValidatePayloadType[padded]() == nil {
		t.Fatal("padded struct accepted")
	}
	if gina.ValidatePayloadType[withPtr]() == nil || gina.ValidatePayloadType[withString]() == nil || gina.ValidatePayloadType[withSlice]() == nil {
		t.Fatal("pointer-carrying payload accepted")
	}
	if gina.ValidatePayloadType[tooBig]() == nil {
		t.Fatal("oversized payload accepted")
	}
}

// ---- messaging ----

var finalN uint64

type player struct {
	peer        gina.Handle
	limit, last uint64
}

func playerHandler(self *player, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagStart:
		s := gina.PayloadAs[startMsg](m)
		self.peer, self.limit = s.Peer, s.Limit
		if s.Serve == 1 {
			b := ballMsg{1}
			gina.Send(ctx, self.peer, tagBall, &b)
		}
	case tagBall:
		n := gina.PayloadAs[ballMsg](m).N
		self.last = n
		if n >= self.limit {
			finalN = n
			return gina.Done()
		}
		b := ballMsg{n + 1}
		gina.Send(ctx, self.peer, tagBall, &b)
	}
	return gina.WaitMessage()
}

func playerSpec(shards int) gina.SystemSpec {
	return gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(8), nil, playerHandler)},
		Shards: make([]gina.ShardSpec, shards),
	}
}

func TestPingPongAcrossShards(t *testing.T) {
	finalN = 0
	s := newSim(t, playerSpec(2), 1, gina.SimConfig{})
	tmp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
	a, e1 := s.Sys.Spawn(0, tmp)
	b, e2 := s.Sys.Spawn(1, tmp)
	if e1 != gina.SpawnErrNone || e2 != gina.SpawnErrNone {
		t.Fatal(e1, e2)
	}
	gina.SendTo(s.Sys, b, tagStart, &startMsg{Peer: a, Limit: 100})
	gina.SendTo(s.Sys, a, tagStart, &startMsg{Peer: b, Limit: 100, Serve: 1})
	idle, err := s.Run(1000)
	if err != nil || !idle {
		t.Fatalf("idle=%v err=%v", idle, err)
	}
	if finalN != 100 {
		t.Fatalf("final ball = %d, want 100", finalN)
	}
}

type sink struct{ got int }

var sinkTicks int

func sinkHandler(self *sink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagDone:
		return gina.Done()
	case tagTick:
		sinkTicks++
	}
	self.got++
	return gina.WaitMessage()
}

func TestMailboxFullAndStaleHandle(t *testing.T) {
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 4}, nil, sinkHandler)},
		Shards: make([]gina.ShardSpec, 1),
	}
	s := newSim(t, spec, 1, gina.SimConfig{})
	h, _ := s.Sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	for i := 0; i < 4; i++ {
		if r := s.Sys.Send(h, tagWork, nil); r != gina.SendOK {
			t.Fatalf("send %d = %v", i, r)
		}
	}
	if r := s.Sys.Send(h, tagWork, nil); r != gina.SendMailboxFull {
		t.Fatalf("5th send = %v, want mailbox_full", r)
	}
	run(t, s, 20)
	s.Sys.Send(h, tagDone, nil)
	run(t, s, 5)
	if r := s.Sys.Send(h, tagWork, nil); r != gina.SendStaleHandle {
		t.Fatalf("send to dead isolate = %v, want stale_handle", r)
	}
	if s.Sys.Shard(0).Live() != 0 {
		t.Fatal("isolate not freed")
	}
}

func TestSystemReserveKeepsTimersAlive(t *testing.T) {
	sinkTicks = 0
	init := func(self *sink, ctx *gina.Ctx, args []byte) gina.Effect {
		ctx.RegisterTimer(time.Millisecond, tagTick)
		return gina.WaitMessage()
	}
	spec := gina.SystemSpec{
		Types:         []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 256}, init, sinkHandler)},
		Shards:        make([]gina.ShardSpec, 1),
		PoolSlots:     32,
		SystemReserve: 8,
	}
	s := newSim(t, spec, 1, gina.SimConfig{})
	h, _ := s.Sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	sent := 0
	for s.Sys.Send(h, tagWork, nil) == gina.SendOK {
		sent++
	}
	if sent != 24 {
		t.Fatalf("user messages accepted = %d, want 24 (32 pool - 8 reserve)", sent)
	}
	s.Clock.Advance(2_000_000)
	run(t, s, 100)
	if sinkTicks != 1 {
		t.Fatalf("timer delivered %d times despite exhausted user pool, want 1", sinkTicks)
	}
}

var order []gina.Tag

type timed struct{}

func TestTimerOrdering(t *testing.T) {
	order = nil
	init := func(self *timed, ctx *gina.Ctx, args []byte) gina.Effect {
		ctx.RegisterTimer(3*time.Millisecond, tagTick)
		ctx.RegisterTimer(1*time.Millisecond, tagTickB)
		return gina.WaitMessage()
	}
	h := func(self *timed, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		order = append(order, m.Tag)
		return gina.WaitMessage()
	}
	spec := gina.SystemSpec{Types: []gina.TypeDesc{gina.RegisterType(1, opts(2), init, h)}, Shards: make([]gina.ShardSpec, 1)}
	s := newSim(t, spec, 1, gina.SimConfig{})
	s.Sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	run(t, s, 50)
	if len(order) != 2 || order[0] != tagTickB || order[1] != tagTick {
		t.Fatalf("timer order = %v", order)
	}
}

type attach struct{ sum int }

var attachSum int
var remoteAttach gina.SendResult

func TestAttachments(t *testing.T) {
	attachSum, remoteAttach = 0, 99
	var peerB gina.Handle
	h := func(self *attach, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case tagStart: // local sender
			me := ctx.Self()
			data := []int{1, 2, 3, 4}
			ctx.SendAttach(me, tagWork, nil, data)
			remoteAttach = ctx.SendAttach(peerB, tagWork, nil, data)
		case tagWork:
			for _, v := range ctx.Attachment().([]int) {
				attachSum += v
			}
		}
		return gina.WaitMessage()
	}
	spec := gina.SystemSpec{Types: []gina.TypeDesc{gina.RegisterType(1, opts(2), nil, h)}, Shards: make([]gina.ShardSpec, 2)}
	s := newSim(t, spec, 1, gina.SimConfig{})
	a, _ := s.Sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	peerB, _ = s.Sys.Spawn(1, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	s.Sys.Send(a, tagStart, nil)
	run(t, s, 20)
	if attachSum != 10 {
		t.Fatalf("attachment sum = %d, want 10", attachSum)
	}
	if remoteAttach != gina.SendAttachNotLocal {
		t.Fatalf("cross-shard attach = %v, want attach_not_local", remoteAttach)
	}
}

func TestSpawnArgs(t *testing.T) {
	type args struct{ X, Y uint64 }
	var got args
	init := func(self *timed, ctx *gina.Ctx, a []byte) gina.Effect {
		got = gina.ArgsAs[args](a)
		return gina.WaitMessage()
	}
	h := func(self *timed, ctx *gina.Ctx, m *gina.Message) gina.Effect { return gina.WaitMessage() }
	spec := gina.SystemSpec{Types: []gina.TypeDesc{gina.RegisterType(1, opts(2), init, h)}, Shards: make([]gina.ShardSpec, 1)}
	s := newSim(t, spec, 1, gina.SimConfig{})
	s.Sys.Spawn(0, gina.WithArgs(gina.SpawnSpec{Type: 1, Group: gina.GroupNone}, &args{7, 9}))
	if got != (args{7, 9}) {
		t.Fatalf("args = %+v", got)
	}
}

// ---- supervision ----

var starts [8]int

type proc struct{}

func procType(id gina.TypeID) gina.TypeDesc {
	init := func(self *proc, ctx *gina.Ctx, args []byte) gina.Effect {
		starts[id]++
		return gina.WaitMessage()
	}
	h := func(self *proc, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case tagBoom:
			var p *int
			*p = 1 // nil dereference: recovered by the trap boundary
		case tagDone:
			return gina.Done()
		}
		return gina.WaitMessage()
	}
	return gina.RegisterType(id, opts(4), init, h)
}

func strategySpec(st gina.Strategy, rt [3]gina.RestartType) gina.SystemSpec {
	boot := []gina.SpawnSpec{}
	for i := 0; i < 3; i++ {
		boot = append(boot, gina.SpawnSpec{Type: gina.TypeID(i + 1), Group: 1, Restart: rt[i]})
	}
	return gina.SystemSpec{
		Types:  []gina.TypeDesc{procType(1), procType(2), procType(3)},
		Shards: []gina.ShardSpec{{Groups: []gina.GroupSpec{{ID: 1, Strategy: st, RestartMax: 10, WindowTicks: 1000}}, Boot: boot}},
	}
}

func TestSupervisionStrategies(t *testing.T) {
	perm := [3]gina.RestartType{}
	for _, c := range []struct {
		name string
		st   gina.Strategy
		want [3]int
	}{
		{"one_for_one", gina.OneForOne, [3]int{1, 2, 1}},
		{"one_for_all", gina.OneForAll, [3]int{2, 2, 2}},
		{"rest_for_one", gina.RestForOne, [3]int{1, 2, 2}},
	} {
		starts = [8]int{}
		s := newSim(t, strategySpec(c.st, perm), 1, gina.SimConfig{})
		mid := s.Sys.BootHandle(0, 1)
		old := s.Sys.BootHandle(0, 0)
		s.Sys.Send(mid, tagBoom, nil)
		run(t, s, 20)
		got := [3]int{starts[1], starts[2], starts[3]}
		if got != c.want {
			t.Errorf("%s: starts = %v, want %v", c.name, got, c.want)
		}
		if s.Sys.Shard(0).Stats().Panics != 1 {
			t.Errorf("%s: panics = %d, want 1", c.name, s.Sys.Shard(0).Stats().Panics)
		}
		if c.st == gina.OneForAll && s.Sys.Send(old, tagWork, nil) != gina.SendStaleHandle {
			t.Errorf("%s: sibling handle should be stale after teardown", c.name)
		}
	}
}

func TestRestartTypes(t *testing.T) {
	rt := [3]gina.RestartType{gina.RestartPermanent, gina.RestartTransient, gina.RestartTemporary}
	for _, c := range []struct {
		name string
		tag  gina.Tag
		want [3]int
	}{
		{"normal exit", tagDone, [3]int{2, 1, 1}},
		{"crash", tagBoom, [3]int{2, 2, 1}},
	} {
		starts = [8]int{}
		s := newSim(t, strategySpec(gina.OneForOne, rt), 1, gina.SimConfig{})
		hs := [3]gina.Handle{s.Sys.BootHandle(0, 0), s.Sys.BootHandle(0, 1), s.Sys.BootHandle(0, 2)}
		for _, h := range hs {
			s.Sys.Send(h, c.tag, nil)
		}
		run(t, s, 20)
		if got := [3]int{starts[1], starts[2], starts[3]}; got != c.want {
			t.Errorf("%s: starts = %v, want %v", c.name, got, c.want)
		}
	}
}

// parent learns about restarts through TagChildExit
var lastExit gina.ChildExit

type boss struct{ worker gina.Handle }

func TestChildExitNotification(t *testing.T) {
	lastExit = gina.ChildExit{}
	init := func(self *boss, ctx *gina.Ctx, args []byte) gina.Effect {
		self.worker, _ = ctx.Spawn(gina.SpawnSpec{Type: 2, Group: gina.GroupRoot})
		return gina.WaitMessage()
	}
	h := func(self *boss, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case gina.TagChildExit:
			lastExit = *gina.PayloadAs[gina.ChildExit](m)
			self.worker = lastExit.New
		case tagBoom:
			ctx.SendRaw(self.worker, tagBoom, nil)
		}
		return gina.WaitMessage()
	}
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(2), init, h), procType(2)},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}}}},
	}
	starts = [8]int{}
	s := newSim(t, spec, 1, gina.SimConfig{})
	s.Sys.Send(s.Sys.BootHandle(0, 0), tagBoom, nil)
	run(t, s, 20)
	if starts[2] != 2 {
		t.Fatalf("worker starts = %d, want 2", starts[2])
	}
	if lastExit.Old == 0 || lastExit.New == 0 || lastExit.Old == lastExit.New || lastExit.Kind != uint64(gina.ExitCrashed) {
		t.Fatalf("bad ChildExit: %+v", lastExit)
	}
}

type loopy struct{}

func TestBudgetLevel2ResetAndQuarantine(t *testing.T) {
	init := func(self *loopy, ctx *gina.Ctx, args []byte) gina.Effect {
		ctx.RegisterTimer(time.Millisecond, tagTick)
		return gina.WaitMessage()
	}
	h := func(self *loopy, ctx *gina.Ctx, m *gina.Message) gina.Effect { return gina.Crash(gina.FaultUser) }
	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{gina.RegisterType(1, opts(2), init, h)},
		Shards: []gina.ShardSpec{{
			Groups: []gina.GroupSpec{{ID: 0, RestartMax: 2, WindowTicks: 1000}},
			Boot:   []gina.SpawnSpec{{Type: 1, Group: 0}},
		}},
		ResetMax: 2, ResetWindow: 10000,
	}
	s := newSim(t, spec, 1, gina.SimConfig{})
	idle, err := s.Run(2000)
	if err != nil || !idle {
		t.Fatalf("idle=%v err=%v", idle, err)
	}
	st := s.Sys.Shard(0).Stats()
	if st.Restarts != 6 || st.Resets != 3 || st.Quarantines != 1 || !s.Sys.Shard(0).Quarantined() {
		t.Fatalf("stats = %+v quarantined=%v", st, s.Sys.Shard(0).Quarantined())
	}
	if r := s.Sys.Send(gina.MakeHandle(0, 1, 0, 1), tagWork, nil); r != gina.SendStaleHandle {
		t.Fatalf("send to quarantined shard = %v", r)
	}
	if err := s.Sys.Revive(0); err != nil {
		t.Fatal(err)
	}
	if s.Sys.Shard(0).Quarantined() || s.Sys.Shard(0).Live() != 1 {
		t.Fatal("revive did not rebuild the shard")
	}
}

type polite struct{}
type stubborn struct{}

func TestShutdown(t *testing.T) {
	hp := func(self *polite, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		if m.Tag == gina.TagShutdown {
			return gina.Done()
		}
		return gina.WaitMessage()
	}
	hs := func(self *stubborn, ctx *gina.Ctx, m *gina.Message) gina.Effect { return gina.WaitMessage() }
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(4), nil, hp), gina.RegisterType(2, opts(4), nil, hs)},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: 0}, {Type: 1, Group: 0}, {Type: 2, Group: 0}}}},
	}
	s := newSim(t, spec, 1, gina.SimConfig{})
	if forced := s.Sys.Shutdown(10); forced != 1 {
		t.Fatalf("forced = %d, want 1 (the stubborn isolate)", forced)
	}
	if s.Sys.Shard(0).Live() != 0 {
		t.Fatal("isolates left after shutdown")
	}
	if err := s.Sys.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

// ---- simulation: determinism, faults, invariants ----

type gossip struct{ hops uint64 }

func gossipSpec(shards int) gina.SystemSpec {
	h := func(self *gossip, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case tagStart, tagBall:
			ttl := uint64(200)
			if m.Tag == tagBall {
				ttl = gina.PayloadAs[ballMsg](m).N
			}
			self.hops++
			if ttl == 0 {
				return gina.WaitMessage()
			}
			next := gina.MakeHandle((ctx.ShardID()+1)%uint8(shards), 1, 0, 1)
			if self.hops%3 == 0 { // some traffic stays local
				next = ctx.Self()
			}
			b := ballMsg{ttl - 1}
			gina.Send(ctx, next, tagBall, &b)
		}
		return gina.WaitMessage()
	}
	sh := make([]gina.ShardSpec, shards)
	for i := range sh {
		sh[i] = gina.ShardSpec{
			Groups: []gina.GroupSpec{{ID: 0, RestartMax: 100000, WindowTicks: 1}},
			Boot:   []gina.SpawnSpec{{Type: 1, Group: 0}},
		}
	}
	return gina.SystemSpec{Types: []gina.TypeDesc{gina.RegisterType(1, opts(4), nil, h)}, Shards: sh}
}

func runGossip(t *testing.T, seed uint64) (hash, events uint64) {
	t.Helper()
	cfg := gina.SimConfig{Faults: gina.FaultConfig{Drop: gina.Ratio{Num: 1, Den: 20}, Crash: gina.Ratio{Num: 1, Den: 50}}}
	s := newSim(t, gossipSpec(4), seed, cfg)
	for i := 0; i < 4; i++ {
		for k := 0; k < 3; k++ {
			s.Sys.Send(gina.MakeHandle(uint8(i), 1, 0, 1), tagStart, nil)
		}
	}
	run(t, s, 400)
	return s.Trace.Hash(), s.Trace.Events()
}

func TestSimulationDeterminismAndInvariants(t *testing.T) {
	distinct := map[uint64]bool{}
	for seed := uint64(1); seed <= 30; seed++ {
		h1, e1 := runGossip(t, seed)
		h2, e2 := runGossip(t, seed)
		if h1 != h2 || e1 != e2 {
			t.Fatalf("seed %d is not reproducible: %#x/%d vs %#x/%d", seed, h1, e1, h2, e2)
		}
		if e1 == 0 {
			t.Fatalf("seed %d did no work", seed)
		}
		distinct[h1] = true
	}
	if len(distinct) < 25 {
		t.Fatalf("only %d distinct traces across 30 seeds: seeds are not steering the run", len(distinct))
	}
}

func TestPartitionDropsCrossShardTraffic(t *testing.T) {
	finalN = 0
	s := newSim(t, playerSpec(2), 1, gina.SimConfig{})
	tmp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
	a, _ := s.Sys.Spawn(0, tmp)
	b, _ := s.Sys.Spawn(1, tmp)
	s.Faults.Partition(0, 1, true)
	gina.SendTo(s.Sys, b, tagStart, &startMsg{Peer: a, Limit: 10})
	gina.SendTo(s.Sys, a, tagStart, &startMsg{Peer: b, Limit: 10, Serve: 1})
	run(t, s, 100)
	if finalN != 0 {
		t.Fatal("ball crossed a partition")
	}
	s.Faults.Partition(0, 1, false)
	gina.SendTo(s.Sys, a, tagStart, &startMsg{Peer: b, Limit: 10, Serve: 1})
	run(t, s, 100)
	if finalN != 10 {
		t.Fatalf("after healing final ball = %d, want 10", finalN)
	}
}

// ---- allocation and rule checks ----

func TestEngineHotPathIsAllocationFree(t *testing.T) {
	sys, err := gina.NewSystem(playerSpec(2), gina.Options{Clock: &gina.SimClock{}})
	if err != nil {
		t.Fatal(err)
	}
	tmp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
	a, _ := sys.Spawn(0, tmp)
	b, _ := sys.Spawn(1, tmp)
	gina.SendTo(sys, b, tagStart, &startMsg{Peer: a, Limit: 1 << 62})
	gina.SendTo(sys, a, tagStart, &startMsg{Peer: b, Limit: 1 << 62, Serve: 1})
	for i := 0; i < 200; i++ {
		sys.Step()
	}
	if n := testing.AllocsPerRun(200, func() { sys.Step() }); n != 0 {
		t.Fatalf("Step allocated %v times per run, want 0", n)
	}
	if sys.Shard(0).Stats().Turns == 0 || sys.Shard(1).Stats().Turns == 0 {
		t.Fatal("no turns ran")
	}
}

func BenchmarkPingPongTwoShards(b *testing.B) {
	sys, _ := gina.NewSystem(playerSpec(2), gina.Options{Clock: &gina.SimClock{}})
	tmp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
	x, _ := sys.Spawn(0, tmp)
	y, _ := sys.Spawn(1, tmp)
	gina.SendTo(sys, y, tagStart, &startMsg{Peer: x, Limit: 1 << 62})
	gina.SendTo(sys, x, tagStart, &startMsg{Peer: y, Limit: 1 << 62, Serve: 1})
	sys.Step()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sys.Step() // one round = each shard runs one turn = 2 messages delivered
	}
}

// BenchmarkSpawnAndExit measures the full isolate lifecycle: spawn (init handler),
// first message, Done, supervision bookkeeping and slot teardown.
func BenchmarkSpawnAndExit(b *testing.B) {
	h := func(self *sink, ctx *gina.Ctx, m *gina.Message) gina.Effect { return gina.Done() }
	spec := gina.SystemSpec{Types: []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 4}, nil, h)}, Shards: make([]gina.ShardSpec, 1)}
	sys, _ := gina.NewSystem(spec, gina.Options{Clock: &gina.SimClock{}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hd, _ := sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
		sys.Send(hd, tagWork, nil)
		sys.Step()
	}
}
