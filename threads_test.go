// Tests for the threaded runtime (System.Start): shards on their own OS threads
// talking through the rings. They use goroutines/channels only to supervise the
// system with a timeout.

package gina_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

// waitOrFail waits for the system to stop; a hang almost certainly means a lost wake-up.
func waitOrFail(t *testing.T, sys *gina.System, within time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { sys.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("system did not stop within %v (lost wake-up or deadlock?)", within)
	}
}

// ---- cross-thread ping-pong: every hop needs the peer to be woken from epoll_wait

type tArgs struct {
	Peer  gina.Handle
	Limit uint64
	Serve uint64
}

type tPlayer struct {
	peer  gina.Handle
	limit uint64
	serve bool
}

var tFinal, tFinishedAt atomic.Uint64

func tPlayerInit(self *tPlayer, ctx *gina.Ctx, args []byte) gina.Effect {
	a := gina.ArgsAs[tArgs](args)
	*self = tPlayer{peer: a.Peer, limit: a.Limit, serve: a.Serve == 1}
	if self.serve { // start late, so both threads are asleep and the first message must wake one
		ctx.RegisterTimer(5*time.Millisecond, tagStart)
	}
	return gina.WaitMessage()
}

func tPlayerHandler(self *tPlayer, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagStart:
		gina.Send(ctx, self.peer, tagBall, &ballMsg{1})
	case tagBall:
		n := gina.PayloadAs[ballMsg](m).N
		if n >= self.limit {
			tFinal.Store(n)
			tFinishedAt.Store(uint64(time.Now().UnixNano()))
			ctx.StopSystem()
			return gina.Done()
		}
		gina.Send(ctx, self.peer, tagBall, &ballMsg{n + 1})
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

func TestThreadsPingPongAcrossShards(t *testing.T) {
	// SpinFor<0: every hop sleeps in epoll_wait and must be woken through the eventfd
	// (the lost-wake-up stress). Default: the spin-then-sleep behaviour used in production.
	for _, c := range []struct {
		name string
		spin time.Duration
	}{{"always-sleep", -1}, {"default-spin", 0}} {
		t.Run(c.name, func(t *testing.T) { pingPong(t, c.spin) })
	}
}

func pingPong(t *testing.T, spin time.Duration) {
	const limit = 100_000
	tFinal.Store(0)
	boot := func(peerShard uint8, serve uint64) []gina.SpawnSpec {
		sp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
		return []gina.SpawnSpec{gina.WithArgs(sp, &tArgs{Peer: gina.MakeHandle(peerShard, 1, 0, 1), Limit: limit, Serve: serve})}
	}
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(2), tPlayerInit, tPlayerHandler)},
		Shards: []gina.ShardSpec{{Boot: boot(1, 1)}, {Boot: boot(0, 0)}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	start := time.Now()
	sys.Start(gina.RunOptions{SpinFor: spin})
	waitOrFail(t, sys, 60*time.Second)
	el := time.Duration(int64(tFinishedAt.Load()) - start.UnixNano())
	if tFinal.Load() != limit {
		t.Fatalf("final ball = %d, want %d", tFinal.Load(), limit)
	}
	t.Logf("%d cross-thread hops in %v (%.2f µs per hop)", limit, el.Round(time.Millisecond), float64(el.Microseconds())/limit)
}

// ---- all-to-all traffic: loss, reordering and ring-full handling across 4 threads

const (
	perPeer = 5000
	nShards = 4
)

type seqMsg struct{ From, Seq uint64 }

var (
	tReceived atomic.Uint64
	tOrderErr atomic.Uint64
)

type blaster struct {
	next [nShards]uint64
	id   uint8
}

func blasterInit(self *blaster, ctx *gina.Ctx, _ []byte) gina.Effect {
	self.id = ctx.ShardID()
	ctx.RegisterTimer(5*time.Millisecond, tagStart)
	return gina.WaitMessage()
}

func blasterHandler(self *blaster, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	left := false
	for round := 0; round < 64; round++ { // a bounded burst per turn, then yield so other work runs
		for peer := uint8(0); peer < nShards; peer++ {
			if peer == self.id || self.next[peer] >= perPeer {
				continue
			}
			msg := seqMsg{From: uint64(self.id), Seq: self.next[peer]}
			switch gina.Send(ctx, gina.MakeHandle(peer, 2, 0, 1), tagWork, &msg) {
			case gina.SendOK:
				self.next[peer]++
			case gina.SendRingFull, gina.SendMailboxFull, gina.SendPoolExhausted:
				// backpressure: try again next turn
			default:
				tOrderErr.Add(1)
			}
			if self.next[peer] < perPeer {
				left = true
			}
		}
	}
	if left {
		return gina.Yield()
	}
	return gina.Done()
}

type tSink struct{ expect [nShards]uint64 }

func tSinkHandler(self *tSink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	if m.Tag == gina.TagShutdown {
		return gina.Done()
	}
	msg := gina.PayloadAs[seqMsg](m)
	if msg.Seq != self.expect[msg.From] { // each ring is FIFO: any gap or reorder is a bug
		tOrderErr.Add(1)
	}
	self.expect[msg.From] = msg.Seq + 1
	if tReceived.Add(1) == nShards*(nShards-1)*perPeer {
		ctx.StopSystem()
	}
	return gina.WaitMessage()
}

func TestThreadsAllToAllNoLossNoReorder(t *testing.T) {
	tReceived.Store(0)
	tOrderErr.Store(0)
	boot := []gina.SpawnSpec{
		{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary},
		{Type: 2, Group: gina.GroupRoot, Restart: gina.RestartTemporary},
	}
	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{
			gina.RegisterType(1, opts(2), blasterInit, blasterHandler),
			gina.RegisterType(2, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 16384}, nil, tSinkHandler),
		},
		Shards:    make([]gina.ShardSpec, nShards),
		PoolSlots: 1 << 16,
	}
	for i := range spec.Shards {
		spec.Shards[i].Boot = boot
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	sys.Start(gina.RunOptions{})
	waitOrFail(t, sys, 60*time.Second)
	if got, want := tReceived.Load(), uint64(nShards*(nShards-1)*perPeer); got != want {
		t.Fatalf("received %d of %d messages", got, want)
	}
	if tOrderErr.Load() != 0 {
		t.Fatalf("%d ordering/send errors", tOrderErr.Load())
	}
	for i := 0; i < nShards; i++ {
		if d := sys.Shard(i).Stats().Dropped; d != 0 {
			t.Errorf("shard %d dropped %d inbound messages", i, d)
		}
	}
}

// ---- timers and stop on shards that are asleep

type sleeper struct{}

var tTimerFiredAt atomic.Int64

func TestThreadsTimerWakesSleepingShardAndStopIsPrompt(t *testing.T) {
	tTimerFiredAt.Store(0)
	init := func(self *sleeper, ctx *gina.Ctx, _ []byte) gina.Effect {
		ctx.RegisterTimer(60*time.Millisecond, tagTick)
		return gina.WaitMessage()
	}
	h := func(self *sleeper, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case tagTick:
			tTimerFiredAt.Store(time.Now().UnixNano())
		case gina.TagShutdown:
			return gina.Done()
		}
		return gina.WaitMessage()
	}
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(2), init, h)},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: 0}}}, {}, {}, {}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	started := time.Now()
	sys.Start(gina.RunOptions{})
	time.Sleep(150 * time.Millisecond) // every shard goes to sleep in epoll_wait
	fired := tTimerFiredAt.Load()
	if fired == 0 {
		t.Fatal("the timer never fired on a sleeping shard")
	}
	if d := time.Duration(fired - started.UnixNano()); d < 55*time.Millisecond || d > 140*time.Millisecond {
		t.Fatalf("timer fired after %v, want about 60ms", d)
	}
	stopAt := time.Now()
	sys.Stop()
	if d := time.Since(stopAt); d > 500*time.Millisecond {
		t.Fatalf("Stop took %v on idle shards: sleeping threads were not woken", d)
	}
}

// ---- graceful shutdown and fault containment on shard threads

type polite2 struct{}

func TestThreadsStopDeliversShutdownToEveryIsolate(t *testing.T) {
	var shutdowns atomic.Int64
	h := func(self *polite2, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		if m.Tag == gina.TagShutdown {
			shutdowns.Add(1)
			return gina.Done()
		}
		return gina.WaitMessage()
	}
	boot := []gina.SpawnSpec{{Type: 1, Group: 0}, {Type: 1, Group: 0}, {Type: 1, Group: 0}}
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(8), nil, h)},
		Shards: []gina.ShardSpec{{Boot: boot}, {Boot: boot}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	sys.Start(gina.RunOptions{ShutdownGrace: 2 * time.Second})
	time.Sleep(50 * time.Millisecond)
	sys.Stop()
	if shutdowns.Load() != 6 {
		t.Fatalf("%d of 6 isolates handled TagShutdown", shutdowns.Load())
	}
	for i := 0; i < 2; i++ {
		if sys.Shard(i).Live() != 0 {
			t.Errorf("shard %d still has %d live isolates", i, sys.Shard(i).Live())
		}
	}
}

type crasher struct{}

func TestThreadsPanicOnShardThreadIsContainedAndSupervised(t *testing.T) {
	var starts atomic.Int64
	init := func(self *crasher, ctx *gina.Ctx, _ []byte) gina.Effect {
		if starts.Add(1) == 1 {
			ctx.RegisterTimer(10*time.Millisecond, tagBoom)
		} else {
			ctx.RegisterTimer(10*time.Millisecond, tagDone) // second incarnation ends the test
		}
		return gina.WaitMessage()
	}
	h := func(self *crasher, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case tagBoom:
			var p *int
			*p = 1 // nil dereference on a shard thread
		case tagDone:
			ctx.StopSystem()
		case gina.TagShutdown:
			return gina.Done()
		}
		return gina.WaitMessage()
	}
	spec := gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, opts(2), init, h)},
		Shards: []gina.ShardSpec{{Boot: []gina.SpawnSpec{{Type: 1, Group: 0}}}, {}},
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	sys.Start(gina.RunOptions{})
	waitOrFail(t, sys, 10*time.Second)
	if st := sys.Shard(0).Stats(); st.Panics != 1 || st.Restarts != 1 {
		t.Fatalf("stats = %+v, want 1 panic and 1 restart", st)
	}
	if starts.Load() != 2 {
		t.Fatalf("isolate started %d times, want 2", starts.Load())
	}
}

func TestThreadsRejectUnsafeCallsWhileRunning(t *testing.T) {
	spec := gina.SystemSpec{Types: []gina.TypeDesc{gina.RegisterType(1, opts(2), nil, sinkHandler)}, Shards: make([]gina.ShardSpec, 1)}
	sys, _ := gina.NewSystem(spec, gina.Options{})
	defer sys.Close()
	sys.Start(gina.RunOptions{})
	defer sys.Stop()
	defer func() {
		if recover() == nil {
			t.Fatal("System.Spawn while running should panic: shards own their state")
		}
	}()
	sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
}
