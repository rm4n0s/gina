package gina_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

const tagExt gina.Tag = gina.TagUserBase + 40

type extMsgBody struct{ Producer, Seq uint32 }

// extSink counts what it receives and checks per-producer order.
type extSink struct {
	last [8]int64
	bad  *atomic.Int64
	got  *atomic.Int64
	want int64
	big  *atomic.Int64
}

type extSinkArgs struct{ _ [8]byte }

func TestSendExternalWakesSleepingShardsInOrder(t *testing.T) {
	const producers, per = 4, 2000
	var bad, got, big atomic.Int64
	sink := gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 8192}, nil,
		func(self *extSink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
			switch m.Tag {
			case tagExt:
				b := gina.PayloadAs[extMsgBody](m)
				if int64(b.Seq) != self.last[b.Producer] { // last counts what this producer has delivered
					bad.Add(1)
				}
				self.last[b.Producer]++
				got.Add(1)
			case tagExt + 1: // large message
				if len(ctx.Data()) == 100_000 {
					big.Add(1)
				}
			case gina.TagShutdown:
				return gina.Done()
			}
			return gina.WaitMessage()
		})
	sys, err := gina.NewSystem(gina.SystemSpec{
		Types:     []gina.TypeDesc{sink},
		Shards:    []gina.ShardSpec{{}, {}},
		PoolSlots: 16384,
	}, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h, _ := sys.Spawn(1, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})

	sys.Start(gina.RunOptions{SpinFor: -1}) // shards sleep in epoll between bursts
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p uint32) {
			defer wg.Done()
			for i := uint32(0); i < per; i++ {
				for gina.SendExternalTo(sys, h, tagExt, &extMsgBody{p, i}) != gina.SendOK {
					time.Sleep(50 * time.Microsecond) // mailbox is bounded: back off and retry
				}
				if i%500 == 0 {
					time.Sleep(2 * time.Millisecond) // let the shard fall asleep again
				}
			}
		}(uint32(p))
	}
	wg.Wait()
	if r := sys.SendExternal(h, tagExt+1, make([]byte, 100_000)); r != gina.SendOK {
		t.Fatalf("large send: %v", r)
	}
	deadline := time.Now().Add(5 * time.Second)
	for (got.Load() < producers*per || big.Load() < 1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	sys.Stop()
	d := sys.Shard(1).Stats().Dropped
	if g := got.Load(); g+int64(d) < producers*per || bad.Load() != 0 {
		t.Fatalf("received %d of %d (dropped %d), out of order: %d", g, producers*per, d, bad.Load())
	}
	if d != 0 {
		t.Logf("note: %d dropped on a full mailbox", d)
	}
	if big.Load() != 1 {
		t.Fatalf("large external message not delivered")
	}
}

func TestSendExternalLatencyFromSleep(t *testing.T) {
	var at atomic.Int64
	typ := gina.RegisterType(1, gina.TypeOptions{SlotCount: 2}, nil,
		func(self *extSink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
			if m.Tag == tagExt {
				at.Store(time.Now().UnixNano())
			}
			if m.Tag == gina.TagShutdown {
				return gina.Done()
			}
			return gina.WaitMessage()
		})
	sys, _ := gina.NewSystem(gina.SystemSpec{Types: []gina.TypeDesc{typ}, Shards: make([]gina.ShardSpec, 2)}, gina.Options{})
	h, _ := sys.Spawn(1, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	sys.Start(gina.RunOptions{SpinFor: -1})
	defer sys.Stop()
	time.Sleep(50 * time.Millisecond)
	var worst time.Duration
	for i := 0; i < 20; i++ {
		at.Store(0)
		t0 := time.Now()
		sys.SendExternal(h, tagExt, nil)
		for at.Load() == 0 && time.Since(t0) < time.Second {
			time.Sleep(10 * time.Microsecond)
		}
		if at.Load() == 0 {
			t.Fatal("never delivered (lost wake-up)")
		}
		if d := time.Duration(at.Load() - t0.UnixNano()); d > worst {
			worst = d
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("worst wake-up latency from a sleeping shard: %v", worst)
	if worst > 50*time.Millisecond {
		t.Fatalf("latency %v: shard was not woken promptly", worst)
	}
}

func TestSendExternalCooperativeDriver(t *testing.T) {
	var got atomic.Int64
	typ := gina.RegisterType(1, gina.TypeOptions{SlotCount: 2}, nil,
		func(self *extSink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
			if m.Tag == tagExt {
				got.Add(1)
			}
			return gina.WaitMessage()
		})
	sys, _ := gina.NewSystem(gina.SystemSpec{Types: []gina.TypeDesc{typ}, Shards: make([]gina.ShardSpec, 1)}, gina.Options{})
	h, _ := sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	if r := sys.SendExternal(h, tagExt, nil); r != gina.SendOK {
		t.Fatal(r)
	}
	sys.RunUntilIdle(10)
	if got.Load() != 1 {
		t.Fatalf("got %d", got.Load())
	}
}

func TestTakeLostCountsDroppedMessages(t *testing.T) {
	var lost atomic.Int64
	typ := gina.RegisterType(1, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 2}, nil,
		func(self *extSink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
			lost.Add(int64(ctx.TakeLost()))
			if again := ctx.TakeLost(); again != 0 {
				t.Errorf("TakeLost did not reset: %d", again)
			}
			return gina.WaitMessage()
		})
	sys, _ := gina.NewSystem(gina.SystemSpec{Types: []gina.TypeDesc{typ}, Shards: make([]gina.ShardSpec, 1)}, gina.Options{})
	h, _ := sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
	sys.RunUntilIdle(5) // the init turn
	for i := 0; i < 5; i++ {
		sys.Send(h, tagExt, nil) // 2 fit, 3 are dropped
	}
	sys.RunUntilIdle(10)
	if lost.Load() != 3 {
		t.Fatalf("lost = %d, want 3", lost.Load())
	}
}
