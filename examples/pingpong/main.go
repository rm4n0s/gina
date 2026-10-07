// Ping-pong between two shards: a ball is passed back and forth through the
// cross-shard rings until it reaches the limit.
//
//	go run ./examples/pingpong                 # two shards on two OS threads (Tina's model)
//	go run ./examples/pingpong -threads=false  # both shards cooperatively on one thread
package main

import (
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rm4n0s/gina"
)

const (
	tagStart = gina.TagUserBase
	tagBall  = gina.TagUserBase + 1
)

type args struct {
	Peer  gina.Handle
	Limit uint64
	Serve uint64
}
type ballMsg struct{ N uint64 }
type player struct {
	peer  gina.Handle
	limit uint64
}

var final, startedAt, finishedAt atomic.Uint64

func init0(self *player, ctx *gina.Ctx, raw []byte) gina.Effect {
	a := gina.ArgsAs[args](raw)
	self.peer, self.limit = a.Peer, a.Limit
	if a.Serve == 1 {
		ctx.RegisterTimer(10*time.Millisecond, tagStart) // start once both threads are idle
	}
	return gina.WaitMessage()
}

func handler(self *player, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagStart:
		startedAt.Store(uint64(time.Now().UnixNano()))
		gina.Send(ctx, self.peer, tagBall, &ballMsg{1})
	case tagBall:
		n := gina.PayloadAs[ballMsg](m).N
		if n >= self.limit {
			final.Store(n)
			finishedAt.Store(uint64(time.Now().UnixNano()))
			ctx.StopSystem()
			return gina.Done()
		}
		gina.Send(ctx, self.peer, tagBall, &ballMsg{n + 1})
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

func main() {
	threads := flag.Bool("threads", true, "run each shard on its own OS thread")
	limit := flag.Uint64("n", 1_000_000, "number of hops")
	pin := flag.Bool("pin", false, "pin the shard threads to CPUs 0 and 1")
	spin := flag.Duration("spin", 0, "idle spin before a shard sleeps (0 = default 20µs, negative = never spin)")
	flag.Parse()

	boot := func(peerShard uint8, serve uint64) []gina.SpawnSpec {
		sp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
		return []gina.SpawnSpec{gina.WithArgs(sp, &args{Peer: gina.MakeHandle(peerShard, 1, 0, 1), Limit: *limit, Serve: serve})}
	}
	sys, err := gina.NewSystem(gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 4}, init0, handler)},
		Shards: []gina.ShardSpec{{Boot: boot(1, 1)}, {Boot: boot(0, 0)}},
	}, gina.Options{})
	if err != nil {
		panic(err)
	}
	defer sys.Close()

	if *threads {
		sys.Run(gina.RunOptions{Pin: *pin, SpinFor: *spin})
	} else {
		sys.RunUntilIdle(int(*limit) * 4) // single thread: the timer fires, the ball flies, StopSystem is a no-op here
	}
	el := time.Duration(int64(finishedAt.Load()) - int64(startedAt.Load()))
	mode := "one thread, cooperative"
	if *threads {
		mode = "two OS threads"
	}
	fmt.Printf("%s: final ball=%d in %v: %.2f M hops/s, %.2f µs per hop\n",
		mode, final.Load(), el.Round(time.Millisecond), float64(final.Load())/el.Seconds()/1e6, el.Seconds()*1e6/float64(final.Load()))
}
