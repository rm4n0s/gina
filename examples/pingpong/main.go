// Ping-pong between two shards: a ball is passed back and forth through the
// cross-shard rings until it reaches the limit.
package main

import (
	"fmt"
	"time"

	"gina"
)

const (
	tagStart = gina.TagUserBase
	tagBall  = gina.TagUserBase + 1
	limit    = 1_000_000
)

type startMsg struct {
	Peer         gina.Handle
	Limit, Serve uint64
}
type ballMsg struct{ N uint64 }
type player struct{ peer, limit uint64 }

var final uint64

func handler(self *player, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagStart:
		s := gina.PayloadAs[startMsg](m)
		self.peer, self.limit = uint64(s.Peer), s.Limit
		if s.Serve == 1 {
			gina.Send(ctx, s.Peer, tagBall, &ballMsg{1})
		}
	case tagBall:
		n := gina.PayloadAs[ballMsg](m).N
		if n >= self.limit {
			final = n
			return gina.Done()
		}
		gina.Send(ctx, gina.Handle(self.peer), tagBall, &ballMsg{n + 1})
	}
	return gina.WaitMessage()
}

func main() {
	sys, err := gina.NewSystem(gina.SystemSpec{
		Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 4}, nil, handler)},
		Shards: make([]gina.ShardSpec, 2),
	}, gina.Options{})
	if err != nil {
		panic(err)
	}
	tmp := gina.SpawnSpec{Type: 1, Group: gina.GroupRoot, Restart: gina.RestartTemporary}
	a, _ := sys.Spawn(0, tmp)
	b, _ := sys.Spawn(1, tmp)
	gina.SendTo(sys, b, tagStart, &startMsg{Peer: a, Limit: limit})
	gina.SendTo(sys, a, tagStart, &startMsg{Peer: b, Limit: limit, Serve: 1})

	t0 := time.Now()
	rounds, idle := sys.RunUntilIdle(10 * limit)
	el := time.Since(t0)
	fmt.Printf("final ball=%d idle=%v rounds=%d elapsed=%v (%.2fM msgs/s, 1 thread, 2 shards)\n",
		final, idle, rounds, el.Round(time.Millisecond), float64(final)/el.Seconds()/1e6)
}
