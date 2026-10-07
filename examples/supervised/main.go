// A boss isolate supervises a worker that keeps crashing. The trap boundary turns
// each panic into a restart; when the restart budget is exhausted the shard does a
// Level-2 reset and rebuilds from its boot spec.
package main

import (
	"fmt"

	"github.com/rm4n0s/gina"
)

const (
	tagBoom = gina.TagUserBase
	tagWork = gina.TagUserBase + 1
)

type boss struct{ worker gina.Handle }
type worker struct{ jobs int }

func bossInit(self *boss, ctx *gina.Ctx, _ []byte) gina.Effect {
	self.worker, _ = ctx.Spawn(gina.SpawnSpec{Type: 2, Group: gina.GroupRoot})
	fmt.Printf("  boss up (shard %d), worker %#x\n", ctx.ShardID(), uint64(self.worker))
	return gina.WaitMessage()
}

func bossHandler(self *boss, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case gina.TagChildExit:
		ce := gina.PayloadAs[gina.ChildExit](m)
		fmt.Printf("  boss: worker %#x exited (kind %d), replacement %#x\n", uint64(ce.Old), ce.Kind, uint64(ce.New))
		self.worker = ce.New
	case tagBoom, tagWork:
		ctx.SendRaw(self.worker, m.Tag, nil)
	}
	return gina.WaitMessage()
}

func workerHandler(self *worker, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagWork:
		self.jobs++
	case tagBoom:
		var p *int
		*p = 42 // nil dereference
	}
	return gina.WaitMessage()
}

func main() {
	sys, err := gina.NewSystem(gina.SystemSpec{
		Types: []gina.TypeDesc{
			gina.RegisterType(1, gina.TypeOptions{SlotCount: 2}, bossInit, bossHandler),
			gina.RegisterType(2, gina.TypeOptions{SlotCount: 4}, nil, workerHandler),
		},
		Shards: []gina.ShardSpec{{
			Groups: []gina.GroupSpec{{ID: gina.GroupRoot, Strategy: gina.OneForOne, RestartMax: 3, WindowTicks: 1000}},
			Boot:   []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}},
		}},
	}, gina.Options{Clock: &gina.SimClock{}})
	if err != nil {
		panic(err)
	}
	for i := 1; i <= 5; i++ {
		fmt.Printf("boom #%d\n", i)
		sys.Send(sys.BootHandle(0, 0), tagBoom, nil)
		sys.RunUntilIdle(100)
		st := sys.Shard(0).Stats()
		fmt.Printf("  panics=%d restarts=%d budgetExceeded=%d level2Resets=%d live=%d\n",
			st.Panics, st.Restarts, st.BudgetExceeded, st.Resets, sys.Shard(0).Live())
	}
}
