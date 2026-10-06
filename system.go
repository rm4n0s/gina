package gina

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"gina/internal/prng"
)

// Clock supplies monotonic nanoseconds. Production uses RealClock; the
// simulator uses SimClock so time is a pure function of the run.
type Clock interface{ Now() uint64 }

// Waiter is an optional Clock capability: advance (or sleep) until ns.
type Waiter interface{ WaitUntil(ns uint64) }

type RealClock struct{ start time.Time }

func NewRealClock() *RealClock { return &RealClock{start: time.Now()} }

func (c *RealClock) Now() uint64 { return uint64(time.Since(c.start)) }
func (c *RealClock) WaitUntil(ns uint64) {
	if now := c.Now(); ns > now {
		time.Sleep(time.Duration(ns - now))
	}
}

type SimClock struct{ now uint64 }

func (c *SimClock) Now() uint64         { return c.now }
func (c *SimClock) WaitUntil(ns uint64) { c.now = max(c.now, ns) }
func (c *SimClock) Advance(d uint64)    { c.now += d }

// Options are the seams the simulator plugs into; all are optional.
type Options struct {
	Clock   Clock
	Trace   *Trace
	Faults  *Faults
	Shuffle *prng.Rand // when set, shard order is shuffled every round
}

// System is a set of shards driven cooperatively on the calling thread.
type System struct {
	spec    SystemSpec
	shards  []*Shard
	clock   Clock
	trace   *Trace
	faults  *Faults
	shuffle *prng.Rand
	order   []uint8
	rounds  uint64
	iop     ioPoller
	stopReq atomic.Bool // set by Stop or Ctx.StopSystem; threaded shards then shut down
	run     *runState   // non-nil once Start was called
}

func NewSystem(spec SystemSpec, opt Options) (*System, error) {
	spec, err := spec.normalize()
	if err != nil {
		return nil, err
	}
	sys := &System{spec: spec, clock: opt.Clock, trace: opt.Trace, faults: opt.Faults, shuffle: opt.Shuffle}
	if sys.clock == nil {
		sys.clock = NewRealClock()
	}
	n := len(spec.Shards)
	for i := 0; i < n; i++ {
		sh, err := newShard(sys, uint8(i), spec)
		if err != nil {
			sys.Close()
			return nil, err
		}
		sys.shards = append(sys.shards, sh)
		sys.order = append(sys.order, uint8(i))
	}
	for i, a := range sys.shards {
		a.in, a.out = make([]*ring, n), make([]*ring, n)
		for j := range sys.shards {
			if i != j {
				a.out[j] = newRing(spec.RingSize)
			}
		}
	}
	for i, a := range sys.shards {
		for j, b := range sys.shards {
			if i != j {
				b.in[i] = a.out[j]
			}
		}
	}
	if err := sys.initIO(); err != nil {
		sys.Close()
		return nil, err
	}
	old := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(old)
	for _, sh := range sys.shards {
		if err := sh.bootSpawns(); err != nil {
			sys.Close()
			return nil, err
		}
	}
	return sys, nil
}

func (sys *System) Shard(i int) *Shard { return sys.shards[i] }
func (sys *System) ShardCount() int    { return len(sys.shards) }
func (sys *System) Rounds() uint64     { return sys.rounds }
func (sys *System) Clock() Clock       { return sys.clock }

// BootHandle returns the current handle of the i-th boot isolate on a shard. It
// changes after a Level-2 reset, so re-read it instead of caching it.
func (sys *System) BootHandle(shard, i int) Handle {
	hs := sys.shards[shard].bootHandles
	if i >= len(hs) {
		return 0
	}
	return hs[i]
}

// Spawn starts an isolate on a shard from outside the system.
func (sys *System) Spawn(shard int, sp SpawnSpec) (Handle, SpawnError) {
	sys.assertStopped("Spawn")
	old := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(old)
	return sys.shards[shard].spawn(&sp, 0)
}

// Send injects a message from outside the system.
func (sys *System) Send(to Handle, tag Tag, payload []byte) SendResult {
	sys.assertStopped("Send")
	if len(payload) > MaxPayload {
		return SendPayloadTooLarge
	}
	if int(to.Shard()) >= len(sys.shards) || sys.shards[to.Shard()].quarantined.Load() {
		return SendStaleHandle
	}
	var m Message
	m.Tag = tag
	m.PayloadSize = uint16(copy(m.Payload[:], payload))
	return sys.shards[to.Shard()].enqueue(to, &m, nil, false)
}

// SendTo injects a typed payload from outside the system.
func SendTo[P any](sys *System, to Handle, tag Tag, p *P) SendResult {
	checkPOD[P](MaxPayload)
	return sys.Send(to, tag, BytesOf(p))
}

// Step runs one round: every shard ticks once. It reports whether any shard
// made progress.
func (sys *System) Step() bool {
	old := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(old)
	if sys.shuffle != nil {
		sys.shuffle.ShuffleU8(sys.order)
	}
	sys.assertStopped("Step")
	progress := false
	for _, id := range sys.order {
		if sys.shards[id].Tick() {
			progress = true
		}
	}
	sys.rounds++
	return progress
}

func (sys *System) busy() bool {
	for _, sh := range sys.shards {
		if sh.pending() {
			return true
		}
	}
	return false
}

func (sys *System) nextTimer() (uint64, bool) {
	var best uint64
	found := false
	for _, sh := range sys.shards {
		if sh.quarantined.Load() {
			continue
		}
		if d, ok := sh.timers.peek(); ok && (!found || d < best) {
			best, found = d, true
		}
	}
	return best, found
}

// RunUntilIdle steps until no shard has work, no timer is pending and no I/O
// operation is outstanding. When idle with timers or I/O pending it blocks in
// epoll_wait (real clock) or jumps the clock (simulated clock) instead of
// spinning. A server therefore never returns from it until it has nothing left
// to serve. It also returns after maxRounds.
func (sys *System) RunUntilIdle(maxRounds int) (rounds int, idle bool) {
	for rounds < maxRounds {
		progress := sys.Step()
		rounds++
		if progress || sys.busy() {
			continue
		}
		due, hasTimer := sys.nextTimer()
		io := sys.pendingIO()
		if !hasTimer && !io {
			return rounds, true
		}
		if rc, real := sys.clock.(*RealClock); real {
			timeout := -1
			if hasTimer {
				timeout = 0
				if now := rc.Now(); due > now {
					timeout = int((due - now + 999_999) / 1_000_000) // round up to ms
				}
			}
			if io {
				sys.waitIO(timeout)
			} else {
				rc.WaitUntil(due)
			}
			continue
		}
		if w, ok := sys.clock.(Waiter); ok && hasTimer {
			w.WaitUntil(due)
			continue
		}
		return rounds, true // simulated clock with only I/O pending: nothing can progress
	}
	return rounds, false
}

func (sys *System) pendingIO() bool {
	for _, sh := range sys.shards {
		if !sh.quarantined.Load() && sh.io.active() > 0 {
			return true
		}
	}
	return false
}

// Listen creates a listening socket in a shard's fd table from outside the
// system and returns it with its bound port. Pass it to an isolate with
// SpawnSpec.HandoffFD.
func (sys *System) Listen(shard int, spec ListenSpec) (FDHandle, uint16, error) {
	sh := sys.shards[shard]
	fd, err := sh.io.listen(spec)
	if err != nil {
		return 0, 0, err
	}
	return fd, sh.io.localPort(fd), nil
}

// Close releases the system's epoll instance and every open socket.
func (sys *System) Close() {
	for _, sh := range sys.shards {
		sh.io.reset()
		sh.io.close()
	}
	sys.closeIO()
}

// Shutdown delivers TagShutdown to every isolate, runs up to maxRounds for them
// to finish, then force-frees stragglers. It returns how many had to be forced.
func (sys *System) Shutdown(maxRounds int) (forced int) {
	for _, sh := range sys.shards {
		if !sh.quarantined.Load() {
			sh.beginShutdown()
		}
	}
	for i := 0; i < maxRounds; i++ {
		sys.Step()
		left := 0
		for _, sh := range sys.shards {
			left += sh.live
		}
		if left == 0 {
			break
		}
	}
	for _, sh := range sys.shards {
		forced += sh.forceStop()
	}
	return forced
}

// Revive rebuilds a quarantined (or any) shard from its boot spec.
func (sys *System) Revive(shard int) error {
	old := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(old)
	sh := sys.shards[shard]
	sh.quarantined.Store(false)
	sh.resetBudget = newBudget(sys.spec.ResetMax, sys.spec.ResetWindow)
	sh.epoch++
	sh.wipe()
	return sh.bootSpawns()
}

// CheckInvariants verifies every shard's structural invariants.
func (sys *System) CheckInvariants() error {
	for _, sh := range sys.shards {
		if err := sh.check(); err != nil {
			return fmt.Errorf("invariant: %w", err)
		}
	}
	return nil
}
