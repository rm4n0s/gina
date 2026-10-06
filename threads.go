// This is the thread host: the one place in the engine that starts goroutines.
// Everything else stays single-threaded per shard.

package gina

import (
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

// RunOptions configures System.Start / Run.
type RunOptions struct {
	// Pin binds shard i's thread to CPU (CPUBase+i) modulo the CPU count, like
	// Tina's target_core. Linux only; ignored elsewhere.
	Pin     bool
	CPUBase int
	// ShutdownGrace is how long Stop waits for isolates to handle TagShutdown
	// before freeing them (default 5s).
	ShutdownGrace time.Duration
	// SpinFor is how long an idle shard polls its rings before it blocks in the
	// kernel. A peer usually answers within a microsecond, and blocking and waking
	// a thread costs a couple of microseconds, so a short spin makes cross-shard
	// round trips several times faster at the price of that much CPU after each
	// burst of work. 0 means the default (20µs); negative disables spinning.
	SpinFor time.Duration
}

type runState struct {
	wg   sync.WaitGroup
	opts RunOptions
}

// Start runs every shard on its own OS thread, Tina's thread-per-core model: one
// goroutine per shard, locked to its thread (and optionally pinned to a core),
// each owning its isolates, pools, timers and sockets outright. The only memory
// shards share is the ring matrix (atomic cursors) and each shard's wake flag.
//
// While the system runs, only isolates may touch it: Step, Spawn and Send panic.
// Use Stop (or Ctx.StopSystem from an isolate) to end it, then Wait.
// User handlers run on several threads at once, so state they share (as opposed
// to state in their own isolate) must be safe for concurrent use.
func (sys *System) Start(o RunOptions) {
	if sys.run != nil {
		panic("gina: System.Start called twice")
	}
	if o.ShutdownGrace == 0 {
		o.ShutdownGrace = 5 * time.Second
	}
	rs := &runState{opts: o}
	sys.run = rs
	for i, sh := range sys.shards {
		rs.wg.Add(1)
		go sh.thread(rs, i)
	}
}

// Wait blocks until every shard thread has exited.
func (sys *System) Wait() {
	if sys.run != nil {
		sys.run.wg.Wait()
	}
}

// Run is Start followed by Wait: it serves until Stop or Ctx.StopSystem.
func (sys *System) Run(o RunOptions) {
	sys.Start(o)
	sys.Wait()
}

// Stop asks every shard to shut down gracefully (TagShutdown to every isolate,
// then force-free whatever is left after ShutdownGrace) and waits for them.
func (sys *System) Stop() {
	sys.requestStop()
	sys.Wait()
}

func (sys *System) requestStop() {
	sys.stopReq.Store(true)
	for _, sh := range sys.shards {
		sh.io.wake() // unconditional: the thread may be between "announce sleep" and "block"
	}
}

// StopSystem ends the whole system from inside an isolate (e.g. when a job is
// done). It returns immediately; shards finish their current tick first.
func (c *Ctx) StopSystem() { c.s.sys.requestStop() }

func (sys *System) assertStopped(op string) {
	if sys.run != nil {
		panic("gina: " + op + " while the system is running: shards own their state; call it before Start or after Wait")
	}
}

func (s *Shard) thread(rs *runState, index int) {
	defer rs.wg.Done()
	s.spin = rs.opts.SpinFor
	if s.spin == 0 {
		s.spin = 20 * time.Microsecond
	}
	runtime.LockOSThread() // never unlocked: the thread exits with the goroutine
	if rs.opts.Pin {
		pinToCPU((rs.opts.CPUBase + index) % runtime.NumCPU())
	}
	debug.SetPanicOnFault(true) // per goroutine: faults in handlers become recoverable panics

	shutting := false
	var deadline uint64
	for {
		if s.sys.stopReq.Load() && !shutting {
			shutting = true
			s.beginShutdown()
			deadline = s.sys.clock.Now() + uint64(rs.opts.ShutdownGrace)
		}
		progress := s.Tick()
		if shutting && (s.live == 0 || s.sys.clock.Now() > deadline) {
			s.forceStop()
			for i, r := range s.out { // let peers drain what this shard sent last
				if r != nil && r.publish() {
					s.sys.shards[i].wake()
				}
			}
			return
		}
		if progress || s.hasWork() {
			continue
		}
		s.idle()
	}
}

// idle blocks until there is work: a message in a ring, a ready socket, a due
// timer, or a stop request. The order matters: announce sleep FIRST, then look
// for work one last time, then block; a sender publishes FIRST, then looks at
// the announcement. With sequentially consistent atomics at least one side
// sees the other, so a wake-up cannot be lost.
func (s *Shard) idle() {
	timeout := -1
	if due, ok := s.timers.peek(); ok {
		now := s.sys.clock.Now()
		if due <= now {
			return
		}
		timeout = int((due - now + 999_999) / 1_000_000) // round up to whole milliseconds
	}
	if s.spin > 0 { // poll briefly before paying for a kernel sleep
		start := time.Now()
		for i := 1; ; i++ {
			if s.hasWork() || s.sys.stopReq.Load() {
				return
			}
			if i&15 == 0 && time.Since(start) >= s.spin {
				break
			}
		}
	}
	s.sleeping.Store(1)
	if s.hasWork() || s.sys.stopReq.Load() {
		s.sleeping.Store(0)
		return
	}
	s.io.poll(timeout)
	s.sleeping.Store(0)
}
