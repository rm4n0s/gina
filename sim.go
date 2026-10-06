package gina

import (
	"fmt"

	"gina/internal/prng"
)

// Ratio is an integer probability Num/Den (no floats, so results are identical
// on every platform). The zero Ratio never fires.
type Ratio struct{ Num, Den uint32 }

// FaultConfig lists the faults the simulator can inject. All default to off.
type FaultConfig struct {
	Drop  Ratio // cross-shard message silently lost
	Crash Ratio // handler turn panics before running
}

// Faults is the fault engine. Each fault domain draws from its own PRNG child so
// enabling one never perturbs another.
type Faults struct {
	cfg      FaultConfig
	dropRNG  *prng.Rand
	crashRNG *prng.Rand
	blocked  [][]bool
}

func NewFaults(cfg FaultConfig, tree prng.Tree) *Faults {
	return &Faults{cfg: cfg, dropRNG: tree.Child("fault.drop"), crashRNG: tree.Child("fault.crash")}
}

// Partition blocks (or heals) traffic between two shards in both directions.
func (f *Faults) Partition(a, b int, on bool) {
	n := max(a, b) + 1
	for len(f.blocked) < n {
		f.blocked = append(f.blocked, nil)
	}
	for i := range f.blocked {
		for len(f.blocked[i]) < n {
			f.blocked[i] = append(f.blocked[i], false)
		}
	}
	f.blocked[a][b], f.blocked[b][a] = on, on
}

func (f *Faults) dropRemote(from, to uint8) bool {
	if int(from) < len(f.blocked) && int(to) < len(f.blocked[from]) && f.blocked[from][to] {
		return true
	}
	return f.dropRNG.Roll(f.cfg.Drop.Num, f.cfg.Drop.Den)
}

func (f *Faults) rollCrash() bool { return f.crashRNG.Roll(f.cfg.Crash.Num, f.cfg.Crash.Den) }

// Trace folds every dispatched turn into an FNV-1a hash. Same seed + same
// config must give the same hash.
type Trace struct{ hash, events uint64 }

func NewTrace() *Trace { return &Trace{hash: 14695981039346656037} }

func (t *Trace) mix(w uint64) {
	for i := 0; i < 8; i++ {
		t.hash ^= w & 0xff
		t.hash *= 1099511628211
		w >>= 8
	}
}

func (t *Trace) add(tick uint64, shard uint8, typ TypeID, slot uint32, tag Tag, eff Effect) {
	t.mix(tick)
	t.mix(uint64(shard)<<48 | uint64(typ)<<40 | uint64(slot))
	t.mix(uint64(tag)<<16 | uint64(eff.Kind)<<8 | uint64(eff.Fault))
	t.events++
}

func (t *Trace) Hash() uint64   { return t.hash }
func (t *Trace) Events() uint64 { return t.events }

// SimConfig configures a deterministic simulation.
type SimConfig struct {
	TickNS     uint64 // simulated nanoseconds per round (default 1ms)
	CheckEvery int    // run invariants every N rounds (default 1)
	Faults     FaultConfig
}

// Sim runs a System on a simulated clock with shuffled shard order, fault
// injection and invariant checking. Everything derives from one seed.
type Sim struct {
	Sys    *System
	Clock  *SimClock
	Trace  *Trace
	Faults *Faults
	Seed   uint64
	cfg    SimConfig
	rounds int
}

func NewSim(spec SystemSpec, seed uint64, cfg SimConfig) (*Sim, error) {
	if cfg.TickNS == 0 {
		cfg.TickNS = 1_000_000
	}
	if cfg.CheckEvery == 0 {
		cfg.CheckEvery = 1
	}
	tree := prng.NewTree(seed)
	s := &Sim{Clock: &SimClock{}, Trace: NewTrace(), Seed: seed, cfg: cfg}
	s.Faults = NewFaults(cfg.Faults, tree)
	sys, err := NewSystem(spec, Options{Clock: s.Clock, Trace: s.Trace, Faults: s.Faults, Shuffle: tree.Child("sched")})
	if err != nil {
		return nil, err
	}
	s.Sys = sys
	return s, nil
}

// Run executes up to maxRounds rounds, checking invariants, and stops early when
// the system is idle (jumping the simulated clock over idle gaps to the next
// timer). Errors carry the seed so a failure can be replayed.
func (s *Sim) Run(maxRounds int) (idle bool, err error) {
	for i := 0; i < maxRounds; i++ {
		progress := s.Sys.Step()
		s.rounds++
		s.Clock.Advance(s.cfg.TickNS)
		if s.rounds%s.cfg.CheckEvery == 0 {
			if err := s.Sys.CheckInvariants(); err != nil {
				return false, fmt.Errorf("sim seed=%d round=%d: %w", s.Seed, s.rounds, err)
			}
		}
		if progress || s.Sys.busy() {
			continue
		}
		due, ok := s.Sys.nextTimer()
		if !ok {
			return true, nil
		}
		s.Clock.WaitUntil(due)
	}
	return false, nil
}
