package gina

import (
	"errors"
	"fmt"
)

type Handler[T any] func(self *T, ctx *Ctx, msg *Message) Effect
type InitHandler[T any] func(self *T, ctx *Ctx, args []byte) Effect

// TypeOptions sizes one isolate type on every shard.
type TypeOptions struct {
	SlotCount       int  // max concurrent isolates of this type per shard (required)
	MailboxCapacity int  // per-isolate FIFO depth (default 256)
	ChunkSize       int  // slots per allocation chunk, power of two (default 256)
	Eager           bool // allocate every chunk at boot (default: on first use)
}

type typeOps struct {
	init func(slot uint32, c *Ctx, args []byte) Effect
	run  func(slot uint32, c *Ctx, m *Message) Effect
	zero func(slot uint32)
}

// TypeDesc is the type-erased registration of an isolate type.
type TypeDesc struct {
	id    TypeID
	opts  TypeOptions
	build func(TypeOptions) typeOps
}

// RegisterType registers isolate state type T. T is an ordinary Go type and may
// hold pointers, slices, maps and strings; the GC owns it. init may be nil.
func RegisterType[T any](id TypeID, opts TypeOptions, init InitHandler[T], h Handler[T]) TypeDesc {
	if h == nil {
		panic("gina: nil handler")
	}
	return TypeDesc{id: id, opts: opts, build: func(o TypeOptions) typeOps {
		st := newSlab[T](o.SlotCount, o.ChunkSize, o.Eager)
		ops := typeOps{
			run:  func(slot uint32, c *Ctx, m *Message) Effect { return h(st.at(slot), c, m) },
			zero: func(slot uint32) { st.zero(slot) },
		}
		if init != nil {
			ops.init = func(slot uint32, c *Ctx, args []byte) Effect { return init(st.at(slot), c, args) }
		} else {
			ops.init = func(uint32, *Ctx, []byte) Effect { return WaitMessage() }
		}
		return ops
	}}
}

type Strategy uint8

const (
	OneForOne Strategy = iota
	OneForAll
	RestForOne
)

// GroupSpec is a supervision group. RestartMax restarts are allowed within
// WindowTicks shard ticks; one more escalates to a Level-2 shard reset.
type GroupSpec struct {
	ID          GroupID
	Strategy    Strategy
	RestartMax  int
	WindowTicks uint64
	MaxChildren int
}

type ShardSpec struct {
	Groups []GroupSpec // group 0 (root) is added with defaults when absent
	Boot   []SpawnSpec // spawned at start and re-spawned after a Level-2 reset
}

type SystemSpec struct {
	Types         []TypeDesc
	Shards        []ShardSpec
	PoolSlots     int    // message envelopes per shard (default 4096)
	SystemReserve int    // envelopes only system messages may use (default PoolSlots/8)
	RingSize      int    // cross-shard ring capacity, power of two >= 16 (default 1024)
	TimerEntries  int    // per shard (default 1024)
	MaxFDs        int    // sockets per shard (default 4096)
	ResetMax      int    // Level-2 resets allowed within ResetWindow before quarantine (default 3)
	ResetWindow   uint64 // ticks (default 10000)

	// MaxMessageBytes is the largest message data SendRaw and SendBlob accept
	// (default 16 MiB). Longer than MaxPayload it lives outside the envelope: a
	// mailbox full of large messages holds that much memory, so size the mailboxes
	// and PoolSlots of the types that receive them with this in mind.
	MaxMessageBytes int
}

func (s SystemSpec) normalize() (SystemSpec, error) {
	if len(s.Shards) < 1 || len(s.Shards) > 255 {
		return s, errors.New("gina: shard count must be 1..255")
	}
	if s.PoolSlots == 0 {
		s.PoolSlots = 4096
	}
	if s.SystemReserve == 0 {
		s.SystemReserve = s.PoolSlots / 8
	}
	if s.SystemReserve >= s.PoolSlots {
		return s, errors.New("gina: SystemReserve must be < PoolSlots")
	}
	if s.RingSize == 0 {
		s.RingSize = 1024
	}
	if s.RingSize < 16 || s.RingSize&(s.RingSize-1) != 0 {
		return s, errors.New("gina: RingSize must be a power of two >= 16")
	}
	if s.TimerEntries == 0 {
		s.TimerEntries = 1024
	}
	if s.MaxFDs == 0 {
		s.MaxFDs = 4096
	}
	if s.MaxMessageBytes == 0 {
		s.MaxMessageBytes = 16 << 20
	}
	if s.MaxMessageBytes < MaxPayload {
		return s, errors.New("gina: MaxMessageBytes must be at least MaxPayload")
	}
	if s.ResetMax == 0 {
		s.ResetMax = 3
	}
	if s.ResetWindow == 0 {
		s.ResetWindow = 10000
	}
	if len(s.Types) == 0 || len(s.Types) > 254 {
		return s, errors.New("gina: need 1..254 types")
	}
	types := make([]TypeDesc, len(s.Types))
	seen := [256]bool{}
	for i, t := range s.Types {
		if t.id == 255 {
			return s, errors.New("gina: type id 255 is reserved")
		}
		if seen[t.id] {
			return s, fmt.Errorf("gina: duplicate type id %d", t.id)
		}
		seen[t.id] = true
		o := t.opts
		if o.SlotCount < 1 || o.SlotCount > MaxSlotsPerType {
			return s, fmt.Errorf("gina: type %d SlotCount must be 1..%d", t.id, MaxSlotsPerType)
		}
		if o.MailboxCapacity == 0 {
			o.MailboxCapacity = 256
		}
		if o.ChunkSize == 0 {
			o.ChunkSize = 256
		}
		if o.ChunkSize&(o.ChunkSize-1) != 0 {
			return s, fmt.Errorf("gina: type %d ChunkSize must be a power of two", t.id)
		}
		t.opts = o
		types[i] = t
	}
	s.Types = types
	shards := make([]ShardSpec, len(s.Shards))
	for i, sh := range s.Shards {
		groups := append([]GroupSpec(nil), sh.Groups...)
		hasRoot := false
		for gi := range groups {
			g := &groups[gi]
			for gj := 0; gj < gi; gj++ {
				if groups[gj].ID == g.ID {
					return s, fmt.Errorf("gina: shard %d: duplicate group %d", i, g.ID)
				}
			}
			if g.ID == GroupNone {
				return s, errors.New("gina: group id 0xFFFF is reserved")
			}
			if g.ID == GroupRoot {
				hasRoot = true
			}
			if g.RestartMax == 0 {
				g.RestartMax = 5
			}
			if g.WindowTicks == 0 {
				g.WindowTicks = 10000
			}
			if g.MaxChildren == 0 {
				g.MaxChildren = 4096
			}
		}
		if !hasRoot {
			groups = append([]GroupSpec{{ID: GroupRoot, RestartMax: 5, WindowTicks: 10000, MaxChildren: 4096}}, groups...)
		}
		shards[i] = ShardSpec{Groups: groups, Boot: sh.Boot}
	}
	s.Shards = shards
	return s, nil
}
