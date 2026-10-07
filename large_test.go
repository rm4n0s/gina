package gina_test

import (
	"bytes"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

const (
	tagLarge = gina.TagUserBase + 0x80 + iota
	tagLargeGo
	tagLargeBlob
)

// pattern returns n deterministic bytes that depend on seed.
func pattern(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) ^ seed
	}
	return b
}

type got struct {
	tag   gina.Tag
	corr  uint32
	large bool
	n     int
	sum   uint64
	prefx int // len(m.Payload[:PayloadSize])
}

func lsum(b []byte) uint64 { h := fnv.New64a(); h.Write(b); return h.Sum64() }

type lsink struct{}

func sinkFor(mu *sync.Mutex, log *[]got) gina.Handler[lsink] {
	return func(_ *lsink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		if m.Tag >= gina.TagUserBase {
			d := ctx.Data()
			mu.Lock()
			*log = append(*log, got{m.Tag, m.Correlation, m.IsLarge(), len(d), lsum(d), int(m.PayloadSize)})
			mu.Unlock()
		}
		return gina.WaitMessage()
	}
}

// lsender sends whatever it is told to, from its own shard.
type lsender struct{}

type sendCmd struct {
	To   gina.Handle
	Size uint32
	Corr uint32
	Seed uint32
	Pad  uint32 // no implicit padding allowed in a payload
}

func senderHandler(blob **gina.Blob) gina.Handler[lsender] {
	return func(_ *lsender, ctx *gina.Ctx, m *gina.Message) gina.Effect {
		switch m.Tag {
		case tagLarge:
			c := gina.PayloadAs[sendCmd](m)
			ctx.SendCorr(c.To, tagLargeGo, c.Corr, pattern(byte(c.Seed), int(c.Size)))
		case tagLargeBlob:
			c := gina.PayloadAs[sendCmd](m)
			if *blob == nil {
				*blob = gina.NewBlob(pattern(byte(c.Seed), int(c.Size)))
			}
			ctx.SendBlob(c.To, tagLargeBlob, c.Corr, *blob)
		}
		return gina.WaitMessage()
	}
}

func largeSpec(mu *sync.Mutex, log *[]got, blob **gina.Blob, shards int) gina.SystemSpec {
	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{
			gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 64}, nil, sinkFor(mu, log)),
			gina.RegisterType(2, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 64}, nil, senderHandler(blob)),
		},
		MaxMessageBytes: 1 << 20,
	}
	for i := 0; i < shards; i++ {
		spec.Shards = append(spec.Shards, gina.ShardSpec{Boot: []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}, {Type: 2, Group: gina.GroupRoot}}})
	}
	return spec
}

func TestLargeMessagesSameShardAndAcrossShards(t *testing.T) {
	var mu sync.Mutex
	var log []got
	var blob *gina.Blob
	s := newSim(t, largeSpec(&mu, &log, &blob, 2), 7, gina.SimConfig{})
	sys := s.Sys
	sink0, sink1 := sys.BootHandle(0, 0), sys.BootHandle(1, 0)
	send0 := sys.BootHandle(0, 1)

	cases := []struct {
		to   gina.Handle
		size int
		corr uint32
	}{
		{sink0, 96, 1}, {sink0, 97, 2}, {sink0, 5000, 3}, {sink0, 1 << 20, 4}, // same shard
		{sink1, 96, 5}, {sink1, 97, 6}, {sink1, 5000, 7}, {sink1, 1 << 20, 8}, // across shards
	}
	for i, c := range cases {
		gina.SendTo(sys, send0, tagLarge, &sendCmd{To: c.to, Size: uint32(c.size), Corr: c.corr, Seed: uint32(i)})
	}
	run(t, s, 50)
	mu.Lock()
	defer mu.Unlock()
	if len(log) != len(cases) {
		t.Fatalf("%d of %d messages arrived", len(log), len(cases))
	}
	byCorr := map[uint32]got{}
	for _, g := range log {
		byCorr[g.corr] = g
	}
	for i, c := range cases {
		g := byCorr[c.corr]
		if g.n != c.size || g.sum != lsum(pattern(byte(i), c.size)) {
			t.Errorf("case %d (%d bytes): got %d bytes, sum ok=%v", i, c.size, g.n, g.sum == lsum(pattern(byte(i), c.size)))
		}
		if g.large != (c.size > gina.MaxPayload) {
			t.Errorf("case %d: IsLarge=%v for %d bytes", i, g.large, c.size)
		}
		if c.size > gina.MaxPayload && g.prefx != gina.MaxPayload {
			t.Errorf("case %d: inline prefix %d, want %d", i, g.prefx, gina.MaxPayload)
		}
	}
}

func TestSmallAndLargeKeepTheirOrderPerSender(t *testing.T) {
	var mu sync.Mutex
	var log []got
	var blob *gina.Blob
	s := newSim(t, largeSpec(&mu, &log, &blob, 2), 1, gina.SimConfig{})
	sys := s.Sys
	sink1, send0 := sys.BootHandle(1, 0), sys.BootHandle(0, 1)
	for i := 0; i < 20; i++ {
		size := 10
		if i%3 == 0 {
			size = 3000
		}
		gina.SendTo(sys, send0, tagLarge, &sendCmd{To: sink1, Size: uint32(size), Corr: uint32(i), Seed: uint32(i)})
	}
	run(t, s, 50)
	mu.Lock()
	defer mu.Unlock()
	if len(log) != 20 {
		t.Fatalf("%d messages", len(log))
	}
	for i, g := range log {
		if g.corr != uint32(i) {
			t.Fatalf("message %d arrived as %d", i, g.corr)
		}
	}
}

func TestMaxMessageBytesAndExternalSend(t *testing.T) {
	var mu sync.Mutex
	var log []got
	var blob *gina.Blob
	s := newSim(t, largeSpec(&mu, &log, &blob, 1), 1, gina.SimConfig{})
	sys := s.Sys
	sink, send := sys.BootHandle(0, 0), sys.BootHandle(0, 1)

	if r := sys.Send(sink, tagLargeGo, make([]byte, 1<<20+1)); r != gina.SendPayloadTooLarge {
		t.Fatalf("over the limit: %v", r)
	}
	if r := sys.Send(sink, tagLargeGo, pattern(3, 50000)); r != gina.SendOK {
		t.Fatalf("external large send: %v", r)
	}
	run(t, s, 10)
	mu.Lock()
	if len(log) != 1 || log[0].n != 50000 || log[0].sum != lsum(pattern(3, 50000)) {
		t.Fatalf("log %+v", log)
	}
	mu.Unlock()
	// an isolate asking for too much gets the same answer and nothing is sent
	gina.SendTo(sys, send, tagLarge, &sendCmd{To: sink, Size: 1<<20 + 1})
	run(t, s, 10)
	mu.Lock()
	defer mu.Unlock()
	if len(log) != 1 {
		t.Fatalf("an oversize message was delivered: %d", len(log))
	}
	if err := sys.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestSenderMayReuseItsBuffer(t *testing.T) {
	var mu sync.Mutex
	var log []got
	var blob *gina.Blob
	spec := largeSpec(&mu, &log, &blob, 1)
	buf := pattern(1, 4000)
	spec.Types = append(spec.Types, gina.RegisterType(3, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 4}, nil,
		func(_ *lsender, ctx *gina.Ctx, m *gina.Message) gina.Effect {
			to := *gina.PayloadAs[gina.Handle](m)
			ctx.SendRaw(to, tagLargeGo, buf)
			for i := range buf { // scribble over it afterwards
				buf[i] = 0xEE
			}
			return gina.WaitMessage()
		}))
	spec.Shards[0].Boot = append(spec.Shards[0].Boot, gina.SpawnSpec{Type: 3, Group: gina.GroupRoot})
	s := newSim(t, spec, 1, gina.SimConfig{})
	sink, scribbler := s.Sys.BootHandle(0, 0), s.Sys.BootHandle(0, 2)
	gina.SendTo(s.Sys, scribbler, tagLarge, &sink)
	run(t, s, 10)
	mu.Lock()
	defer mu.Unlock()
	if len(log) != 1 || log[0].sum != lsum(pattern(1, 4000)) {
		t.Fatalf("the receiver saw the lsender's later writes: %+v", log)
	}
}

// A running System accepts no calls from outside, so the broadcast is driven by
// an isolate: it publishes one Blob to every sink on the other shards, once per
// timer tick, and the sinks check what they get. One 150 KB buffer is read by
// three threads at once (run this with -race).
type lcaster struct {
	sinks []gina.Handle
	blob  *gina.Blob
	round int
}

func TestSharedBlobAcrossShardThreads(t *testing.T) {
	const shards, rounds, size = 4, 30, 150_000
	var delivered, bad atomic.Int64
	want := lsum(pattern(9, size))
	const tagGo = gina.TagUserBase + 0x90
	spec := gina.SystemSpec{
		Types: []gina.TypeDesc{
			gina.RegisterType(1, gina.TypeOptions{SlotCount: 2, MailboxCapacity: 64}, nil,
				func(_ *lsink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
					if m.Tag == tagLargeBlob {
						d := ctx.Data()
						if len(d) != size || lsum(d) != want || m.Correlation != uint32(ctx.ShardID()) || !m.IsLarge() {
							bad.Add(1)
						}
						if delivered.Add(1) == (shards-1)*rounds {
							ctx.StopSystem()
						}
					}
					return gina.WaitMessage()
				}),
			gina.RegisterType(2, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 8}, func(c *lcaster, ctx *gina.Ctx, _ []byte) gina.Effect {
				c.blob = gina.NewBlob(pattern(9, size)) // one copy, shared by every receiver
				for i := 1; i < shards; i++ {
					c.sinks = append(c.sinks, gina.MakeHandle(uint8(i), 1, 0, 1))
				}
				ctx.RegisterTimer(20*time.Millisecond, tagGo)
				return gina.WaitMessage()
			}, func(c *lcaster, ctx *gina.Ctx, m *gina.Message) gina.Effect {
				if m.Tag == tagGo && c.round < rounds {
					c.round++
					for _, h := range c.sinks {
						ctx.SendBlob(h, tagLargeBlob, uint32(h.Shard()), c.blob)
					}
					ctx.RegisterTimer(time.Millisecond, tagGo)
				}
				return gina.WaitMessage()
			}),
		},
		MaxMessageBytes: 1 << 20,
	}
	spec.Shards = append(spec.Shards, gina.ShardSpec{Boot: []gina.SpawnSpec{{Type: 2, Group: gina.GroupRoot}}})
	for i := 1; i < shards; i++ {
		spec.Shards = append(spec.Shards, gina.ShardSpec{Boot: []gina.SpawnSpec{{Type: 1, Group: gina.GroupRoot}}})
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Close()
	sys.Start(gina.RunOptions{ShutdownGrace: 2 * time.Second})
	waitOrFail(t, sys, 20*time.Second)
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d messages arrived damaged", n)
	}
	if n := delivered.Load(); n != (shards-1)*rounds {
		t.Fatalf("delivered %d of %d", n, (shards-1)*rounds)
	}
}

func TestLargeDataIsBytesEqualToWhatWasSent(t *testing.T) {
	// the prefix in Payload is the start of the data
	var mu sync.Mutex
	var log []got
	var blob *gina.Blob
	var prefixOK bool
	spec := largeSpec(&mu, &log, &blob, 1)
	spec.Types[0] = gina.RegisterType(1, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 8}, nil,
		func(_ *lsink, ctx *gina.Ctx, m *gina.Message) gina.Effect {
			if m.Tag == tagLargeGo {
				prefixOK = bytes.Equal(m.Payload[:m.PayloadSize], ctx.Data()[:gina.MaxPayload])
			}
			return gina.WaitMessage()
		})
	s := newSim(t, spec, 1, gina.SimConfig{})
	s.Sys.Send(s.Sys.BootHandle(0, 0), tagLargeGo, pattern(2, 1000))
	run(t, s, 5)
	if !prefixOK {
		t.Fatal("Payload is not the prefix of Data")
	}
}
