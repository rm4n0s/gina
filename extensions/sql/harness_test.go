package sql

import (
	"encoding/binary"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

// The tests run a threaded system with a probe isolate on shard 0 and the DB on
// shard 1. The test goroutine hands the probe closures to run (do), the probe
// sends its requests and decodes the replies, and the test reads them from a
// channel. Rows are decoded into []any with Scan into *any.

const (
	typeProbe gina.TypeID = 1
	tagDo     gina.Tag    = gina.TagUserBase + 1
)

// rec is a reply, decoded.
type rec struct {
	id      uint32
	kind    ReplyKind
	cols    []Column
	rows    [][]any
	batches int // ReplyRows messages seen for the request
	more    bool
	tag     string
	n       int64
	err     error
	tx      Tx
	partial bool // a batch with More set, reported when the probe is in manual mode
	from    gina.Handle
}

type probe struct {
	acc map[uint32]*rec
}

type harness struct {
	t      *testing.T
	sys    *gina.System
	db     *DB
	probe  gina.Handle
	out    chan rec
	manual atomic.Bool // do not Continue by itself

	mu   sync.Mutex
	fns  []func(g *gina.Ctx)
	seen map[uint32][]rec
}

// pgConfig is the server the integration tests need. Start one with
//
//	docker run -d -e POSTGRES_PASSWORD=secret -e POSTGRES_USER=app -e POSTGRES_DB=app -p 127.0.0.1:55432:5432 postgres:17-alpine
//
// or point GINA_PG_ADDR (and GINA_PG_USER, GINA_PG_PASSWORD, GINA_PG_DATABASE) at another.
func pgConfig(t *testing.T) Config {
	t.Helper()
	addr := os.Getenv("GINA_PG_ADDR")
	if addr == "" {
		addr = "127.0.0.1:55432"
	}
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		t.Fatalf("GINA_PG_ADDR: %v", err)
	}
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Skipf("no PostgreSQL at %s (see pgConfig): %v", addr, err)
	}
	c.Close()
	env := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	return Config{Addr: ap, User: env("GINA_PG_USER", "app"), Password: env("GINA_PG_PASSWORD", "secret"), Database: env("GINA_PG_DATABASE", "app"), Shard: 1}
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{t: t, out: make(chan rec, 4096), seen: map[uint32][]rec{}}
	db, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.db = db
	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 2)}
	spec.Types = append(spec.Types, gina.RegisterType(typeProbe, gina.TypeOptions{SlotCount: 4, MailboxCapacity: 4096}, nil, h.probeHandler))
	spec.Shards[0].Boot = append(spec.Shards[0].Boot, gina.SpawnSpec{Type: typeProbe, Group: gina.GroupRoot, Restart: gina.RestartPermanent})
	if err := db.Install(&spec); err != nil {
		t.Fatal(err)
	}
	h.sys, err = gina.NewSystem(spec, gina.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h.sys.Start(gina.RunOptions{})
	t.Cleanup(func() { h.sys.Stop(); h.sys.Wait(); h.sys.Close() })
	h.probe = gina.MakeHandle(0, typeProbe, 0, 1)
	return h
}

func (h *harness) probeHandler(p *probe, g *gina.Ctx, m *gina.Message) gina.Effect {
	if p.acc == nil {
		p.acc = map[uint32]*rec{}
	}
	switch m.Tag {
	case gina.TagShutdown:
		return gina.Done()
	case tagDo:
		h.mu.Lock()
		fn := h.fns[binary.BigEndian.Uint32(m.Payload[:4])]
		h.mu.Unlock()
		fn(g)
	case TagReply:
		rep, err := Decode(g, m)
		if err != nil {
			h.out <- rec{id: m.Correlation, err: err}
			break
		}
		r := p.acc[rep.ID]
		if r == nil {
			r = &rec{id: rep.ID}
			p.acc[rep.ID] = r
		}
		r.kind, r.from = rep.Kind, m.Source
		switch rep.Kind {
		case ReplyRows:
			r.batches++
			r.cols = rep.Rows.Columns()
			for rep.Rows.Next() {
				row := make([]any, len(r.cols))
				dest := make([]any, len(row))
				for i := range dest {
					dest[i] = &row[i]
				}
				if err := rep.Rows.Scan(dest...); err != nil {
					r.err = err
				}
				r.rows = append(r.rows, row)
			}
			r.more, r.tag, r.n = rep.More, rep.Tag, rep.RowsAffected
			if rep.More {
				if h.manual.Load() {
					c := *r
					c.partial = true
					h.out <- c
				} else {
					rep.Continue(g)
				}
				return gina.WaitMessage()
			}
		case ReplyDone:
			r.tag, r.n = rep.Tag, rep.RowsAffected
		case ReplyError:
			r.err = rep.Err
		case ReplyTx:
			r.tx = rep.Tx
		}
		delete(p.acc, rep.ID)
		h.out <- *r
	}
	return gina.WaitMessage()
}

// run runs fn on the probe's thread.
func (h *harness) run(fn func(g *gina.Ctx)) {
	h.mu.Lock()
	h.fns = append(h.fns, fn)
	i := uint32(len(h.fns) - 1)
	h.mu.Unlock()
	if h.sys.SendExternal(h.probe, tagDo, binary.BigEndian.AppendUint32(nil, i)) != gina.SendOK {
		h.t.Fatal("could not reach the probe")
	}
}

// wait returns the terminal reply to request id (or the next partial one in manual
// mode).
func (h *harness) wait(id uint32) rec { return h.waitFor(id, 15*time.Second) }

func (h *harness) waitFor(id uint32, d time.Duration) rec {
	h.t.Helper()
	timeout := time.After(d)
	for {
		h.mu.Lock()
		if q := h.seen[id]; len(q) > 0 {
			r := q[0]
			h.seen[id] = q[1:]
			h.mu.Unlock()
			return r
		}
		h.mu.Unlock()
		select {
		case r := <-h.out:
			h.mu.Lock()
			h.seen[r.id] = append(h.seen[r.id], r)
			h.mu.Unlock()
		case <-timeout:
			h.t.Fatalf("no reply to request %d within %v", id, d)
		}
	}
}

// expectNone fails if a reply to id arrives within d.
func (h *harness) expectNone(id uint32, d time.Duration) {
	h.t.Helper()
	timeout := time.After(d)
	for {
		h.mu.Lock()
		if len(h.seen[id]) > 0 {
			h.mu.Unlock()
			h.t.Fatalf("unexpected reply to request %d: %+v", id, h.seen[id][0])
		}
		h.mu.Unlock()
		select {
		case r := <-h.out:
			h.mu.Lock()
			h.seen[r.id] = append(h.seen[r.id], r)
			h.mu.Unlock()
		case <-timeout:
			return
		}
	}
}

var nextID atomic.Uint32

func newID() uint32 { return nextID.Add(1) }

// query runs a statement through the pool and waits for its end.
func (h *harness) query(sql string, args ...any) rec {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: sql, Args: args}) })
	return h.wait(id)
}

func (h *harness) exec(sql string, args ...any) rec {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) { h.db.Exec(g, &Request{ID: id, SQL: sql, Args: args}) })
	return h.wait(id)
}

func (h *harness) mustQuery(sql string, args ...any) rec {
	h.t.Helper()
	r := h.query(sql, args...)
	if r.err != nil {
		h.t.Fatalf("%s: %v", sql, r.err)
	}
	return r
}

func (h *harness) mustExec(sql string, args ...any) rec {
	h.t.Helper()
	r := h.exec(sql, args...)
	if r.err != nil {
		h.t.Fatalf("%s: %v", sql, r.err)
	}
	return r
}

func (h *harness) begin(opts ...func(*Request)) (Tx, rec) {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) {
		r := &Request{ID: id}
		for _, o := range opts {
			o(r)
		}
		h.db.Begin(g, r)
	})
	r := h.wait(id)
	return r.tx, r
}

func (h *harness) txQuery(tx Tx, sql string, args ...any) rec {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) { tx.Query(g, &Request{ID: id, SQL: sql, Args: args}) })
	return h.wait(id)
}

func (h *harness) txExec(tx Tx, sql string, args ...any) rec {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) { tx.Exec(g, &Request{ID: id, SQL: sql, Args: args}) })
	return h.wait(id)
}

func (h *harness) commit(tx Tx) rec {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) { tx.Commit(g, &Request{ID: id}) })
	return h.wait(id)
}

func (h *harness) rollback(tx Tx) rec {
	h.t.Helper()
	id := newID()
	h.run(func(g *gina.Ctx) { tx.Rollback(g, &Request{ID: id}) })
	return h.wait(id)
}

// eventually polls cond until it holds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func scalar(t *testing.T, r rec) any {
	t.Helper()
	if r.err != nil || len(r.rows) != 1 || len(r.rows[0]) != 1 {
		t.Fatalf("not a scalar: %+v", r)
	}
	return r.rows[0][0]
}
