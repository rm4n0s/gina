// SQL: a PostgreSQL or SQLite pool made of isolates. Workers move money between
// accounts in transactions, then a big result is streamed with backpressure and a
// slow query is cut off by its timeout.
//
//	docker run -d --name gina-pg -e POSTGRES_PASSWORD=secret -e POSTGRES_USER=app -e POSTGRES_DB=app -p 127.0.0.1:55432:5432 postgres:17-alpine
//	go run ./examples/sql                        # 8 workers, 4 connections
//	go run ./examples/sql -workers 32 -conns 8 -transfers 200
//	go run ./examples/sql -driver sqlite         # no server: a file in the temporary directory (needs cgo)
//
//	shard 0                                shard 1 (the pool's)
//	 coordinator ──setup/report/stream──┐
//	 worker 1..N ──Begin/Query/Commit───┼─▶ pool isolate ─▶ conn isolates ─▶ PostgreSQL
//	      ▲                             │                        │
//	      └───────── TagReply ◀─────────┴────────────────────────┘
//
// Nothing here blocks and nothing is a goroutine: a worker is a state machine that
// sends a request, returns, and is called again when the reply arrives. Requests
// of different workers share the pool's connections; a transaction owns one for
// its length. The total of all balances must stay what it was, whatever the
// interleaving, and the example checks it.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/rm4n0s/gina"
	"github.com/rm4n0s/gina/extensions/sql"
)

const (
	typeCoord  gina.TypeID = 1
	typeWorker gina.TypeID = 2 // the sql extension takes 230-232

	tagStart gina.Tag = gina.TagUserBase + 1 // timer: begin
	tagGo    gina.Tag = gina.TagUserBase + 2 // coordinator -> worker: start transferring
	tagDone  gina.Tag = gina.TagUserBase + 3 // worker -> coordinator: workerDone
)

type workerDone struct{ Committed, Failed uint32 }

type workerArgs struct {
	Coord     gina.Handle
	Accounts  uint32
	Transfers uint32
	Seed      uint64
}

// ---- a worker: Begin, lock two accounts, move money, Commit; repeat ----

// The request ID says which step an answer belongs to.
const (
	stBegin uint32 = iota + 1
	stLock
	stDebit
	stCredit
	stCommit
	stRollback
)

type worker struct {
	db        *sql.DB
	sqlite    bool
	coord     gina.Handle
	rng       *rand.Rand
	accounts  uint32
	left      uint32
	tx        sql.Tx
	from, to  uint32
	amount    int64
	committed uint32
	failed    uint32
}

func (a *app) workerInit(w *worker, ctx *gina.Ctx, raw []byte) gina.Effect {
	args := gina.ArgsAs[workerArgs](raw)
	w.db, w.sqlite, w.coord, w.accounts, w.left = a.db, a.sqlite, args.Coord, args.Accounts, args.Transfers
	w.rng = rand.New(rand.NewPCG(args.Seed, 1))
	return gina.WaitMessage()
}

func (w *worker) handle(ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagGo:
		w.next(ctx)
	case sql.TagReply:
		rep, err := sql.Decode(ctx, m)
		if err != nil {
			return gina.Done()
		}
		w.reply(ctx, &rep)
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

// next starts a transfer, or reports to the coordinator when there are none left.
func (w *worker) next(ctx *gina.Ctx) {
	if w.left == 0 {
		gina.Send(ctx, w.coord, tagDone, &workerDone{w.committed, w.failed})
		return
	}
	w.left--
	w.from = w.rng.Uint32N(w.accounts) + 1
	w.to = w.rng.Uint32N(w.accounts-1) + 1
	if w.to >= w.from {
		w.to++
	}
	w.amount = int64(w.rng.IntN(50) + 1)
	w.db.Begin(ctx, &sql.Request{ID: stBegin, Isolation: sql.IsolationReadCommitted})
}

func (w *worker) reply(ctx *gina.Ctx, rep *sql.Reply) {
	if rep.Kind == sql.ReplyError {
		// Anything that goes wrong ends the transfer: undo it if a transaction is open.
		if rep.ID == stBegin || rep.ID == stRollback || rep.ID == stCommit {
			w.failed++
			w.next(ctx)
			return
		}
		w.tx.Rollback(ctx, &sql.Request{ID: stRollback})
		w.failed++
		return
	}
	switch rep.ID {
	case stBegin:
		w.tx = rep.Tx
		// Lock both rows in id order, so two workers cannot wait for each other.
		lock := `SELECT balance FROM accounts WHERE id IN ($1, $2) ORDER BY id`
		if !w.sqlite { // SQLite has one writer at a time and needs no row locks
			lock += ` FOR UPDATE`
		}
		w.tx.Query(ctx, &sql.Request{ID: stLock, SQL: lock, Args: []any{w.from, w.to}})
	case stLock:
		var balances [2]int64
		for i := 0; rep.Rows.Next() && i < 2; i++ {
			rep.Rows.Scan(&balances[i])
		}
		fromBalance := balances[0] // the lower id comes first
		if w.from > w.to {
			fromBalance = balances[1]
		}
		if fromBalance < w.amount { // not enough money: nothing to do
			w.tx.Rollback(ctx, &sql.Request{ID: stRollback})
			return
		}
		w.tx.Exec(ctx, &sql.Request{ID: stDebit, SQL: `UPDATE accounts SET balance = balance - $1 WHERE id = $2`, Args: []any{w.amount, w.from}})
	case stDebit:
		w.tx.Exec(ctx, &sql.Request{ID: stCredit, SQL: `UPDATE accounts SET balance = balance + $1 WHERE id = $2`, Args: []any{w.amount, w.to}})
	case stCredit:
		w.tx.Commit(ctx, &sql.Request{ID: stCommit})
	case stCommit:
		w.committed++
		w.next(ctx)
	case stRollback:
		w.next(ctx)
	}
}

// ---- the coordinator: setup, check, stream, time out ----

const (
	idSetup uint32 = iota + 100
	idFill
	idSum
	idStream
	idSlow
)

type app struct {
	db       *sql.DB
	sqlite   bool // the statements below differ in a few places
	accounts uint32
	workers  uint32
	each     uint32
}

type coord struct {
	started   time.Time
	pending   uint32
	committed uint32
	failed    uint32
	total     int64
	streamed  int
	batches   int
}

func (a *app) coordInit(c *coord, ctx *gina.Ctx, _ []byte) gina.Effect {
	ctx.RegisterTimer(20*time.Millisecond, tagStart) // let the pool shard come up
	return gina.WaitMessage()
}

func (a *app) coordHandle(c *coord, ctx *gina.Ctx, m *gina.Message) gina.Effect {
	switch m.Tag {
	case tagStart:
		// No arguments: the simple protocol, so one request may hold several statements.
		a.db.Exec(ctx, &sql.Request{ID: idSetup, SQL: `DROP TABLE IF EXISTS accounts; CREATE TABLE accounts (id int PRIMARY KEY, balance bigint NOT NULL)`})
	case tagDone:
		d := gina.PayloadAs[workerDone](m)
		c.committed += d.Committed
		c.failed += d.Failed
		if c.pending--; c.pending == 0 {
			el := time.Since(c.started)
			fmt.Printf("%d workers: %d transfers committed, %d failed, in %v (%.0f commits/s)\n",
				a.workers, c.committed, c.failed, el.Round(time.Millisecond), float64(c.committed)/el.Seconds())
			a.db.Query(ctx, &sql.Request{ID: idSum, SQL: `SELECT sum(balance), count(*) FROM accounts`})
		}
	case sql.TagReply:
		rep, err := sql.Decode(ctx, m)
		if err != nil {
			return a.fail(ctx, err)
		}
		if rep.Kind == sql.ReplyError && rep.ID != idSlow {
			return a.fail(ctx, rep.Err)
		}
		a.reply(c, ctx, &rep)
	case gina.TagShutdown:
		return gina.Done()
	}
	return gina.WaitMessage()
}

func (a *app) fail(ctx *gina.Ctx, err error) gina.Effect {
	fmt.Fprintln(os.Stderr, "error:", err)
	ctx.StopSystem()
	return gina.Done()
}

func (a *app) reply(c *coord, ctx *gina.Ctx, rep *sql.Reply) {
	switch rep.ID {
	case idSetup:
		fill := `INSERT INTO accounts SELECT g, 1000 FROM generate_series(1, $1::int) AS g`
		if a.sqlite {
			fill = `INSERT INTO accounts WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM g WHERE n < $1) SELECT n, 1000 FROM g`
		}
		a.db.Exec(ctx, &sql.Request{ID: idFill, SQL: fill, Args: []any{a.accounts}})
	case idFill:
		c.started = time.Now()
		c.pending = a.workers
		for i := uint32(0); i < a.workers; i++ {
			sp := gina.WithArgs(gina.SpawnSpec{Type: typeWorker, Group: gina.GroupNone, Restart: gina.RestartTemporary},
				&workerArgs{Coord: ctx.Self(), Accounts: a.accounts, Transfers: a.each, Seed: uint64(i) + 1})
			w, err := ctx.Spawn(sp)
			if err != gina.SpawnErrNone {
				fmt.Fprintln(os.Stderr, "could not start a worker:", err)
				ctx.StopSystem()
				return
			}
			ctx.SendRaw(w, tagGo, nil)
		}
	case idSum:
		var count int64
		rep.Rows.Next()
		rep.Rows.Scan(&c.total, &count)
		want := int64(a.accounts) * 1000
		verdict := "OK"
		if c.total != want {
			verdict = "WRONG"
		}
		fmt.Printf("total balance %d over %d accounts (expected %d): %s\n", c.total, count, want, verdict)
		// 200 000 rows, a thousand to a batch; the connection reads the next batch only when asked.
		stream := `SELECT g, md5(g::text) FROM generate_series(1, 200000) AS g`
		if a.sqlite {
			stream = `WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM g WHERE n < 200000) SELECT n, hex(randomblob(16)) FROM g`
		}
		a.db.Query(ctx, &sql.Request{ID: idStream, SQL: stream, BatchRows: 1000})
	case idStream:
		c.batches++
		c.streamed += rep.Rows.Len()
		if rep.More {
			rep.Continue(ctx) // ask for the next batch; a consumer that is busy simply does it later
			return
		}
		fmt.Printf("streamed %d rows in %d batches (%s)\n", c.streamed, c.batches, rep.Tag)
		// PostgreSQL is asked to cancel a statement that sleeps. SQLite cannot be
		// interrupted inside a statement, so it gets a result that never ends: the
		// timeout is checked between rows.
		slow := `SELECT pg_sleep(10)`
		if a.sqlite {
			slow = `WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM g) SELECT n FROM g`
		}
		a.db.Query(ctx, &sql.Request{ID: idSlow, SQL: slow, Timeout: 300 * time.Millisecond})
	case idSlow:
		if rep.Kind == sql.ReplyRows {
			if rep.More {
				rep.Continue(ctx)
			}
			return
		}
		fmt.Printf("a query that never ends, with a 300ms timeout: %v\n", rep.Err)
		s := a.db.Stats()
		fmt.Printf("pool: %d opened, %d open, %d idle, %d queued requests waited\n", s.Opened, s.Open, s.Idle, s.WaitCount)
		ctx.StopSystem()
	}
}

func main() {
	driver := flag.String("driver", "postgres", "postgres or sqlite")
	path := flag.String("path", filepath.Join(os.TempDir(), "gina-sql-example.db"), "SQLite database file")
	addr := flag.String("addr", "127.0.0.1:55432", "PostgreSQL address (an IP address: Gina does no name resolution)")
	user := flag.String("user", "app", "user")
	password := flag.String("password", "secret", "password")
	dbname := flag.String("db", "app", "database")
	conns := flag.Int("conns", 4, "connections in the pool (PostgreSQL; SQLite uses one)")
	workers := flag.Int("workers", 8, "worker isolates")
	transfers := flag.Int("transfers", 100, "transfers per worker")
	accounts := flag.Int("accounts", 20, "accounts")
	flag.Parse()

	var cfg sql.Config
	switch *driver {
	case "postgres":
		ap, err := netip.ParseAddrPort(*addr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg = sql.Config{Addr: ap, User: *user, Password: *password, Database: *dbname, Shard: 1, MaxOpenConns: *conns}
	case "sqlite":
		cfg = sql.Config{Driver: sql.DriverSQLite, Path: *path, Shard: 1, Params: map[string]string{"journal_mode": "WAL"}}
	default:
		fmt.Fprintln(os.Stderr, "-driver is postgres or sqlite")
		os.Exit(2)
	}
	db, err := sql.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	a := &app{db: db, sqlite: *driver == "sqlite", accounts: uint32(*accounts), workers: uint32(*workers), each: uint32(*transfers)}

	spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 2)}
	spec.Types = append(spec.Types,
		gina.RegisterType(typeCoord, gina.TypeOptions{SlotCount: 1, MailboxCapacity: 256}, a.coordInit, a.coordHandle),
		gina.RegisterType(typeWorker, gina.TypeOptions{SlotCount: *workers, MailboxCapacity: 16}, a.workerInit, (*worker).handle),
	)
	spec.Shards[0].Boot = append(spec.Shards[0].Boot, gina.SpawnSpec{Type: typeCoord, Group: gina.GroupRoot, Restart: gina.RestartTemporary})
	if err := db.Install(&spec); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	sys, err := gina.NewSystem(spec, gina.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer sys.Close()
	sys.Run(gina.RunOptions{})
}
