package sql

import (
	"crypto/x509"
	"errors"
	gtls "github.com/rm4n0s/gina/extensions/tls"
	"math"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

// Integration tests against a real PostgreSQL (see pgConfig). They are skipped when
// none is listening.

func TestPGQueryAndTypes(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.mustExec(`DROP TABLE IF EXISTS t_types`)
	h.mustExec(`CREATE TABLE t_types (id serial PRIMARY KEY, i int, b bigint, f float8, s text, ok bool, bin bytea, ts timestamptz, d date, n numeric, j jsonb, u uuid)`)
	when := time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC)
	r := h.mustExec(`INSERT INTO t_types (i, b, f, s, ok, bin, ts, d, n, j, u) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		int32(-7), int64(math.MaxInt64), 2.5, "héllo\nworld", true, []byte{0, 1, 2, 255}, when, "2024-05-06", "12345.6789", `{"a":[1,2]}`, "123e4567-e89b-12d3-a456-426614174000")
	if r.tag != "INSERT 0 1" || r.n != 1 {
		t.Fatalf("%+v", r)
	}
	r = h.mustExec(`INSERT INTO t_types (i) VALUES ($1)`, nil) // NULLs everywhere else
	r = h.mustQuery(`SELECT i, b, f, s, ok, bin, ts, d, n, j, u FROM t_types ORDER BY id`)
	if len(r.rows) != 2 || r.tag != "SELECT 2" || r.n != 2 {
		t.Fatalf("%+v", r)
	}
	row := r.rows[0]
	if row[0] != int64(-7) || row[1] != int64(math.MaxInt64) || row[2] != 2.5 || row[3] != "héllo\nworld" || row[4] != true {
		t.Fatalf("%#v", row)
	}
	if b, _ := row[5].([]byte); string(b) != "\x00\x01\x02\xff" {
		t.Fatalf("bytea %#v", row[5])
	}
	if ts, _ := row[6].(time.Time); !ts.Equal(when) {
		t.Fatalf("timestamptz %#v", row[6])
	}
	if d, _ := row[7].(time.Time); d.Format("2006-01-02") != "2024-05-06" {
		t.Fatalf("date %#v", row[7])
	}
	if row[8] != "12345.6789" || row[9] != `{"a": [1, 2]}` || row[10] != "123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("%#v", row[8:])
	}
	for i, v := range r.rows[1][1:] {
		if v != nil {
			t.Fatalf("column %d should be NULL, got %#v", i+1, v)
		}
	}
	if r.cols[0].Name != "i" || r.cols[0].OID != OIDInt4 || r.cols[5].OID != OIDBytea {
		t.Fatalf("%+v", r.cols)
	}
}

func TestPGExecMultiAndNoRows(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.mustExec(`DROP TABLE IF EXISTS t_multi`)
	// no arguments: the simple protocol, several statements
	r := h.mustExec(`CREATE TABLE t_multi (n int); INSERT INTO t_multi VALUES (1),(2),(3); UPDATE t_multi SET n = n + 1 WHERE n > 1; SELECT * FROM t_multi`)
	if r.n != 3+2+3 { // the counts of the command tags: INSERT 0 3, UPDATE 2, SELECT 3
		t.Fatalf("affected %d (%+v)", r.n, r)
	}
	// a Query of a statement with no result set ends with Done
	r = h.mustQuery(`DELETE FROM t_multi WHERE n = $1`, 1)
	if r.kind != ReplyDone || r.tag != "DELETE 1" || r.n != 1 {
		t.Fatalf("%+v", r)
	}
	// an empty statement
	if r := h.mustExec(``); r.tag != "" {
		t.Fatalf("%+v", r)
	}
	// a result with no rows still has its columns
	r = h.mustQuery(`SELECT n, 'x' AS name FROM t_multi WHERE false`)
	if r.kind != ReplyRows || len(r.rows) != 0 || len(r.cols) != 2 || r.cols[1].Name != "name" {
		t.Fatalf("%+v", r)
	}
	// a result with no columns
	r = h.mustQuery(`SELECT FROM generate_series(1, 3)`)
	if len(r.rows) != 3 || len(r.cols) != 0 {
		t.Fatalf("%+v", r)
	}
}

func TestPGServerErrors(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.mustExec(`DROP TABLE IF EXISTS t_err`)
	h.mustExec(`CREATE TABLE t_err (id int PRIMARY KEY)`)
	h.mustExec(`INSERT INTO t_err VALUES (1)`)

	r := h.exec(`INSERT INTO t_err VALUES ($1)`, 1)
	var pe *Error
	if !errors.As(r.err, &pe) || pe.Code != "23505" || pe.Constraint != "t_err_pkey" || pe.Table != "t_err" || pe.Client != ClientNone {
		t.Fatalf("%+v", r.err)
	}
	r = h.query(`SELEC 1`)
	if !errors.As(r.err, &pe) || pe.Code != "42601" || pe.Position == "" {
		t.Fatalf("%+v", r.err)
	}
	r = h.query(`SELECT 1/$1::int`, 0)
	if !errors.As(r.err, &pe) || pe.Code != "22012" {
		t.Fatalf("%+v", r.err)
	}
	// the connection is fine afterwards
	if v := scalar(t, h.mustQuery(`SELECT 41 + $1::int`, 1)); v != int64(42) {
		t.Fatalf("%v", v)
	}
	// a bad argument type is caught before anything is sent
	r = h.query(`SELECT $1`, struct{}{})
	if !errors.Is(r.err, ErrBadRequest) {
		t.Fatalf("%+v", r.err)
	}
	// and so is an error in the middle of a result: the rows before it are not delivered as a result
	r = h.query(`SELECT 10 / (3 - n) FROM generate_series(1, 5) AS n`)
	if !errors.As(r.err, &pe) || pe.Code != "22012" {
		t.Fatalf("%+v", r)
	}
	if h.db.Stats().Open > 1 {
		t.Fatalf("errors should not cost connections: %+v", h.db.Stats())
	}
}

func TestPGStreamingBackpressure(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `SELECT n, repeat('x', 100) FROM generate_series(1, 1000) AS n`, BatchRows: 100})
	})
	// Each batch waits for Continue: after the first nothing more arrives.
	r := h.wait(id)
	if !r.partial || len(r.rows) != 100 || !r.more {
		t.Fatalf("%+v", r)
	}
	h.expectNone(id, 300*time.Millisecond)
	conn := r.from
	for batch := 2; batch <= 9; batch++ { // the tenth is the last, and comes with the tag
		h.run(func(g *gina.Ctx) { g.SendCorr(conn, TagContinue, id, nil) })
		r = h.wait(id)
		if !r.partial || len(r.rows) != batch*100 {
			t.Fatalf("batch %d: %d rows, %+v", batch, len(r.rows), r.partial)
		}
	}
	h.run(func(g *gina.Ctx) { g.SendCorr(conn, TagContinue, id, nil) })
	r = h.wait(id)
	if r.partial || r.more || len(r.rows) != 1000 || r.tag != "SELECT 1000" || r.batches != 10 { // no empty batch at the end
		t.Fatalf("%d rows, %+v", len(r.rows), r)
	}
	if r.rows[999][0] != int64(1000) {
		t.Fatalf("%v", r.rows[999])
	}
	// the connection went back to the pool
	eventually(t, "the connection to be idle", func() bool { return h.db.Stats().Idle == 1 })
}

func TestPGStreamAutoContinueBigResult(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	// 20000 rows of about 200 bytes: many batches closed by size
	r := h.mustQuery(`SELECT n, repeat('y', 200) FROM generate_series(1, 20000) AS n`)
	if len(r.rows) != 20000 || r.batches < 20 {
		t.Fatalf("%d rows in %d batches", len(r.rows), r.batches)
	}
	for i, row := range r.rows {
		if row[0] != int64(i+1) {
			t.Fatalf("row %d: %v", i, row[0])
		}
	}
}

func TestPGStreamCancel(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `SELECT n FROM generate_series(1, 100000000) AS n`, BatchRows: 10})
	})
	r := h.wait(id)
	if !r.partial {
		t.Fatalf("%+v", r)
	}
	conn := r.from
	h.run(func(g *gina.Ctx) { g.SendCorr(conn, TagCancel, id, nil) })
	r = h.wait(id)
	if !errors.Is(r.err, ErrCanceled) {
		t.Fatalf("%+v", r)
	}
	// the connection is clean and reusable
	h.manual.Store(false)
	if v := scalar(t, h.mustQuery(`SELECT 1`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Open != 1 {
		t.Fatalf("the cancelled query should not have cost the connection: %+v", s)
	}
}

func TestPGStreamAbandoned(t *testing.T) {
	cfg := pgConfig(t)
	cfg.StreamTimeout = 200 * time.Millisecond
	h := newHarness(t, cfg)
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `SELECT n FROM generate_series(1, 100000000) AS n`, BatchRows: 10})
	})
	if r := h.wait(id); !r.partial {
		t.Fatalf("%+v", r)
	}
	// nobody continues: the connection gives up, cancels the query and is free again
	r := h.wait(id)
	if !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r)
	}
	h.manual.Store(false)
	if v := scalar(t, h.mustQuery(`SELECT 2`)); v != int64(2) {
		t.Fatalf("%v", v)
	}
}

func TestPGRequestTimeout(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	id := newID()
	start := time.Now()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `SELECT pg_sleep(30)`, Timeout: 250 * time.Millisecond})
	})
	r := h.wait(id)
	if !errors.Is(r.err, ErrTimeout) || time.Since(start) > 3*time.Second {
		t.Fatalf("%v after %v", r.err, time.Since(start))
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatalf("timed out early: %v", time.Since(start))
	}
	// the same connection works again, and no other was opened
	if v := scalar(t, h.mustQuery(`SELECT 3`)); v != int64(3) {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Opened != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestPGCancelRunning(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	id := newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: `SELECT pg_sleep(30)`}) })
	time.Sleep(200 * time.Millisecond) // let it reach the server
	h.run(func(g *gina.Ctx) { h.db.Cancel(g, id) })
	r := h.wait(id)
	if !errors.Is(r.err, ErrCanceled) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT 4`)); v != int64(4) {
		t.Fatalf("%v", v)
	}
}

func TestPGTransactions(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.mustExec(`DROP TABLE IF EXISTS t_tx`)
	h.mustExec(`CREATE TABLE t_tx (n int)`)

	// rolled back: invisible to everyone, and the owner sees its own writes
	tx, r := h.begin()
	if r.err != nil || tx.Conn == 0 {
		t.Fatalf("%+v", r)
	}
	h.txExec(tx, `INSERT INTO t_tx VALUES (1)`)
	if v := scalar(t, h.txQuery(tx, `SELECT count(*) FROM t_tx`)); v != int64(1) {
		t.Fatalf("inside: %v", v)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM t_tx`)); v != int64(0) {
		t.Fatalf("outside, before commit: %v", v)
	}
	if r := h.rollback(tx); r.err != nil || r.tag != "ROLLBACK" {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM t_tx`)); v != int64(0) {
		t.Fatalf("after rollback: %v", v)
	}

	// committed
	tx, _ = h.begin(func(r *Request) { r.Isolation = IsolationSerializable })
	h.txExec(tx, `INSERT INTO t_tx VALUES ($1)`, 2)
	if v := scalar(t, h.txQuery(tx, `SHOW transaction_isolation`)); v != "serializable" {
		t.Fatalf("%v", v)
	}
	if r := h.commit(tx); r.err != nil || r.tag != "COMMIT" {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT n FROM t_tx`)); v != int64(2) {
		t.Fatalf("after commit: %v", v)
	}

	// a finished transaction cannot be used again
	if r := h.txQuery(tx, `SELECT 1`); !errors.Is(r.err, ErrTxDone) {
		t.Fatalf("%+v", r)
	}

	// read-only
	tx, _ = h.begin(func(r *Request) { r.ReadOnly = true })
	r = h.txExec(tx, `INSERT INTO t_tx VALUES (3)`)
	var pe *Error
	if !errors.As(r.err, &pe) || pe.Code != "25006" {
		t.Fatalf("%+v", r.err)
	}
	// the transaction is now failed: statements are refused, commit rolls back (and says so)
	r = h.txQuery(tx, `SELECT 1`)
	if !errors.As(r.err, &pe) || pe.Code != "25P02" {
		t.Fatalf("%+v", r.err)
	}
	if r := h.commit(tx); !errors.Is(r.err, ErrTxDone) {
		t.Fatalf("%+v", r)
	}
	eventually(t, "the connection to be free", func() bool { return h.db.Stats().InUse == 0 })
}

func TestPGTransactionIsPrivate(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	tx, r := h.begin()
	if r.err != nil {
		t.Fatal(r.err)
	}
	// another isolate (the test goroutine speaks as the system, with Source 0) cannot use it
	id := newID()
	h.sys.SendExternal(tx.Conn, TagTx, mustMarshal(t, &Request{ID: id, SQL: "SELECT 1", ReplyTo: h.probe}, opQuery, tx.ID))
	h.expectNone(id, 100*time.Millisecond) // the refusal goes to the sender, which is nobody
	if v := scalar(t, h.txQuery(tx, `SELECT 5`)); v != int64(5) {
		t.Fatalf("%v", v)
	}
	h.rollback(tx)
}

func mustMarshal(t *testing.T, r *Request, op opCode, txid uint32) []byte {
	t.Helper()
	b, err := r.marshal(op, txid, r.ReplyTo)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPGTransactionIdleTimeout(t *testing.T) {
	cfg := pgConfig(t)
	cfg.TxTimeout = 200 * time.Millisecond
	h := newHarness(t, cfg)
	h.mustExec(`DROP TABLE IF EXISTS t_txto`)
	h.mustExec(`CREATE TABLE t_txto (n int)`)
	tx, _ := h.begin()
	h.txExec(tx, `INSERT INTO t_txto VALUES (1)`)
	time.Sleep(600 * time.Millisecond) // forgotten
	if r := h.txQuery(tx, `SELECT 1`); !errors.Is(r.err, ErrTxDone) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM t_txto`)); v != int64(0) {
		t.Fatalf("the abandoned transaction was not rolled back: %v", v)
	}
	if s := h.db.Stats(); s.InUse != 0 || s.Open != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestPGPoolLimitsAndQueue(t *testing.T) {
	cfg := pgConfig(t)
	cfg.MaxOpenConns = 3
	h := newHarness(t, cfg)
	const n = 30
	ids := make([]uint32, n)
	for i := range ids {
		ids[i] = newID()
		id := ids[i]
		h.run(func(g *gina.Ctx) {
			h.db.Query(g, &Request{ID: id, SQL: `SELECT pg_sleep(0.05), $1::int`, Args: []any{int(id)}})
		})
	}
	for _, id := range ids {
		r := h.wait(id)
		if r.err != nil || r.rows[0][1] != int64(id) {
			t.Fatalf("%d: %+v", id, r)
		}
	}
	s := h.db.Stats()
	if s.Open > 3 || s.Opened > 3 || s.WaitCount == 0 {
		t.Fatalf("%+v", s)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'gina'`)); v.(int64) > 3 {
		t.Fatalf("server sees %v sessions", v)
	}
}

func TestPGQueueLimitAndTimeout(t *testing.T) {
	cfg := pgConfig(t)
	cfg.MaxOpenConns = 1
	cfg.Queue = 2
	cfg.QueueTimeout = 300 * time.Millisecond
	h := newHarness(t, cfg)
	slow := newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: slow, SQL: `SELECT pg_sleep(1.5)`}) })
	time.Sleep(100 * time.Millisecond)
	a, b, c := newID(), newID(), newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: a, SQL: `SELECT 1`})
		h.db.Query(g, &Request{ID: b, SQL: `SELECT 2`})
		h.db.Query(g, &Request{ID: c, SQL: `SELECT 3`})
	})
	if r := h.wait(c); !errors.Is(r.err, ErrOverloaded) { // the queue holds two
		t.Fatalf("%+v", r)
	}
	for _, id := range []uint32{a, b} {
		if r := h.wait(id); !errors.Is(r.err, ErrTimeout) {
			t.Fatalf("%d: %+v", id, r)
		}
	}
	if r := h.wait(slow); r.err != nil {
		t.Fatalf("%+v", r)
	}
	if s := h.db.Stats(); s.TimedOut != 2 || s.Waiting != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestPGCancelQueued(t *testing.T) {
	cfg := pgConfig(t)
	cfg.MaxOpenConns = 1
	h := newHarness(t, cfg)
	slow, queued := newID(), newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: slow, SQL: `SELECT pg_sleep(0.6)`}) })
	time.Sleep(100 * time.Millisecond)
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: queued, SQL: `SELECT 1`}) })
	time.Sleep(50 * time.Millisecond)
	h.run(func(g *gina.Ctx) { h.db.Cancel(g, queued) })
	if r := h.wait(queued); !errors.Is(r.err, ErrCanceled) {
		t.Fatalf("%+v", r)
	}
	if r := h.wait(slow); r.err != nil {
		t.Fatalf("%+v", r)
	}
}

func TestPGWrongPassword(t *testing.T) {
	cfg := pgConfig(t)
	cfg.Password = "wrong"
	h := newHarness(t, cfg)
	r := h.query(`SELECT 1`)
	var pe *Error
	if !errors.Is(r.err, ErrConnect) || !errors.As(r.err, &pe) || pe.Code != "28P01" {
		t.Fatalf("%+v", r.err)
	}
	if s := h.db.Stats(); s.Failed == 0 || s.Open != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestPGConnectRefused(t *testing.T) {
	cfg := pgConfig(t)
	cfg.Addr = mustAddr("127.0.0.1:1")
	h := newHarness(t, cfg)
	if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrConnect) {
		t.Fatalf("%+v", r.err)
	}
}

func TestPGServerKillsConnection(t *testing.T) {
	cfg := pgConfig(t)
	cfg.MaxOpenConns = 2
	h := newHarness(t, cfg)
	// Two connections, both idle afterwards.
	a, b := newID(), newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: a, SQL: `SELECT pg_sleep(0.2), pg_backend_pid()`})
		h.db.Query(g, &Request{ID: b, SQL: `SELECT pg_sleep(0.2), pg_backend_pid()`})
	})
	pids := map[int64]bool{h.wait(a).rows[0][1].(int64): true, h.wait(b).rows[0][1].(int64): true}
	if len(pids) != 2 {
		t.Fatalf("expected two backends, got %v", pids)
	}
	eventually(t, "both connections to be idle", func() bool { return h.db.Stats().Idle == 2 })
	// The most recently used connection runs this and kills the other, idle one.
	mine := scalar(t, h.mustQuery(`SELECT pg_backend_pid()`)).(int64)
	var other int64
	for p := range pids {
		if p != mine {
			other = p
		}
	}
	if ok := scalar(t, h.mustQuery(`SELECT pg_terminate_backend($1)`, other)); ok != true {
		t.Fatalf("%v", ok)
	}
	eventually(t, "the pool to notice", func() bool { return h.db.Stats().Closed >= 1 })
	// Requests keep working, on a new connection when needed.
	for i := 0; i < 3; i++ {
		h.mustQuery(`SELECT pg_sleep(0.05)`)
	}
	if s := h.db.Stats(); s.Open > 2 {
		t.Fatalf("%+v", s)
	}
}

func TestPGServerKillsRunningQuery(t *testing.T) {
	cfg := pgConfig(t)
	cfg.MaxOpenConns = 2
	h := newHarness(t, cfg)
	id := newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: `SELECT pg_sleep(30), pg_backend_pid()`}) })
	time.Sleep(200 * time.Millisecond)
	h.mustExec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(30)%' AND pid <> pg_backend_pid()`)
	r := h.wait(id)
	var pe *Error
	if r.err == nil || !(errors.Is(r.err, ErrConnLost) || (errors.As(r.err, &pe) && pe.Code == "57P01")) {
		t.Fatalf("%+v", r.err)
	}
	if v := scalar(t, h.mustQuery(`SELECT 6`)); v != int64(6) {
		t.Fatalf("%v", v)
	}
}

func TestPGLifetimeAndIdle(t *testing.T) {
	cfg := pgConfig(t)
	cfg.ConnMaxIdleTime = 150 * time.Millisecond
	h := newHarness(t, cfg)
	h.mustQuery(`SELECT 1`)
	if s := h.db.Stats(); s.Idle != 1 {
		t.Fatalf("%+v", s)
	}
	eventually(t, "the idle connection to be closed", func() bool { return h.db.Stats().Open == 0 })

	cfg = pgConfig(t)
	cfg.ConnMaxLifetime = 200 * time.Millisecond
	h = newHarness(t, cfg)
	pid := scalar(t, h.mustQuery(`SELECT pg_backend_pid()`))
	time.Sleep(400 * time.Millisecond)
	eventually(t, "the old connection to be retired", func() bool { return h.db.Stats().Open == 0 })
	if v := scalar(t, h.mustQuery(`SELECT pg_backend_pid()`)); v == pid {
		t.Fatal("the connection outlived its lifetime")
	}
}

func TestPGMaxIdle(t *testing.T) {
	cfg := pgConfig(t)
	cfg.MaxOpenConns = 5
	cfg.MaxIdleConns = 1
	h := newHarness(t, cfg)
	ids := make([]uint32, 5)
	for i := range ids {
		ids[i] = newID()
		id := ids[i]
		h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: `SELECT pg_sleep(0.2)`}) })
	}
	for _, id := range ids {
		h.wait(id)
	}
	eventually(t, "surplus connections to close", func() bool { s := h.db.Stats(); return s.Open == 1 && s.Idle == 1 })
}

func TestPGSessionLeftInTransactionIsReset(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	h.mustExec(`DROP TABLE IF EXISTS t_reset`)
	h.mustExec(`CREATE TABLE t_reset (n int)`)
	// A statement that opens a transaction and walks away: the connection rolls it back before reuse.
	h.mustExec(`BEGIN; INSERT INTO t_reset VALUES (1)`)
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM t_reset`)); v != int64(0) {
		t.Fatalf("a leaked transaction was visible to the next request: %v", v)
	}
}

func TestPGParams(t *testing.T) {
	cfg := pgConfig(t)
	cfg.Params = map[string]string{"search_path": "pg_catalog", "application_name": "ignored", "client_encoding": "LATIN1"}
	cfg.ApplicationName = "gina-test"
	h := newHarness(t, cfg)
	if v := scalar(t, h.mustQuery(`SHOW search_path`)); v != "pg_catalog" {
		t.Fatalf("%v", v)
	}
	if v := scalar(t, h.mustQuery(`SHOW client_encoding`)); v != "UTF8" {
		t.Fatalf("%v", v)
	}
	if v := scalar(t, h.mustQuery(`SHOW application_name`)); v != "gina-test" {
		t.Fatalf("%v", v)
	}
}

func TestPGHugeValuesAndArgs(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	big := make([]byte, 3<<20)
	for i := range big {
		big[i] = byte(i * 7)
	}
	r := h.mustQuery(`SELECT $1::bytea, length($1::bytea)`, big)
	got := r.rows[0][0].([]byte)
	if len(got) != len(big) || string(got) != string(big) || r.rows[0][1] != int64(len(big)) {
		t.Fatalf("round trip of %d bytes failed (%d back)", len(big), len(got))
	}
}

func TestPGAckEveryRequestEnds(t *testing.T) {
	h := newHarness(t, pgConfig(t))
	// requests from one isolate, pipelined: each gets its own answer
	ids := []uint32{newID(), newID(), newID()}
	h.run(func(g *gina.Ctx) {
		for i, id := range ids {
			h.db.Query(g, &Request{ID: id, SQL: `SELECT $1::int`, Args: []any{i}})
		}
	})
	for i, id := range ids {
		if v := scalar(t, h.wait(id)); v != int64(i) {
			t.Fatalf("%d: %v", i, v)
		}
	}
}

func mustAddr(s string) (ap netip.AddrPort) {
	ap, err := netip.ParseAddrPort(s)
	if err != nil {
		panic(err)
	}
	return ap
}

// TestPGTLS needs a server with TLS 1.3 and a certificate for "localhost":
//
//	GINA_PG_TLS_ADDR=127.0.0.1:55433 GINA_PG_TLS_CERT=server.crt go test -run TestPGTLS
//
// (the credentials are those of pgConfig). It is skipped without them.
func TestPGTLS(t *testing.T) {
	addr, certFile := os.Getenv("GINA_PG_TLS_ADDR"), os.Getenv("GINA_PG_TLS_CERT")
	if addr == "" || certFile == "" {
		t.Skip("GINA_PG_TLS_ADDR and GINA_PG_TLS_CERT are not set")
	}
	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		t.Fatal("no certificate in " + certFile)
	}
	t.Setenv("GINA_PG_ADDR", addr)
	cfg := pgConfig(t)
	cfg.TLS = &gtls.ClientConfig{ServerName: "localhost", RootCAs: roots}
	h := newHarness(t, cfg)
	r := h.mustQuery(`SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()`)
	if r.rows[0][0] != true || r.rows[0][1] != "TLSv1.3" {
		t.Fatalf("%v", r.rows)
	}
	t.Logf("cipher %v", r.rows[0][2])
	if r := h.mustQuery(`SELECT n FROM generate_series(1, 50000) n`); len(r.rows) != 50000 {
		t.Fatalf("%d rows", len(r.rows))
	}

	cfg.TLS = &gtls.ClientConfig{ServerName: "db.example.com", RootCAs: roots}
	h = newHarness(t, cfg)
	if r := h.query(`SELECT 1`); !errors.Is(r.err, ErrConnect) {
		t.Fatalf("a certificate for another name was accepted: %v", r.err)
	}
}
