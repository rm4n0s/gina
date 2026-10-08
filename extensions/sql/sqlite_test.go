package sql

import (
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

// Tests of the SQLite driver. They need cgo (the driver is a C library) and a
// writable temporary directory, and nothing else.

func sqliteConfig(t *testing.T) Config {
	t.Helper()
	if _, err := sqliteOpen(":memory:"); err != nil {
		t.Skipf("SQLite is not available: %v", err)
	}
	return Config{Driver: DriverSQLite, Path: filepath.Join(t.TempDir(), "test.db"), Shard: 1}
}

func newSQLite(t *testing.T) *harness { return newHarness(t, sqliteConfig(t)) }

func TestSQLiteQueryAndTypes(t *testing.T) {
	h := newSQLite(t)
	h.mustExec(`CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, f REAL, s TEXT, ok BOOLEAN, bin BLOB, ts TIMESTAMP, d DATE, n NUMERIC, x)`)
	when := time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC)
	r := h.mustExec(`INSERT INTO t (i, f, s, ok, bin, ts, d, n, x) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		int64(math.MaxInt64), 2.5, "héllo\nworld", true, []byte{0, 1, 2, 255}, when, when, "12345.5", int32(7))
	if r.tag != "INSERT 0 1" || r.n != 1 {
		t.Fatalf("%+v", r)
	}
	h.mustExec(`INSERT INTO t (i) VALUES ($1)`, nil) // NULLs everywhere else
	r = h.mustQuery(`SELECT i, f, s, ok, bin, ts, d, n, x FROM t ORDER BY id`)
	if len(r.rows) != 2 || r.tag != "SELECT 2" || r.n != 2 {
		t.Fatalf("%+v", r)
	}
	row := r.rows[0]
	if row[0] != int64(math.MaxInt64) || row[1] != 2.5 || row[2] != "héllo\nworld" || row[3] != true {
		t.Fatalf("%#v", row)
	}
	if b, _ := row[4].([]byte); string(b) != "\x00\x01\x02\xff" {
		t.Fatalf("blob %#v", row[4])
	}
	if ts, _ := row[5].(time.Time); !ts.Equal(when) {
		t.Fatalf("timestamp %#v", row[5])
	}
	if d, _ := row[6].(time.Time); d.Format("2006-01-02") != "2024-05-06" {
		t.Fatalf("date %#v", row[6])
	}
	if row[7] != "12345.5" || row[8] != int64(7) { // NUMERIC has no Go type of its own; x has no declared type
		t.Fatalf("%#v", row[7:])
	}
	for i, v := range r.rows[1][1:] {
		if v != nil {
			t.Fatalf("column %d should be NULL, got %#v", i+1, v)
		}
	}
	want := []uint32{OIDInt8, OIDFloat8, OIDText, OIDBool, OIDBytea, OIDTimestamptz, OIDDate, OIDNumeric, OIDInt8}
	for i, c := range r.cols {
		if c.OID != want[i] {
			t.Fatalf("column %s has OID %d, want %d", c.Name, c.OID, want[i])
		}
	}
}

func TestSQLiteExpressionTypes(t *testing.T) {
	h := newSQLite(t)
	// no declared types: the first value that is not NULL decides
	r := h.mustQuery(`SELECT 1, 2.5, 'a', x'ff', NULL`)
	want := []uint32{OIDInt8, OIDFloat8, OIDText, OIDBytea, OIDText}
	for i, c := range r.cols {
		if c.OID != want[i] {
			t.Fatalf("column %d has OID %d, want %d", i, c.OID, want[i])
		}
	}
	if r.rows[0][0] != int64(1) || r.rows[0][1] != 2.5 || r.rows[0][2] != "a" || r.rows[0][4] != nil {
		t.Fatalf("%#v", r.rows[0])
	}
	// a column that holds different types becomes text, or float if all are numbers
	r = h.mustQuery(`SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 'two' UNION ALL SELECT 3)`)
	if r.cols[0].OID != OIDText || r.rows[1][0] != "two" {
		t.Fatalf("%+v", r)
	}
	r = h.mustQuery(`SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2.5)`)
	if r.cols[0].OID != OIDFloat8 || r.rows[0][0] != 1.0 || r.rows[1][0] != 2.5 {
		t.Fatalf("%+v", r)
	}
	// typed arguments are bound as what they are
	r = h.mustQuery(`SELECT typeof($1), typeof($2), typeof($3), typeof($4), typeof($5), typeof($6)`, 1, 1.5, "s", []byte{1}, true, nil)
	for i, w := range []string{"integer", "real", "text", "blob", "integer", "null"} {
		if r.rows[0][i] != w {
			t.Fatalf("typeof($%d) = %v, want %s", i+1, r.rows[0][i], w)
		}
	}
	// placeholders keep their numbers whatever their order, and may repeat
	r = h.mustQuery(`SELECT $2, $1, $2, '$1', "$1" || ?2 -- $1
		/* $2 */`, "a", "b")
	if len(r.rows) != 1 || r.rows[0][0] != "b" || r.rows[0][1] != "a" || r.rows[0][2] != "b" || r.rows[0][3] != "$1" {
		t.Fatalf("%#v %v", r.rows, r.err)
	}
}

func TestSQLiteExecMultiAndNoRows(t *testing.T) {
	h := newSQLite(t)
	r := h.mustExec(`CREATE TABLE m (n INTEGER); INSERT INTO m VALUES (1),(2),(3); UPDATE m SET n = n + 1 WHERE n > 1`)
	if r.tag != "CREATE" { // the tag follows the first statement
		t.Fatalf("%+v", r)
	}
	r = h.mustExec(`UPDATE m SET n = n + 1 WHERE n > 1`)
	if r.tag != "UPDATE 2" || r.n != 2 {
		t.Fatalf("%+v", r)
	}
	// a DDL statement does not report the rows of the DML before it
	if r = h.mustExec(`CREATE TABLE other (n INTEGER)`); r.n != 0 || r.tag != "CREATE" {
		t.Fatalf("%+v", r)
	}
	// a Query of a statement with no result set ends with Done
	r = h.mustQuery(`DELETE FROM m WHERE n = $1`, 1)
	if r.kind != ReplyDone || r.tag != "DELETE 1" || r.n != 1 {
		t.Fatalf("%+v", r)
	}
	// RETURNING makes it rows
	r = h.mustQuery(`INSERT INTO m VALUES (10), (11) RETURNING n`)
	if r.kind != ReplyRows || len(r.rows) != 2 || r.tag != "INSERT 0 2" || r.n != 2 {
		t.Fatalf("%+v", r)
	}
	// an empty statement
	if r := h.mustExec(``); r.tag != "" || r.n != 0 {
		t.Fatalf("%+v", r)
	}
	if r := h.mustQuery(` -- nothing`); r.kind != ReplyDone {
		t.Fatalf("%+v", r)
	}
	// a result with no rows still has its columns
	r = h.mustQuery(`SELECT n, 'x' AS name FROM m WHERE 0`)
	if r.kind != ReplyRows || len(r.rows) != 0 || len(r.cols) != 2 || r.cols[1].Name != "name" || r.tag != "SELECT 0" {
		t.Fatalf("%+v", r)
	}
	// Exec of a SELECT discards the rows
	if r := h.mustExec(`SELECT n FROM m`); r.kind != ReplyDone {
		t.Fatalf("%+v", r)
	}
}

func TestSQLiteErrors(t *testing.T) {
	h := newSQLite(t)
	h.mustExec(`CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, age INTEGER CHECK (age >= 0))`)
	h.mustExec(`INSERT INTO u (email, age) VALUES ('a@x', 1)`)
	var pe *Error
	r := h.exec(`INSERT INTO u (email, age) VALUES ('a@x', 1)`)
	if !errors.As(r.err, &pe) || pe.Client != ClientNone || pe.Code != "SQLITE_CONSTRAINT_UNIQUE" || pe.Table != "u" || pe.Column != "email" {
		t.Fatalf("%+v", r.err)
	}
	r = h.exec(`INSERT INTO u (email) VALUES (NULL)`)
	if !errors.As(r.err, &pe) || pe.Code != "SQLITE_CONSTRAINT_NOTNULL" || pe.Column != "email" {
		t.Fatalf("%+v", r.err)
	}
	r = h.exec(`INSERT INTO u (email, age) VALUES ('b@x', -1)`)
	if !errors.As(r.err, &pe) || pe.Code != "SQLITE_CONSTRAINT_CHECK" {
		t.Fatalf("%+v", r.err)
	}
	r = h.query(`SELEKT 1`)
	if !errors.As(r.err, &pe) || pe.Code != "SQLITE_ERROR" || !strings.Contains(pe.Message, "syntax error") {
		t.Fatalf("%+v", r.err)
	}
	r = h.query(`SELECT * FROM nope`)
	if !errors.As(r.err, &pe) || !strings.Contains(pe.Message, "no such table") {
		t.Fatalf("%+v", r.err)
	}
	// an error halfway through a result ends it with the error, not with rows
	r = h.query(`SELECT abs(-9223372036854775807 - n) FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)`)
	if r.err == nil || !strings.Contains(r.err.Error(), "integer overflow") {
		t.Fatalf("%+v", r)
	}
	// the connection survives all of it
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM u`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Opened != 1 || s.Open != 1 {
		t.Fatalf("%+v", s)
	}
	// bad arguments
	if r := h.query(`SELECT $1`, struct{}{}); !errors.Is(r.err, ErrBadRequest) {
		t.Fatalf("%+v", r)
	}
	if r := h.query(`SELECT $1, $2`, 1); r.err == nil {
		t.Fatalf("missing argument accepted: %+v", r)
	}
}

func TestSQLiteStreamingBackpressure(t *testing.T) {
	h := newSQLite(t)
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < 1000) SELECT n, printf('%0100d', n) FROM s`, BatchRows: 100})
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
	if r.rows[999][0] != int64(1000) || r.rows[0][1] != strings.Repeat("0", 99)+"1" {
		t.Fatalf("%v", r.rows[999])
	}
	eventually(t, "the connection to be idle", func() bool { return h.db.Stats().Idle == 1 })
}

func TestSQLiteStreamBigResult(t *testing.T) {
	h := newSQLite(t)
	// rows of about 200 bytes: batches closed by size as well as by count
	r := h.mustQuery(`WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < 20000) SELECT n, printf('%0200d', n) FROM s`)
	if len(r.rows) != 20000 || r.batches < 20 {
		t.Fatalf("%d rows in %d batches", len(r.rows), r.batches)
	}
	for i, row := range r.rows {
		if row[0] != int64(i+1) {
			t.Fatalf("row %d: %v", i, row[0])
		}
	}
}

func TestSQLiteStreamCancel(t *testing.T) {
	h := newSQLite(t)
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s) SELECT n FROM s`, BatchRows: 10})
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
	h.manual.Store(false)
	if v := scalar(t, h.mustQuery(`SELECT 1`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Open != 1 {
		t.Fatalf("the cancelled query should not have cost the connection: %+v", s)
	}
}

func TestSQLiteStreamAbandoned(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.StreamTimeout = 200 * time.Millisecond
	h := newHarness(t, cfg)
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s) SELECT n FROM s`, BatchRows: 10})
	})
	if r := h.wait(id); !r.partial {
		t.Fatalf("%+v", r)
	}
	r := h.wait(id)
	if !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r)
	}
	h.manual.Store(false)
	if v := scalar(t, h.mustQuery(`SELECT 2`)); v != int64(2) {
		t.Fatalf("%v", v)
	}
}

func TestSQLiteRequestTimeout(t *testing.T) {
	// Request.Timeout also covers a consumer that is slow to ask for more
	h := newSQLite(t)
	h.manual.Store(true)
	id := newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s) SELECT n FROM s`, BatchRows: 10, Timeout: 200 * time.Millisecond})
	})
	if r := h.wait(id); !r.partial {
		t.Fatalf("%+v", r)
	}
	if r := h.wait(id); !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r)
	}
	h.manual.Store(false)
	// and one that is fast enough but reads on: the deadline is checked as rows are read
	id = newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: id, SQL: `WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s) SELECT n FROM s`, BatchRows: 1 << 20, Timeout: 100 * time.Millisecond})
	})
	if r := h.wait(id); !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r.err)
	}
	if v := scalar(t, h.mustQuery(`SELECT 3`)); v != int64(3) {
		t.Fatalf("%v", v)
	}
}

func TestSQLiteTransactions(t *testing.T) {
	h := newSQLite(t)
	h.mustExec(`CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)`)
	h.mustExec(`INSERT INTO acct VALUES (1, 100), (2, 100)`)

	tx, r := h.begin()
	if r.err != nil || r.kind != ReplyTx {
		t.Fatalf("%+v", r)
	}
	if r := h.txExec(tx, `UPDATE acct SET bal = bal - 30 WHERE id = $1`, 1); r.err != nil || r.n != 1 {
		t.Fatalf("%+v", r)
	}
	h.txExec(tx, `UPDATE acct SET bal = bal + 30 WHERE id = $1`, 2)
	if v := scalar(t, h.txQuery(tx, `SELECT bal FROM acct WHERE id = 1`)); v != int64(70) {
		t.Fatalf("%v", v)
	}
	if r := h.commit(tx); r.err != nil || r.tag != "COMMIT" {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT bal FROM acct WHERE id = 2`)); v != int64(130) {
		t.Fatalf("%v", v)
	}
	// the transaction is over
	if r := h.txQuery(tx, `SELECT 1`); !errors.Is(r.err, ErrTxDone) {
		t.Fatalf("%+v", r)
	}

	tx, _ = h.begin()
	h.txExec(tx, `DELETE FROM acct`)
	if v := scalar(t, h.txQuery(tx, `SELECT count(*) FROM acct`)); v != int64(0) {
		t.Fatalf("%v", v)
	}
	if r := h.rollback(tx); r.err != nil || r.tag != "ROLLBACK" {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM acct`)); v != int64(2) {
		t.Fatalf("rollback did not undo: %v", v)
	}

	// an error inside a transaction leaves it usable, as in SQLite
	tx, _ = h.begin()
	h.txExec(tx, `UPDATE acct SET bal = 0 WHERE id = 1`)
	if r := h.txExec(tx, `INSERT INTO acct VALUES (1, 5)`); r.err == nil {
		t.Fatal("duplicate key accepted")
	}
	h.commit(tx)
	if v := scalar(t, h.mustQuery(`SELECT bal FROM acct WHERE id = 1`)); v != int64(0) {
		t.Fatalf("%v", v)
	}
}

func TestSQLiteReadOnlyTransaction(t *testing.T) {
	h := newSQLite(t)
	h.mustExec(`CREATE TABLE ro (n INTEGER)`)
	tx, r := h.begin(func(r *Request) { r.ReadOnly = true; r.Isolation = IsolationSerializable })
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r := h.txExec(tx, `INSERT INTO ro VALUES (1)`); r.err == nil || !strings.Contains(r.err.Error(), "readonly") {
		t.Fatalf("a write in a read-only transaction: %+v", r)
	}
	h.commit(tx)
	// and the connection is writable again afterwards
	if r := h.mustExec(`INSERT INTO ro VALUES (2)`); r.n != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestSQLiteTransactionIsPrivate(t *testing.T) {
	h := newSQLite(t)
	tx, r := h.begin()
	if r.err != nil {
		t.Fatal(r.err)
	}
	id := newID()
	h.sys.SendExternal(tx.Conn, TagTx, mustMarshal(t, &Request{ID: id, SQL: "SELECT 1", ReplyTo: h.probe}, opQuery, tx.ID))
	h.expectNone(id, 100*time.Millisecond)
	if v := scalar(t, h.txQuery(tx, `SELECT 5`)); v != int64(5) {
		t.Fatalf("%v", v)
	}
	h.rollback(tx)
}

func TestSQLiteTransactionIdleTimeout(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.TxTimeout = 200 * time.Millisecond
	h := newHarness(t, cfg)
	h.mustExec(`CREATE TABLE txto (n INTEGER)`)
	tx, _ := h.begin()
	h.txExec(tx, `INSERT INTO txto VALUES (1)`)
	time.Sleep(600 * time.Millisecond) // forgotten
	if r := h.txQuery(tx, `SELECT 1`); !errors.Is(r.err, ErrTxDone) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM txto`)); v != int64(0) {
		t.Fatalf("the abandoned transaction was not rolled back: %v", v)
	}
	if s := h.db.Stats(); s.InUse != 0 || s.Open != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestSQLiteSessionLeftInTransactionIsReset(t *testing.T) {
	h := newSQLite(t)
	h.mustExec(`CREATE TABLE reset (n INTEGER)`)
	h.mustExec(`BEGIN; INSERT INTO reset VALUES (1)`)
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM reset`)); v != int64(0) {
		t.Fatalf("a leaked transaction was visible to the next request: %v", v)
	}
	// a COMMIT sent as a statement inside a transaction ends it quietly
	tx, _ := h.begin()
	h.txExec(tx, `INSERT INTO reset VALUES (2)`)
	h.txExec(tx, `COMMIT`)
	if r := h.txQuery(tx, `SELECT 1`); !errors.Is(r.err, ErrTxDone) {
		t.Fatalf("%+v", r)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM reset`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.InUse != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestSQLitePoolOneConnectionQueues(t *testing.T) {
	h := newSQLite(t) // MaxOpenConns defaults to 1
	h.mustExec(`CREATE TABLE q (n INTEGER)`)
	const n = 30
	ids := make([]uint32, n)
	for i := range ids {
		ids[i] = newID()
		id := ids[i]
		h.run(func(g *gina.Ctx) {
			h.db.Exec(g, &Request{ID: id, SQL: `INSERT INTO q VALUES ($1)`, Args: []any{int(id)}})
		})
	}
	for _, id := range ids {
		if r := h.wait(id); r.err != nil || r.n != 1 {
			t.Fatalf("%d: %+v", id, r)
		}
	}
	if s := h.db.Stats(); s.Open != 1 || s.Opened != 1 || s.WaitCount == 0 {
		t.Fatalf("%+v", s)
	}
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM q`)); v != int64(n) {
		t.Fatalf("%v", v)
	}
}

func TestSQLiteSeveralConnectionsWAL(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.MaxOpenConns = 3
	cfg.Params = map[string]string{"journal_mode": "WAL"}
	h := newHarness(t, cfg)
	if v := scalar(t, h.mustQuery(`PRAGMA journal_mode`)); v != "wal" {
		t.Fatalf("%v", v)
	}
	h.mustExec(`CREATE TABLE w (n INTEGER)`)
	h.mustExec(`INSERT INTO w VALUES (1)`)
	// A transaction holds one connection and a write; a reader on another sees the
	// committed state, without waiting for a lock that could never be released.
	tx, _ := h.begin()
	h.txExec(tx, `INSERT INTO w VALUES (2)`)
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM w`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	h.commit(tx)
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM w`)); v != int64(2) {
		t.Fatalf("%v", v)
	}
	if s := h.db.Stats(); s.Opened != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestSQLiteBusyIsAnErrorNotADeadlock(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.MaxOpenConns = 2 // the default rollback journal: a writer excludes the others
	h := newHarness(t, cfg)
	h.mustExec(`CREATE TABLE b (n INTEGER)`)
	tx, _ := h.begin()
	h.txExec(tx, `INSERT INTO b VALUES (1)`)
	start := time.Now()
	var pe *Error
	r := h.exec(`INSERT INTO b VALUES (2)`) // runs on the second connection
	if !errors.As(r.err, &pe) || pe.Code != "SQLITE_BUSY" || time.Since(start) > 2*time.Second {
		t.Fatalf("%+v after %v", r.err, time.Since(start))
	}
	h.commit(tx)
	if r := h.mustExec(`INSERT INTO b VALUES (3)`); r.n != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestSQLiteQueueLimitAndTimeout(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.Queue = 2
	cfg.QueueTimeout = 300 * time.Millisecond
	h := newHarness(t, cfg)
	tx, _ := h.begin() // holds the only connection
	a, b, c := newID(), newID(), newID()
	h.run(func(g *gina.Ctx) {
		h.db.Query(g, &Request{ID: a, SQL: `SELECT 1`})
		h.db.Query(g, &Request{ID: b, SQL: `SELECT 2`})
		h.db.Query(g, &Request{ID: c, SQL: `SELECT 3`})
	})
	if r := h.wait(c); !errors.Is(r.err, ErrOverloaded) {
		t.Fatalf("%+v", r)
	}
	for _, id := range []uint32{a, b} {
		if r := h.wait(id); !errors.Is(r.err, ErrTimeout) {
			t.Fatalf("%d: %+v", id, r)
		}
	}
	h.rollback(tx)
	if v := scalar(t, h.mustQuery(`SELECT 4`)); v != int64(4) {
		t.Fatalf("%v", v)
	}
}

func TestSQLiteCancelQueued(t *testing.T) {
	h := newSQLite(t)
	tx, _ := h.begin()
	id := newID()
	h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: `SELECT 1`}) })
	time.Sleep(50 * time.Millisecond)
	h.run(func(g *gina.Ctx) { h.db.Cancel(g, id) })
	if r := h.wait(id); !errors.Is(r.err, ErrCanceled) {
		t.Fatalf("%+v", r)
	}
	h.rollback(tx)
}

func TestSQLiteLifetimeAndIdle(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.ConnMaxIdleTime = 150 * time.Millisecond
	h := newHarness(t, cfg)
	h.mustQuery(`SELECT 1`)
	if s := h.db.Stats(); s.Idle != 1 {
		t.Fatalf("%+v", s)
	}
	eventually(t, "the idle connection to be closed", func() bool { return h.db.Stats().Open == 0 })
	h.mustQuery(`SELECT 1`) // and a new one opens

	cfg = sqliteConfig(t)
	cfg.ConnMaxLifetime = 200 * time.Millisecond
	h = newHarness(t, cfg)
	h.mustQuery(`SELECT 1`)
	eventually(t, "the old connection to be retired", func() bool { return h.db.Stats().Open == 0 })
	h.mustQuery(`SELECT 1`)
	if s := h.db.Stats(); s.Opened != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestSQLiteParams(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.Params = map[string]string{"foreign_keys": "ON", "busy_timeout": "1234"}
	h := newHarness(t, cfg)
	if v := scalar(t, h.mustQuery(`PRAGMA foreign_keys`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	if v := scalar(t, h.mustQuery(`PRAGMA busy_timeout`)); v != int64(1234) {
		t.Fatalf("%v", v)
	}
	h = newSQLite(t)
	if v := scalar(t, h.mustQuery(`PRAGMA busy_timeout`)); v != int64(0) {
		t.Fatalf("the default must not block the shard: %v", v)
	}
}

func TestSQLiteOpenFailure(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.Path = filepath.Join(t.TempDir(), "missing", "dir", "x.db")
	h := newHarness(t, cfg)
	r := h.query(`SELECT 1`)
	var pe *Error
	if !errors.Is(r.err, ErrConnect) || !errors.As(r.err, &pe) || pe.Code != "SQLITE_CANTOPEN" {
		t.Fatalf("%+v", r.err)
	}
	if s := h.db.Stats(); s.Failed != 1 || s.Open != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestSQLiteInMemory(t *testing.T) {
	cfg := sqliteConfig(t)
	cfg.Path = ":memory:"
	h := newHarness(t, cfg)
	h.mustExec(`CREATE TABLE mem (n INTEGER)`)
	h.mustExec(`INSERT INTO mem VALUES (1)`)
	if v := scalar(t, h.mustQuery(`SELECT n FROM mem`)); v != int64(1) {
		t.Fatalf("%v", v)
	}
	// a shared in-memory database may have several connections
	cfg.Path = "file:" + t.Name() + "?mode=memory&cache=shared"
	cfg.MaxOpenConns = 2
	h = newHarness(t, cfg)
	h.mustExec(`CREATE TABLE mem (n INTEGER)`)
	if v := scalar(t, h.mustQuery(`SELECT count(*) FROM mem`)); v != int64(0) {
		t.Fatalf("%v", v)
	}
}

func TestSQLitePersistsAcrossPools(t *testing.T) {
	cfg := sqliteConfig(t)
	h := newHarness(t, cfg)
	h.mustExec(`CREATE TABLE p (n INTEGER)`)
	h.mustExec(`INSERT INTO p VALUES (42)`)
	h.sys.Stop()
	h.sys.Wait()
	h2 := newHarness(t, cfg)
	if v := scalar(t, h2.mustQuery(`SELECT n FROM p`)); v != int64(42) {
		t.Fatalf("%v", v)
	}
}

func TestSQLiteHugeValues(t *testing.T) {
	h := newSQLite(t)
	big := make([]byte, 3<<20)
	for i := range big {
		big[i] = byte(i * 7)
	}
	r := h.mustQuery(`SELECT $1, length($1)`, big)
	got := r.rows[0][0].([]byte)
	if len(got) != len(big) || string(got) != string(big) || r.rows[0][1] != int64(len(big)) {
		t.Fatalf("round trip of %d bytes failed (%d back)", len(big), len(got))
	}
}

func TestSQLiteAckEveryRequestEnds(t *testing.T) {
	h := newSQLite(t)
	h.mustExec(`CREATE TABLE ack (n INTEGER)`)
	ids := make([]uint32, 200)
	for i := range ids {
		ids[i] = newID()
		id := ids[i]
		sql := []string{`SELECT 1`, `INSERT INTO ack VALUES (1)`, `SELEKT`, `SELECT n FROM ack`}[i%4]
		h.run(func(g *gina.Ctx) { h.db.Query(g, &Request{ID: id, SQL: sql}) })
	}
	for i, id := range ids {
		r := h.wait(id)
		if (i%4 == 2) != (r.err != nil) {
			t.Fatalf("request %d: %+v", i, r)
		}
	}
	h.expectNone(ids[0], 100*time.Millisecond)
}

func TestSQLiteConfig(t *testing.T) {
	bad := []Config{
		{Driver: DriverSQLite},
		{Driver: DriverSQLite, Path: "x.db", Params: map[string]string{"journal_mode": "WAL; DROP TABLE x"}},
		{Driver: DriverSQLite, Path: "x.db", Params: map[string]string{"a b": "1"}},
		{Driver: DriverSQLite, Path: ":memory:", MaxOpenConns: 2},
		{Driver: DriverSQLite, Path: ":memory:", ConnMaxIdleTime: time.Second},
		{Driver: Driver(9), Path: "x.db"},
	}
	for i, c := range bad {
		if _, err := New(c); err == nil {
			t.Errorf("config %d accepted: %+v", i, c)
		}
	}
	db, err := New(Config{Driver: DriverSQLite, Path: "x.db"})
	if err != nil || db.cfg.MaxOpenConns != 1 {
		t.Fatalf("%v %+v", err, db)
	}
	if _, err := New(Config{Driver: DriverSQLite, Path: "file:m?mode=memory&cache=shared", MaxOpenConns: 4}); err != nil {
		t.Fatal(err)
	}
	// PostgreSQL still wants its address
	if _, err := New(Config{}); err == nil {
		t.Fatal("a PostgreSQL config without an address was accepted")
	}
}

func TestSQLiteRewrite(t *testing.T) {
	for in, want := range map[string]string{
		`SELECT $1`:                       `SELECT ?1`,
		`SELECT $12, $2`:                  `SELECT ?12, ?2`,
		`SELECT 'it''s $1', $1`:           `SELECT 'it''s $1', ?1`,
		`SELECT "a$1", $1`:                `SELECT "a$1", ?1`,
		"SELECT `$1`, [$1], $1":           "SELECT `$1`, [$1], ?1",
		"SELECT 1 -- $1\n, $1":            "SELECT 1 -- $1\n, ?1",
		`SELECT /* $1 */ $1`:              `SELECT /* $1 */ ?1`,
		`SELECT a$1, $name, $`:            `SELECT a$1, $name, $`,
		`SELECT 'unterminated $1`:         `SELECT 'unterminated $1`,
		`SELECT /* unterminated $1`:       `SELECT /* unterminated $1`,
		`SELECT ?, ?2, :a, @b`:            `SELECT ?, ?2, :a, @b`,
		`INSERT INTO t VALUES ($1, '$2')`: `INSERT INTO t VALUES (?1, '$2')`,
	} {
		if got := sqliteSQL(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{
		`select 1`: "SELECT", "  \n INSERT INTO t": "INSERT", `-- c` + "\n" + `UPDATE t`: "UPDATE", `/* c */ delete from t`: "DELETE",
		`REPLACE INTO t VALUES (1)`: "INSERT", `(select 1)`: "SELECT", ``: "", `-- only`: "", `;`: "",
	} {
		if got := sqlVerb(in); got != want {
			t.Errorf("%q: verb %q, want %q", in, got, want)
		}
	}
}

func TestSQLiteArgKinds(t *testing.T) {
	when := time.Date(2024, 1, 2, 3, 4, 5, 6, time.FixedZone("x", 3600))
	r := &Request{ID: 1, SQL: "x", Args: []any{nil, "s", []byte{1, 2}, 5, uint8(6), uint64(math.MaxUint64), 1.5, float32(2.5), true, false, when, math.Inf(1)}}
	b, err := r.marshal(opQuery, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	q, err := parseRequest(b)
	if err != nil {
		t.Fatal(err)
	}
	args, err := sqliteArgs(q.params)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]any, len(args))
	for i, a := range args {
		got[i] = a.Value
	}
	if got[0] != nil || got[1] != "s" || string(got[2].([]byte)) != "\x01\x02" || got[3] != int64(5) || got[4] != int64(6) ||
		got[5] != strings.TrimSpace("18446744073709551615") || got[6] != 1.5 || got[7] != 2.5 || got[8] != true || got[9] != false ||
		!got[10].(time.Time).Equal(when) || got[11] != math.Inf(1) {
		t.Fatalf("%#v", got)
	}
	// to PostgreSQL they are still text
	if q.params[3].format != 0 || string(q.params[3].data) != "5" || string(q.params[8].data) != "true" {
		t.Fatalf("%+v", q.params)
	}
}
