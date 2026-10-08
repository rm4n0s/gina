package sql

// The SQLite connection: the isolate that owns one database handle, in place of the
// PostgreSQL connection's socket. It plays the same part for the pool (tagOpen,
// tagReady, tagRun, tagFree, tagRetire, tagClose), takes the same requests, and
// answers with the same replies, so everything above it is shared.
//
// What differs is that SQLite is a library, not a server. There is nothing to wait
// for: a statement runs in the turn that starts it, on the shard's thread, and
// returns its rows (or its error) before the handler does. In practice:
//
//   - A statement that takes long, or waits for a lock, stops every isolate on the
//     shard for as long as it runs. Request.Timeout and Cancel cannot interrupt a
//     statement that is running; they act between the batches of a result being
//     streamed (and Request.Timeout also while the consumer is slow to Continue).
//     Config.Shard names a shard to give it, if the database is not small and fast.
//   - Connections of one pool therefore run one after the other on one thread, and
//     SQLite allows one writer at a time anyway. A connection that waits for a lock
//     held by another connection of the same pool would wait for ever, because the
//     holder cannot run while the waiter does. So the default is one connection
//     (Config.MaxOpenConns), and busy_timeout is 0 unless Config.Params sets it:
//     a locked database is an error ("SQLITE_BUSY") at once. Use
//     Params{"journal_mode": "WAL"} with several connections, so that readers and the
//     writer do not lock one another.
//   - Rows are streamed as for PostgreSQL: a batch at a time, the next one read when
//     the consumer says Continue. Until then the statement stays open, and with it
//     the read transaction SQLite gives it.
//
// Values are sent as text with PostgreSQL type OIDs, so Scan works as it does there.
// The OID comes from the column's declared type (INTEGER: OIDInt8, REAL: OIDFloat8,
// TEXT: OIDText, BLOB: OIDBytea, BOOLEAN: OIDBool, DATE: OIDDate, DATETIME and
// TIMESTAMP: OIDTimestamptz, NUMERIC: OIDNumeric) or, for a column with none (an
// expression, a view), from the first value that is not NULL. SQLite lets a column
// hold any type: a value that does not fit makes the column text.
//
// Placeholders are $1, $2, ... as for PostgreSQL (rewritten to SQLite's ?1, ?2),
// or SQLite's own ?, ?NNN. Arguments keep their Go type: integers, floats, bools,
// strings, []byte and time.Time are bound as what they are, not as text.

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rm4n0s/gina"
)

// sqliteConn is what the driver's connection offers that this file uses.
type sqliteConn interface {
	driver.Conn
	driver.ExecerContext
	driver.QueryerContext
	AutoCommit() bool
}

var bgctx = context.Background() // never done: the driver then starts no goroutine

func (c *Config) sqliteDefaults() error {
	if c.Path == "" {
		return errors.New("sql: Config.Path is required for DriverSQLite")
	}
	for k, v := range c.Params {
		if !pragmaName(k) || !pragmaValue(v) {
			return errors.New("sql: Config.Params " + strconv.Quote(k) + ": a SQLite PRAGMA takes a name of letters and underscores and a value of letters, digits and _.-")
		}
	}
	if privateMemory(c.Path) {
		switch {
		case c.MaxOpenConns > 1:
			return errors.New("sql: each connection to " + strconv.Quote(c.Path) + " would get a database of its own: use MaxOpenConns 1, or a shared in-memory database (\"file:name?mode=memory&cache=shared\")")
		case c.MaxIdleConns < 0 || c.ConnMaxLifetime > 0 || c.ConnMaxIdleTime > 0:
			return errors.New("sql: an in-memory database lives as long as its connection: Config can neither close it when idle nor after a lifetime")
		}
	}
	return nil
}

func pragmaName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

func pragmaValue(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != '_' && r != '.' && r != '-' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// privateMemory reports whether every connection to path would have its own,
// empty, in-memory database.
func privateMemory(path string) bool {
	mem := strings.HasPrefix(path, ":memory:") || strings.HasPrefix(path, "file::memory:") || strings.Contains(path, "mode=memory")
	return mem && !strings.Contains(path, "cache=shared")
}

// ---- the isolate ----

type sactive struct {
	id        uint32
	op        opCode
	replyTo   gina.Handle
	batchRows int
	verb      string // INSERT, SELECT, ...

	rows  driver.Rows
	cols  []column
	dest  []driver.Value
	cell  []byte
	pend  []byte // the row just read; it goes in the next batch when this one is full
	total int64  // rows read

	rowbuf  []byte
	nrows   int
	hasPend bool

	deadline uint64 // Request.Timeout; 0 = none
}

type sconn struct {
	pool gina.Handle
	db   sqliteConn

	opened, ready bool
	born          uint64
	idleSince     uint64
	lifeExpired   bool

	// the lease
	leased    bool
	owner     gina.Handle
	tx        bool
	txid      uint32
	txUntil   uint64
	queryOnly bool // PRAGMA query_only is on, for a read-only transaction
	rq        *sactive

	// streaming
	blocked     bool
	streamUntil uint64
	held        []byte
	heldTo      gina.Handle
	heldID      uint32
	heldThen    thenKind
	heldTries   int

	timerLife, timerIdle, timerQuery, timerStream, timerTx, timerRetry gina.TimerID
}

func (d *DB) sqliteHandler(c *sconn, g *gina.Ctx, m *gina.Message) gina.Effect {
	if c.step(d, g, m) {
		c.closeDB()
		return gina.Done()
	}
	return gina.WaitMessage()
}

// step handles one message. It returns true when the isolate is finished.
func (c *sconn) step(d *DB, g *gina.Ctx, m *gina.Message) bool {
	switch m.Tag {
	case tagOpen:
		if !c.opened {
			c.opened, c.pool = true, m.Source
			return c.open(d, g)
		}
	case tagRun:
		return c.onRun(d, g, m)
	case TagTx:
		return c.onTx(d, g, m)
	case TagCancel:
		if c.mine(m.Source) && c.rq != nil && c.rq.id == m.Correlation {
			return c.abort(d, g, ClientCanceled, "canceled")
		}
	case tagCancelRun:
		if b := g.Data(); len(b) == 8 && c.rq != nil && c.rq.id == m.Correlation && uint64(c.owner) == be64(b) {
			return c.abort(d, g, ClientCanceled, "canceled")
		}
	case TagContinue:
		if c.blocked && c.rq != nil && c.rq.id == m.Correlation && c.mine(m.Source) {
			c.blocked = false
			g.CancelTimer(c.timerStream)
			return c.pump(d, g)
		}
	case tagClose, gina.TagShutdown:
		return true
	case tagTimerLife:
		if g.Now() >= c.born+uint64(d.cfg.ConnMaxLifetime) {
			c.lifeExpired = true
			if c.ready && !c.leased {
				return c.retire(g)
			}
		}
	case tagTimerIdle:
		if c.ready && !c.leased && d.cfg.ConnMaxIdleTime > 0 && g.Now() >= c.idleSince+uint64(d.cfg.ConnMaxIdleTime) {
			return c.retire(g)
		}
	case tagTimerQuery:
		if rq := c.rq; rq != nil && rq.deadline != 0 && g.Now() >= rq.deadline {
			return c.abort(d, g, ClientTimeout, "the request timed out")
		}
	case tagTimerStream:
		if c.blocked && g.Now() >= c.streamUntil {
			return c.abort(d, g, ClientTimeout, "nobody asked for the rest of the result in time")
		}
	case tagTimerTx:
		if c.tx && c.leased && c.rq == nil && c.held == nil && g.Now() >= c.txUntil {
			c.tx = false
			return c.finish(d, g)
		}
	case tagTimerRetry:
		return c.redeliver(d, g)
	}
	return false
}

func (c *sconn) mine(src gina.Handle) bool {
	if !c.leased {
		return false
	}
	return src == c.owner || (c.rq != nil && src == c.rq.replyTo)
}

func (c *sconn) closeDB() {
	if c.rq != nil {
		c.closeRows(c.rq)
	}
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}
}

// ---- opening ----

func (c *sconn) open(d *DB, g *gina.Ctx) bool {
	cfg := &d.cfg
	db, err := sqliteOpen(cfg.Path)
	if err != nil {
		return c.failOpen(g, err)
	}
	c.db = db
	pragmas := map[string]string{}
	if _, ok := cfg.Params["busy_timeout"]; !ok && !strings.Contains(cfg.Path, "_busy_timeout") {
		pragmas["busy_timeout"] = "0" // see the top of this file
	}
	for k, v := range cfg.Params {
		pragmas[k] = v
	}
	keys := make([]string, 0, len(pragmas))
	for k := range pragmas {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := c.exec("PRAGMA " + k + " = " + pragmas[k]); err != nil {
			return c.failOpen(g, err)
		}
	}
	c.ready, c.born = true, g.Now()
	if cfg.ConnMaxLifetime > 0 {
		c.timerLife = g.RegisterTimer(cfg.ConnMaxLifetime, tagTimerLife)
	}
	if g.SendRaw(c.pool, tagReady, nil) != gina.SendOK {
		return true
	}
	c.armIdle(d, g)
	return false
}

func (c *sconn) failOpen(g *gina.Ctx, err error) bool {
	e := sqliteErr(err)
	e.Client = ClientConnect
	g.SendRaw(c.pool, tagFailed, encodeError(e))
	return true
}

func (c *sconn) exec(sql string) error {
	_, err := c.db.ExecContext(bgctx, sql, nil)
	return err
}

func (c *sconn) armIdle(d *DB, g *gina.Ctx) {
	c.idleSince = g.Now()
	if d.cfg.ConnMaxIdleTime > 0 {
		c.timerIdle = g.RegisterTimer(d.cfg.ConnMaxIdleTime, tagTimerIdle)
	}
}

// retire asks the pool whether this idle connection may go; the pool answers
// tagClose unless it has just given it work. A pool that no longer exists cannot
// answer: the connection is an orphan, so it goes.
func (c *sconn) retire(g *gina.Ctx) bool {
	return g.SendRaw(c.pool, tagRetire, nil) == gina.SendStaleHandle
}

// ---- requests ----

func (c *sconn) onRun(d *DB, g *gina.Ctx, m *gina.Message) bool {
	b := g.Data()
	if !c.ready || c.leased || len(b) < 8 {
		return false
	}
	q, err := parseRequest(b[8:])
	owner := gina.Handle(be64(b))
	if q.replyTo == 0 {
		q.replyTo = owner
	}
	c.leased, c.owner = true, owner
	g.CancelTimer(c.timerIdle)
	if err != nil || (q.op != opQuery && q.op != opExec && q.op != opBegin) {
		reply(g, q.replyTo, m.Correlation, encodeError(clientErr(ClientBadRequest, "malformed request")))
		return c.release(d, g)
	}
	return c.start(d, g, &q)
}

func (c *sconn) onTx(d *DB, g *gina.Ctx, m *gina.Message) bool {
	q, err := parseRequest(g.Data())
	if err != nil {
		reply(g, m.Source, m.Correlation, encodeError(clientErr(ClientBadRequest, "malformed request")))
		return false
	}
	if !c.leased || !c.tx || q.txid != c.txid || m.Source != c.owner {
		reply(g, m.Source, q.id, encodeError(clientErr(ClientTxDone, "the transaction has ended, or is not yours")))
		return false
	}
	if c.rq != nil || c.held != nil {
		reply(g, m.Source, q.id, encodeError(clientErr(ClientBadRequest, "another request of this transaction is still running")))
		return false
	}
	if q.replyTo == 0 {
		q.replyTo = m.Source
	}
	g.CancelTimer(c.timerTx)
	return c.start(d, g, &q)
}

// start runs a request. All of it happens now, except what a streamed result
// leaves for later.
func (c *sconn) start(d *DB, g *gina.Ctx, q *wireRequest) bool {
	rq := &sactive{id: q.id, op: q.op, replyTo: q.replyTo, batchRows: q.batchRows}
	if rq.batchRows <= 0 {
		rq.batchRows = d.cfg.BatchRows
	}
	if q.timeout > 0 {
		rq.deadline = g.Now() + uint64(q.timeout)
		c.timerQuery = g.RegisterTimer(q.timeout, tagTimerQuery)
	}
	c.rq = rq
	switch q.op {
	case opBegin:
		return c.begin(d, g, q)
	case opCommit:
		return c.endTx(d, g, "COMMIT")
	case opRollback:
		return c.endTx(d, g, "ROLLBACK")
	}
	rq.verb = sqlVerb(q.sql)
	args, err := sqliteArgs(q.params)
	if err != nil {
		return c.endWith(d, g, encodeError(clientErr(ClientBadRequest, err.Error())))
	}
	sql := sqliteSQL(q.sql)
	if q.op == opExec {
		res, err := c.db.ExecContext(bgctx, sql, args)
		if err != nil {
			return c.endWith(d, g, encodeError(sqliteErr(err)))
		}
		var n int64
		if dmlVerb(rq.verb) {
			n, _ = res.RowsAffected()
		}
		return c.endWith(d, g, encodeDone(sqliteTag(rq.verb, n), n))
	}
	rows, err := c.db.QueryContext(bgctx, sql, args)
	if err != nil {
		return c.endWith(d, g, encodeError(sqliteErr(err)))
	}
	rq.rows = rows
	names := rows.Columns()
	rq.cols = make([]column, len(names))
	rq.dest = make([]driver.Value, len(names))
	tn, _ := rows.(interface{ ColumnTypeDatabaseTypeName(int) string })
	for i, name := range names {
		rq.cols[i].name = name
		if tn != nil {
			rq.cols[i].oid = declaredOID(tn.ColumnTypeDatabaseTypeName(i))
		}
	}
	return c.pump(d, g)
}

func (c *sconn) begin(d *DB, g *gina.Ctx, q *wireRequest) bool {
	// SQLite transactions are serializable whatever is asked for (q.isolation).
	if err := c.exec("BEGIN"); err != nil {
		return c.endWith(d, g, encodeError(sqliteErr(err)))
	}
	if q.readOnly {
		if err := c.exec("PRAGMA query_only = 1"); err != nil {
			c.exec("ROLLBACK")
			return c.endWith(d, g, encodeError(sqliteErr(err)))
		}
		c.queryOnly = true
	}
	c.tx = true
	c.txid++
	return c.endWith(d, g, encodeTx(c.txid))
}

// endTx commits or rolls back. A COMMIT that fails (the database is locked) leaves
// the transaction open, as SQLite does: finish sees that and keeps the lease.
func (c *sconn) endTx(d *DB, g *gina.Ctx, sql string) bool {
	if err := c.exec(sql); err != nil {
		return c.endWith(d, g, encodeError(sqliteErr(err)))
	}
	c.tx = false
	return c.endWith(d, g, encodeDone(sql, 0))
}

// ---- results ----

// pump reads rows into the current batch. A full batch is sent and the connection
// waits for Continue; the end of the result ends the request.
func (c *sconn) pump(d *DB, g *gina.Ctx) bool {
	rq := c.rq
	if rq.hasPend {
		rq.rowbuf = append(rq.rowbuf, rq.pend...)
		rq.nrows++
		rq.hasPend = false
	}
	for {
		err := rq.rows.Next(rq.dest)
		if err == io.EOF {
			return c.complete(d, g)
		}
		if err != nil {
			return c.endWith(d, g, encodeError(sqliteErr(err)))
		}
		rq.total++
		if rq.total&63 == 0 && rq.deadline != 0 && g.Now() >= rq.deadline {
			return c.abort(d, g, ClientTimeout, "the request timed out")
		}
		rq.readRow()
		// A full batch is sent when the next row shows that the result goes on, so a
		// result that is an exact multiple of the batch size has no empty batch at its end.
		if rq.nrows >= rq.batchRows || len(rq.rowbuf) >= batchBytes {
			rq.hasPend = true
			return c.sendBatch(d, g)
		}
		rq.rowbuf = append(rq.rowbuf, rq.pend...)
		rq.nrows++
	}
}

func (c *sconn) sendBatch(d *DB, g *gina.Ctx) bool {
	rq := c.rq
	payload := encodeRows(rq.colsEncoded(), rq.nrows, rq.rowbuf, true, "", 0)
	rq.rowbuf, rq.nrows = rq.rowbuf[:0], 0
	if len(payload) > d.maxMsg {
		return c.abort(d, g, ClientProtocol, "a row is larger than the system's MaxMessageBytes")
	}
	switch c.deliver(d, g, rq.replyTo, rq.id, payload, thenBlock) {
	case delivered:
		c.block(d, g)
	case gone:
		return c.abort(d, g, ClientCanceled, "the caller is gone")
	}
	return false
}

func (c *sconn) block(d *DB, g *gina.Ctx) {
	c.blocked = true
	c.streamUntil = g.Now() + uint64(d.cfg.StreamTimeout)
	c.timerStream = g.RegisterTimer(d.cfg.StreamTimeout, tagTimerStream)
}

// complete ends a query whose rows have all been read.
func (c *sconn) complete(d *DB, g *gina.Ctx) bool {
	rq := c.rq
	c.closeRows(rq) // a statement is done only when it is reset
	affected := rq.total
	if dmlVerb(rq.verb) {
		affected = c.changes()
	} else if len(rq.cols) == 0 {
		affected = 0
	}
	tag := sqliteTag(rq.verb, affected)
	if len(rq.cols) > 0 && !dmlVerb(rq.verb) {
		tag = "SELECT " + strconv.FormatInt(affected, 10)
	}
	var payload []byte
	if len(rq.cols) == 0 {
		payload = encodeDone(tag, affected)
	} else if payload = encodeRows(rq.colsEncoded(), rq.nrows, rq.rowbuf, false, tag, affected); len(payload) > d.maxMsg {
		payload = encodeError(clientErr(ClientProtocol, "the last rows are larger than the system's MaxMessageBytes"))
	}
	return c.endWith(d, g, payload)
}

// changes is the row count of the statement that just finished.
func (c *sconn) changes() int64 {
	rows, err := c.db.QueryContext(bgctx, "SELECT changes()", nil)
	if err != nil {
		return 0
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if rows.Next(dest) == nil {
		if n, ok := dest[0].(int64); ok {
			return n
		}
	}
	return 0
}

func (c *sconn) closeRows(rq *sactive) {
	if rq.rows != nil {
		rq.rows.Close()
		rq.rows = nil
	}
}

// abort ends the request in progress with an error of the given kind.
func (c *sconn) abort(d *DB, g *gina.Ctx, why ClientError, msg string) bool {
	if c.rq == nil {
		return false
	}
	if c.held != nil { // a batch nobody took: the error replaces it
		c.held = nil
		g.CancelTimer(c.timerRetry)
	}
	if c.blocked {
		c.blocked = false
		g.CancelTimer(c.timerStream)
	}
	return c.endWith(d, g, encodeError(clientErr(why, msg)))
}

// endWith sends the final reply of the request and, once it is delivered, ends the
// lease unless a transaction goes on.
func (c *sconn) endWith(d *DB, g *gina.Ctx, payload []byte) bool {
	rq := c.rq
	g.CancelTimer(c.timerQuery)
	c.closeRows(rq)
	c.rq = nil
	if c.deliver(d, g, rq.replyTo, rq.id, payload, thenFinish) == held {
		return false
	}
	return c.finish(d, g)
}

// deliver sends a reply. A mailbox or ring that is full is not the end of the
// world for a reply the caller is waiting for: keep it and try again shortly.
func (c *sconn) deliver(d *DB, g *gina.Ctx, to gina.Handle, id uint32, payload []byte, then thenKind) deliverResult {
	switch g.SendCorr(to, TagReply, id, payload) {
	case gina.SendOK:
		return delivered
	case gina.SendMailboxFull, gina.SendRingFull, gina.SendPoolExhausted:
		if c.held == nil {
			d.redelivered.Add(1)
		}
		c.held, c.heldTo, c.heldID, c.heldThen, c.heldTries = payload, to, id, then, 0
		c.timerRetry = g.RegisterTimer(redeliverGap, tagTimerRetry)
		return held
	}
	return gone
}

func (c *sconn) redeliver(d *DB, g *gina.Ctx) bool {
	if c.held == nil {
		return false
	}
	c.heldTries++
	res := gone
	if c.heldTries < redeliverMax {
		res = c.deliver(d, g, c.heldTo, c.heldID, c.held, c.heldThen)
	}
	if res == held {
		return false
	}
	then := c.heldThen
	c.held = nil
	switch then {
	case thenBlock:
		if res == delivered {
			c.block(d, g)
			return false
		}
		return c.abort(d, g, ClientCanceled, "the caller is gone")
	case thenFinish:
		return c.finish(d, g)
	}
	return false
}

// finish ends the lease, unless it is a transaction that goes on.
func (c *sconn) finish(d *DB, g *gina.Ctx) bool {
	if c.tx && c.db.AutoCommit() {
		c.tx = false // a COMMIT or ROLLBACK sent as a statement
	}
	if c.tx {
		if d.cfg.TxTimeout > 0 {
			c.txUntil = g.Now() + uint64(d.cfg.TxTimeout)
			c.timerTx = g.RegisterTimer(d.cfg.TxTimeout, tagTimerTx)
		}
		return false
	}
	// Undo what a statement left open before the next user gets the connection.
	if !c.db.AutoCommit() {
		c.exec("ROLLBACK")
	}
	if c.queryOnly {
		c.exec("PRAGMA query_only = 0")
		c.queryOnly = false
	}
	if !c.db.AutoCommit() {
		g.SendRaw(c.pool, tagFree, []byte{1, 0}) // it cannot be cleaned: drop it
		return true
	}
	return c.release(d, g)
}

// release gives the connection back to the pool.
func (c *sconn) release(d *DB, g *gina.Ctx) bool {
	c.leased, c.owner, c.tx, c.rq, c.blocked = false, 0, false, nil, false
	g.CancelTimer(c.timerTx)
	if c.lifeExpired {
		g.SendRaw(c.pool, tagFree, []byte{1, 0})
		return true
	}
	if g.SendRaw(c.pool, tagFree, []byte{0, 0}) != gina.SendOK {
		return true
	}
	c.armIdle(d, g)
	return false
}

// ---- rows as text ----

// readRow turns the row in dest into the wire form, in rq.pend, and settles the
// type of each column: a value that does not fit it makes the column wider.
func (rq *sactive) readRow() {
	b := rq.pend[:0]
	for i, v := range rq.dest {
		if v == nil {
			b = append(b, 0)
			continue
		}
		col := &rq.cols[i]
		cell := rq.cell[:0]
		switch x := v.(type) {
		case int64:
			col.oid = widen(col.oid, OIDInt8)
			cell = strconv.AppendInt(cell, x, 10)
		case float64:
			col.oid = widen(col.oid, OIDFloat8)
			cell = appendFloat(cell, x, 64)
		case string:
			col.oid = widen(col.oid, OIDText)
			cell = append(cell, x...)
		case bool:
			col.oid = widen(col.oid, OIDBool)
			if x {
				cell = append(cell, 't')
			} else {
				cell = append(cell, 'f')
			}
		case time.Time:
			col.oid = widen(col.oid, OIDTimestamptz)
			if col.oid == OIDDate {
				cell = x.AppendFormat(cell, "2006-01-02")
			} else {
				cell = x.AppendFormat(cell, "2006-01-02 15:04:05.999999999Z07:00")
			}
		case []byte:
			col.oid = widen(col.oid, OIDBytea)
			if col.oid == OIDBytea {
				cell = append(cell, `\x`...)
				cell = hex.AppendEncode(cell, x)
			} else {
				cell = append(cell, x...)
			}
		default:
			col.oid = OIDText
			cell = fmt.Append(cell, x)
		}
		rq.cell = cell
		b = appendLP1(b, cell)
	}
	rq.pend = b
}

// widen is the type of a column that was have and has now met a value of type got.
func widen(have, got uint32) uint32 {
	switch {
	case have == 0 || have == got:
		return got
	case have == OIDNumeric && (got == OIDInt8 || got == OIDFloat8),
		have == OIDBool && got == OIDInt8,
		have == OIDDate && got == OIDTimestamptz:
		return have
	case have == OIDInt8 && got == OIDFloat8, have == OIDFloat8 && got == OIDInt8:
		return OIDFloat8
	}
	return OIDText
}

func (rq *sactive) colsEncoded() []byte {
	cols := slices.Clone(rq.cols)
	for i := range cols {
		if cols[i].oid == 0 {
			cols[i].oid = OIDText
		}
	}
	return encodeColumns(cols)
}

// declaredOID maps a declared column type to a PostgreSQL type by SQLite's affinity
// rules, plus the names the driver turns into bool and time.Time. 0 means unknown.
func declaredOID(decl string) uint32 {
	t := strings.ToLower(strings.TrimSpace(decl))
	switch {
	case t == "":
		return 0
	case t == "date":
		return OIDDate
	case t == "datetime" || t == "timestamp":
		return OIDTimestamptz
	case strings.Contains(t, "bool"):
		return OIDBool
	case strings.Contains(t, "int"):
		return OIDInt8
	case strings.Contains(t, "char"), strings.Contains(t, "clob"), strings.Contains(t, "text"):
		return OIDText
	case strings.Contains(t, "blob"):
		return OIDBytea
	case strings.Contains(t, "real"), strings.Contains(t, "floa"), strings.Contains(t, "doub"):
		return OIDFloat8
	case strings.Contains(t, "numeric"), strings.Contains(t, "decimal"):
		return OIDNumeric
	}
	return 0
}

// ---- statements ----

// sqliteArgs gives each argument back its Go type.
func sqliteArgs(params []param) ([]driver.NamedValue, error) {
	if len(params) == 0 {
		return nil, nil
	}
	args := make([]driver.NamedValue, len(params))
	for i, p := range params {
		args[i].Ordinal = i + 1
		var err error
		switch {
		case p.null:
		case p.kind == kindBinary:
			args[i].Value = slices.Clone(p.data)
		case p.kind == kindInt:
			args[i].Value, err = strconv.ParseInt(string(p.data), 10, 64)
		case p.kind == kindFloat:
			args[i].Value, err = strconv.ParseFloat(string(p.data), 64)
		case p.kind == kindBool:
			args[i].Value = string(p.data) == "true"
		case p.kind == kindTime:
			args[i].Value, err = time.Parse(time.RFC3339Nano, string(p.data))
		default:
			args[i].Value = string(p.data)
		}
		if err != nil {
			return nil, errors.New("argument " + itoa(i+1) + ": " + err.Error())
		}
	}
	return args, nil
}

// sqliteSQL rewrites PostgreSQL's $1 placeholders to SQLite's ?1, leaving strings,
// quoted names, comments and names such as a$1 alone.
func sqliteSQL(sql string) string {
	if strings.IndexByte(sql, '$') < 0 {
		return sql
	}
	b := make([]byte, 0, len(sql))
	for i := 0; i < len(sql); {
		ch := sql[i]
		switch {
		case ch == '\'' || ch == '"' || ch == '`' || ch == '[':
			end := ch
			if ch == '[' {
				end = ']'
			}
			j := i + 1
			for j < len(sql) {
				if sql[j] == end {
					if end != ']' && j+1 < len(sql) && sql[j+1] == end { // '' inside a string
						j += 2
						continue
					}
					break
				}
				j++
			}
			j = min(j+1, len(sql))
			b = append(b, sql[i:j]...)
			i = j
		case ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				j = len(sql) - i
			}
			b = append(b, sql[i:i+j]...)
			i += j
		case ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				j = len(sql) - i - 2
			} else {
				j += 2
			}
			j = min(i+2+j, len(sql))
			b = append(b, sql[i:j]...)
			i = j
		case ch == '$' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9' && (i == 0 || !identChar(sql[i-1])):
			b = append(b, '?')
			i++
		default:
			b = append(b, ch)
			i++
		}
	}
	return string(b)
}

func identChar(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// sqlVerb is the first word of a statement, upper case, with a REPLACE counted as
// the INSERT it is.
func sqlVerb(sql string) string {
	i := 0
	for i < len(sql) {
		switch {
		case sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r' || sql[i] == '(' || sql[i] == ';':
			i++
		case strings.HasPrefix(sql[i:], "--"):
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				return ""
			}
			i += j
		case strings.HasPrefix(sql[i:], "/*"):
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				return ""
			}
			i += j + 4
		default:
			j := i
			for j < len(sql) && (sql[j] >= 'a' && sql[j] <= 'z' || sql[j] >= 'A' && sql[j] <= 'Z') {
				j++
			}
			if v := strings.ToUpper(sql[i:j]); v != "REPLACE" {
				return v
			}
			return "INSERT"
		}
	}
	return ""
}

func dmlVerb(v string) bool { return v == "INSERT" || v == "UPDATE" || v == "DELETE" }

// sqliteTag is the command tag PostgreSQL would give the statement.
func sqliteTag(verb string, n int64) string {
	switch verb {
	case "INSERT":
		return "INSERT 0 " + strconv.FormatInt(n, 10)
	case "UPDATE", "DELETE":
		return verb + " " + strconv.FormatInt(n, 10)
	}
	return verb
}

// ---- errors ----

var sqliteNames = map[int]string{
	1: "SQLITE_ERROR", 2: "SQLITE_INTERNAL", 3: "SQLITE_PERM", 4: "SQLITE_ABORT", 5: "SQLITE_BUSY", 6: "SQLITE_LOCKED", 7: "SQLITE_NOMEM",
	8: "SQLITE_READONLY", 9: "SQLITE_INTERRUPT", 10: "SQLITE_IOERR", 11: "SQLITE_CORRUPT", 12: "SQLITE_NOTFOUND", 13: "SQLITE_FULL",
	14: "SQLITE_CANTOPEN", 15: "SQLITE_PROTOCOL", 16: "SQLITE_EMPTY", 17: "SQLITE_SCHEMA", 18: "SQLITE_TOOBIG", 19: "SQLITE_CONSTRAINT",
	20: "SQLITE_MISMATCH", 21: "SQLITE_MISUSE", 22: "SQLITE_NOLFS", 23: "SQLITE_AUTH", 24: "SQLITE_FORMAT", 25: "SQLITE_RANGE",
	26: "SQLITE_NOTADB", 27: "SQLITE_NOTICE", 28: "SQLITE_WARNING",
	// extended codes worth telling apart
	261: "SQLITE_BUSY_RECOVERY", 517: "SQLITE_BUSY_SNAPSHOT", 773: "SQLITE_BUSY_TIMEOUT",
	275: "SQLITE_CONSTRAINT_CHECK", 787: "SQLITE_CONSTRAINT_FOREIGNKEY", 1299: "SQLITE_CONSTRAINT_NOTNULL",
	1555: "SQLITE_CONSTRAINT_PRIMARYKEY", 1811: "SQLITE_CONSTRAINT_TRIGGER", 2067: "SQLITE_CONSTRAINT_UNIQUE",
	2579: "SQLITE_CONSTRAINT_ROWID",
}

// sqliteErr is the Error for a failure of the driver. Code is the name of
// SQLite's result code, the extended one where it says more ("SQLITE_BUSY",
// "SQLITE_CONSTRAINT_UNIQUE").
func sqliteErr(err error) *Error {
	e := &Error{Severity: "ERROR", Message: err.Error()}
	if primary, extended, ok := sqliteCodes(err); ok {
		if e.Code = sqliteNames[extended]; e.Code == "" {
			if e.Code = sqliteNames[primary]; e.Code == "" {
				e.Code = "SQLITE_" + strconv.Itoa(extended)
			}
		}
	}
	// "UNIQUE constraint failed: users.email, users.org"
	if kind, rest, ok := strings.Cut(e.Message, " constraint failed: "); ok {
		if kind == "CHECK" {
			e.Constraint = rest
		} else if t, col, ok := strings.Cut(strings.Split(rest, ", ")[0], "."); ok {
			e.Table, e.Column = t, col
		}
	}
	return e
}
