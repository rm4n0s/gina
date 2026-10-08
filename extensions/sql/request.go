package sql

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/rm4n0s/gina"
)

// Message tags. The first group is the API: requests go to the pool (or, inside a
// transaction, to the connection that holds it) and answers come back as TagReply.
const (
	// TagRequest is delivered to the pool isolate; use DB.Query, DB.Exec and
	// DB.Begin rather than building it.
	TagRequest gina.Tag = gina.TagUserBase + 0x50
	// TagTx is delivered to the connection holding a transaction; use the Tx methods.
	TagTx gina.Tag = gina.TagUserBase + 0x51
	// TagCancel asks the pool or the connection to cancel the request whose ID is the
	// message's Correlation; use DB.Cancel, Tx.Cancel or Reply.Cancel.
	TagCancel gina.Tag = gina.TagUserBase + 0x52
	// TagReply is delivered to the isolate that sent a request (or to its ReplyTo):
	// the answer to it, possibly in several messages. The message's Correlation is
	// the Request.ID. Read it with Decode.
	TagReply gina.Tag = gina.TagUserBase + 0x53
	// TagContinue asks a connection that is streaming a result for the next batch;
	// use Reply.Continue.
	TagContinue gina.Tag = gina.TagUserBase + 0x54
)

// Between the pool, its connections and the cancellers.
const (
	tagOpen      gina.Tag = gina.TagUserBase + 0x60 // pool -> conn: start connecting
	tagRun       gina.Tag = gina.TagUserBase + 0x61 // pool -> conn: owner (8 bytes) and a request
	tagReady     gina.Tag = gina.TagUserBase + 0x62 // conn -> pool: logged in, free
	tagFree      gina.Tag = gina.TagUserBase + 0x63 // conn -> pool: finished with its lease; payload 1 = the conn is going away
	tagFailed    gina.Tag = gina.TagUserBase + 0x64 // conn -> pool: could not connect; an encoded error
	tagRetire    gina.Tag = gina.TagUserBase + 0x65 // conn -> pool: idle too long, may I go?
	tagClose     gina.Tag = gina.TagUserBase + 0x66 // pool -> conn: close
	tagCancelRun gina.Tag = gina.TagUserBase + 0x67 // pool -> conn: cancel request (Correlation) of owner (8 bytes)
	tagCancelGo  gina.Tag = gina.TagUserBase + 0x68 // conn -> canceller: the packet to send
	// Timers. Each handler checks its deadline, so a timer message that was already
	// queued when its subject ended does no harm.
	tagTimerLife   gina.Tag = gina.TagUserBase + 0x70 // conn: lifetime
	tagTimerIdle   gina.Tag = gina.TagUserBase + 0x71 // conn: idle
	tagTimerQuery  gina.Tag = gina.TagUserBase + 0x72 // conn: Request.Timeout
	tagTimerStream gina.Tag = gina.TagUserBase + 0x73 // conn: nobody asked for the next batch
	tagTimerTx     gina.Tag = gina.TagUserBase + 0x74 // conn: transaction idle
	tagTimerRetry  gina.Tag = gina.TagUserBase + 0x75 // conn: try delivering again
	tagTimerQueue  gina.Tag = gina.TagUserBase + 0x76 // pool: expire queued requests
)

// Isolation is a transaction isolation level for Begin.
type Isolation uint8

const (
	IsolationDefault Isolation = iota // the server's default (read committed)
	IsolationReadCommitted
	IsolationRepeatableRead
	IsolationSerializable
)

var isolationNames = [...]string{"", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

// Request is one statement, or the start or end of a transaction.
type Request struct {
	// ID comes back in the Correlation of every reply. Pick numbers that are unique
	// among the requests this isolate has in flight; Cancel names a request by it.
	ID uint32
	// SQL with $1, $2, ... placeholders for Args. Ignored by Begin, Commit and Rollback.
	SQL  string
	Args []any
	// Timeout limits the time the server spends on the request, from when a
	// connection starts it (time spent waiting for a free connection is limited by
	// Config.QueueTimeout). When it passes the connection asks the server to cancel
	// the statement and the request ends with ErrTimeout. Zero means no limit.
	Timeout time.Duration
	// BatchRows is how many rows go in one ReplyRows (default Config.BatchRows; a
	// batch is also closed at about 64 KiB). Query only.
	BatchRows int
	// ReplyTo receives the replies; the zero Handle means the sender. Give a
	// long-lived isolate here when the sender is short-lived.
	ReplyTo gina.Handle

	// Begin only.
	Isolation Isolation
	ReadOnly  bool
}

type opCode uint8

const (
	opQuery opCode = iota + 1
	opExec
	opBegin
	opCommit
	opRollback
)

const (
	wireVersion   = 1
	wireHeaderLen = 27
	maxTimeoutMs  = 1<<32 - 1
)

// marshal builds the request message: a fixed header, then for statements the SQL
// and the arguments.
//
//	version u8, op u8, id u32, replyTo u64, txid u32, timeout ms u32, batchRows u32, flags u8
//	sql lp, nargs uvarint, [kind u8 (0 NULL, 1 text, 2 binary, 3 int, 4 float, 5 bool, 6 time), lp unless NULL]...
//
// Kinds 3 to 6 are text for PostgreSQL, which infers types from the statement;
// they tell SQLite which type to bind.
func (r *Request) marshal(op opCode, txid uint32, replyTo gina.Handle) ([]byte, error) {
	b := make([]byte, 0, wireHeaderLen+len(r.SQL)+16+16*len(r.Args))
	b = append(b, wireVersion, byte(op))
	b = binary.BigEndian.AppendUint32(b, r.ID)
	b = binary.BigEndian.AppendUint64(b, uint64(replyTo))
	b = binary.BigEndian.AppendUint32(b, txid)
	ms := r.Timeout / time.Millisecond
	if r.Timeout > 0 && ms == 0 {
		ms = 1
	}
	if r.Timeout < 0 || ms > maxTimeoutMs {
		return nil, errors.New("Timeout out of range")
	}
	b = binary.BigEndian.AppendUint32(b, uint32(ms))
	if r.BatchRows < 0 {
		return nil, errors.New("negative BatchRows")
	}
	b = binary.BigEndian.AppendUint32(b, uint32(min(r.BatchRows, 1<<30)))
	var flags byte
	if r.ReadOnly {
		flags |= 1
	}
	if int(r.Isolation) >= len(isolationNames) {
		return nil, errors.New("unknown Isolation")
	}
	flags |= byte(r.Isolation) << 1
	b = append(b, flags)
	if op != opQuery && op != opExec {
		return b, nil
	}
	b = appendStr(b, r.SQL)
	b = binary.AppendUvarint(b, uint64(len(r.Args)))
	for i, a := range r.Args {
		p, err := encodeArg(a)
		if err != nil {
			return nil, errors.New("argument " + itoa(i+1) + ": " + err.Error())
		}
		switch {
		case p.null:
			b = append(b, 0)
		case p.format == 1:
			b = appendLP(append(b, 2), p.data)
		case p.kind >= kindInt:
			b = appendLP(append(b, byte(p.kind)+1), p.data)
		default:
			b = appendLP(append(b, 1), p.data)
		}
	}
	return b, nil
}

func itoa(i int) string {
	var buf [20]byte
	n := len(buf)
	for {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
		if i == 0 {
			break
		}
	}
	return string(buf[n:])
}

// wireRequest is a request as the pool and the connection read it. Strings and
// byte slices point into the message: use them during the turn.
type wireRequest struct {
	op        opCode
	id        uint32
	replyTo   gina.Handle
	txid      uint32
	timeout   time.Duration
	batchRows int
	readOnly  bool
	isolation Isolation
	sql       string
	params    []param
}

var errWireRequest = errors.New("sql: malformed request")

func parseRequest(b []byte) (q wireRequest, err error) {
	if len(b) < wireHeaderLen || b[0] != wireVersion {
		return q, errWireRequest
	}
	q.op = opCode(b[1])
	q.id = binary.BigEndian.Uint32(b[2:])
	q.replyTo = gina.Handle(binary.BigEndian.Uint64(b[6:]))
	q.txid = binary.BigEndian.Uint32(b[14:])
	q.timeout = time.Duration(binary.BigEndian.Uint32(b[18:])) * time.Millisecond
	q.batchRows = int(binary.BigEndian.Uint32(b[22:]))
	flags := b[26]
	q.readOnly = flags&1 != 0
	q.isolation = Isolation(flags >> 1)
	if q.op < opQuery || q.op > opRollback || int(q.isolation) >= len(isolationNames) {
		return q, errWireRequest
	}
	if q.op != opQuery && q.op != opExec {
		return q, nil
	}
	r := wreader{b: b[wireHeaderLen:]}
	q.sql = string(r.bytes())
	n := r.uvarint()
	if r.bad || n > uint64(len(r.b)) || n > 65535 {
		return q, errWireRequest
	}
	q.params = make([]param, n)
	for i := range q.params {
		switch k := r.u8(); k {
		case 0:
			q.params[i].null = true
		case 1:
			q.params[i].data = r.bytes()
		case 2:
			q.params[i].format = 1
			q.params[i].kind = kindBinary
			q.params[i].data = r.bytes()
		case 3, 4, 5, 6: // typed text: see marshal
			q.params[i].kind = paramKind(k - 1)
			q.params[i].data = r.bytes()
		default:
			return q, errWireRequest
		}
	}
	if r.bad {
		return q, errWireRequest
	}
	return q, nil
}

// ---- the API ----

func (r *Request) replyTarget(g *gina.Ctx) gina.Handle {
	if r.ReplyTo != 0 {
		return r.ReplyTo
	}
	return g.Self()
}

// send marshals r and delivers it; a request that cannot be built is answered
// with a ReplyError (ErrBadRequest) instead of being sent.
func (r *Request) send(g *gina.Ctx, to gina.Handle, tag gina.Tag, op opCode, txid uint32) gina.SendResult {
	replyTo := r.replyTarget(g)
	b, err := r.marshal(op, txid, replyTo)
	if err != nil {
		return g.SendCorr(replyTo, TagReply, r.ID, encodeError(clientErr(ClientBadRequest, err.Error())))
	}
	res := g.SendCorr(to, tag, r.ID, b)
	if res == gina.SendStaleHandle && tag == TagTx {
		// The connection that held the transaction is gone: say so, as every request is
		// answered.
		g.SendCorr(replyTo, TagReply, r.ID, encodeError(clientErr(ClientTxDone, "the transaction's connection is gone")))
		return gina.SendOK
	}
	return res
}

// Query runs a statement that returns rows. The answer is one or more ReplyRows
// (see Reply.More) ending with a final one, or a ReplyDone for a statement without
// a result set, or a ReplyError. The result says only whether Gina accepted the
// message.
func (d *DB) Query(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, d.Pool(), TagRequest, opQuery, 0)
}

// Exec runs a statement and discards any rows it returns. The answer is a
// ReplyDone (with RowsAffected) or a ReplyError. Without arguments the SQL is sent
// as a simple query and may hold several statements separated by semicolons;
// RowsAffected is then their sum.
func (d *DB) Exec(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, d.Pool(), TagRequest, opExec, 0)
}

// Begin starts a transaction on a connection of its own. The answer is a
// ReplyTx (use its Tx) or a ReplyError. The transaction belongs to the isolate that
// called Begin: only it can use the Tx. Always end it with Commit or Rollback; one
// left idle for Config.TxTimeout is rolled back.
func (d *DB) Begin(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, d.Pool(), TagRequest, opBegin, 0)
}

// Cancel asks for the request with this ID, sent by the calling isolate, to be
// cancelled: dropped from the queue if it still waits there, or cancelled on the
// server. It then ends with ErrCanceled, unless it had already finished. For
// requests inside a transaction use Tx.Cancel.
func (d *DB) Cancel(g *gina.Ctx, id uint32) gina.SendResult {
	return g.SendCorr(d.Pool(), TagCancel, id, nil)
}

// Query is DB.Query inside the transaction. One request at a time: wait for the
// reply to the last one before sending the next.
func (t Tx) Query(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, t.Conn, TagTx, opQuery, t.ID)
}

// Exec is DB.Exec inside the transaction.
func (t Tx) Exec(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, t.Conn, TagTx, opExec, t.ID)
}

// Commit ends the transaction. The answer is a ReplyDone, or a ReplyError (the
// server may refuse to commit, and a transaction that failed earlier is rolled
// back instead: that is an error too).
func (t Tx) Commit(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, t.Conn, TagTx, opCommit, t.ID)
}

// Rollback ends the transaction, undoing it.
func (t Tx) Rollback(g *gina.Ctx, r *Request) gina.SendResult {
	return r.send(g, t.Conn, TagTx, opRollback, t.ID)
}

// Cancel is DB.Cancel for a request inside the transaction.
func (t Tx) Cancel(g *gina.Ctx, id uint32) gina.SendResult {
	return g.SendCorr(t.Conn, TagCancel, id, nil)
}
