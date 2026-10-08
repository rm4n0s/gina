package sql

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/rm4n0s/gina"
)

// ClientError classifies an Error that did not come from the server.
type ClientError uint8

const (
	// ClientNone marks an Error sent by the server: Code is its SQLSTATE.
	ClientNone        ClientError = iota
	ClientOverloaded              // the pool's queue was full, or the pool is not running
	ClientConnLost                // the connection failed while the request was on it
	ClientConnect                 // a connection could not be made (network, TLS or login)
	ClientTimeout                 // Request.Timeout, Config.QueueTimeout or Config.StreamTimeout ran out
	ClientCanceled                // Cancel was called
	ClientTxDone                  // the transaction has ended (committed, rolled back, timed out) or is not yours
	ClientBadRequest              // the request cannot be sent (an argument of an unsupported type, ...)
	ClientUnsupported             // the server asked for something this client does not do (COPY, ...)
	ClientProtocol                // the server sent something the protocol does not allow
)

var clientNames = [...]string{"", "overloaded", "connection lost", "connect", "timeout", "canceled", "transaction done", "bad request", "unsupported", "protocol"}

// Error is what a failed request carries: a server error (ClientNone, with the
// fields PostgreSQL sends) or one made on this side. It implements error; use
// errors.Is with the Err* values below to test for the client kinds, and errors.As
// to read a server error's Code (a SQLSTATE such as "23505", unique violation).
type Error struct {
	Client ClientError

	Severity   string // ERROR, FATAL, ...
	Code       string // SQLSTATE, for server errors
	Message    string
	Detail     string
	Hint       string
	Position   string
	Where      string
	Schema     string
	Table      string
	Column     string
	DataType   string
	Constraint string
}

func (e *Error) Error() string {
	if e.Client != ClientNone {
		s := "sql: " + clientNames[e.Client]
		if e.Message != "" {
			s += ": " + e.Message
		}
		return s
	}
	s := "sql: " + e.Severity + ": " + e.Message
	if e.Code != "" {
		s += " (SQLSTATE " + e.Code + ")"
	}
	return s
}

// Is makes errors.Is(err, sql.ErrTimeout) and the like match by kind.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Client != ClientNone && t.Client == e.Client
}

// The client error kinds, for errors.Is.
var (
	ErrOverloaded  = &Error{Client: ClientOverloaded}
	ErrConnLost    = &Error{Client: ClientConnLost}
	ErrConnect     = &Error{Client: ClientConnect}
	ErrTimeout     = &Error{Client: ClientTimeout}
	ErrCanceled    = &Error{Client: ClientCanceled}
	ErrTxDone      = &Error{Client: ClientTxDone}
	ErrBadRequest  = &Error{Client: ClientBadRequest}
	ErrUnsupported = &Error{Client: ClientUnsupported}
	ErrProtocol    = &Error{Client: ClientProtocol}
)

func clientErr(k ClientError, msg string) *Error { return &Error{Client: k, Message: msg} }

// ReplyKind says which kind of Reply a TagReply message carries.
type ReplyKind uint8

const (
	// ReplyRows carries rows of a Query's result. If More is set the connection is
	// waiting: call Continue for the next batch, or Cancel to give up. The last
	// batch has More false and carries the command Tag.
	ReplyRows ReplyKind = iota + 1
	// ReplyDone ends a statement that returned no rows (or an Exec).
	ReplyDone
	// ReplyError ends a request that failed. Every request ends with exactly one
	// ReplyDone, ReplyError or final ReplyRows (More false), or with a ReplyTx for
	// a Begin.
	ReplyError
	// ReplyTx answers a Begin: the transaction is open and Tx names it.
	ReplyTx
)

// Tx names an open transaction: the connection that holds it and its number on
// that connection. Send its statements with Tx.Query and Tx.Exec and end it with
// Tx.Commit or Tx.Rollback. A Tx belongs to the isolate that began it.
type Tx struct {
	Conn gina.Handle
	ID   uint32
}

// Column describes a result column.
type Column struct {
	Name string
	OID  uint32 // the type's OID in pg_type; see the OID* constants
}

// Reply is a decoded TagReply message.
type Reply struct {
	// ID is the Request.ID this answers.
	ID   uint32
	Kind ReplyKind

	// Rows is this batch (ReplyRows). It reads the message's data, so use it
	// during the handler that decoded it.
	Rows Rows
	More bool

	// Tag is the command tag ("SELECT 3", "INSERT 0 1") of a ReplyDone or of the
	// last ReplyRows; RowsAffected is the row count taken from it.
	Tag          string
	RowsAffected int64

	// Err is set for ReplyError; it is an *Error.
	Err error

	// Tx is set for ReplyTx.
	Tx Tx

	from gina.Handle
}

// Continue asks the connection for the next batch of a ReplyRows with More set.
// Until it does (or Cancel, or Config.StreamTimeout) the connection stops reading
// the server, so a slow consumer slows the query instead of filling memory.
func (r *Reply) Continue(g *gina.Ctx) gina.SendResult {
	return g.SendCorr(r.from, TagContinue, r.ID, nil)
}

// Cancel stops a query whose rows are being streamed. The request still ends with
// a ReplyError (ErrCanceled), after the server has let go of the query.
func (r *Reply) Cancel(g *gina.Ctx) gina.SendResult {
	return g.SendCorr(r.from, TagCancel, r.ID, nil)
}

// ---- wire format of a reply ----
//
//	rows:  kind, flags(1 = more), ncols, [name lp, oid u32]..., nrows, rows lp, then if not more: tag lp, affected i64
//	       rows = nrows times, for each column: uvarint(len+1) (0 is NULL), bytes
//	done:  kind, tag lp, affected i64
//	error: kind, client u8, then the fields as lp strings in the order of errFields
//	tx:    kind, id u32

func appendLP(b []byte, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func errFields(e *Error) []*string {
	return []*string{&e.Severity, &e.Code, &e.Message, &e.Detail, &e.Hint, &e.Position, &e.Where, &e.Schema, &e.Table, &e.Column, &e.DataType, &e.Constraint}
}

func encodeError(e *Error) []byte {
	b := []byte{byte(ReplyError), byte(e.Client)}
	for _, f := range errFields(e) {
		b = appendStr(b, *f)
	}
	return b
}

func encodeDone(tag string, affected int64) []byte {
	b := appendStr([]byte{byte(ReplyDone)}, tag)
	return binary.BigEndian.AppendUint64(b, uint64(affected))
}

func encodeTx(id uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{byte(ReplyTx)}, id)
}

// encodeColumns is the column section of a rows reply, built once per query.
func encodeColumns(cols []column) []byte {
	b := binary.AppendUvarint(nil, uint64(len(cols)))
	for _, c := range cols {
		b = appendStr(b, c.name)
		b = binary.BigEndian.AppendUint32(b, c.oid)
	}
	return b
}

// encodeRows assembles a rows reply from the encoded columns and rows.
func encodeRows(colsEnc []byte, nrows int, rowData []byte, more bool, tag string, affected int64) []byte {
	b := make([]byte, 0, 2+len(colsEnc)+binary.MaxVarintLen32+len(rowData)+len(tag)+16)
	b = append(b, byte(ReplyRows))
	if more {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = append(b, colsEnc...)
	b = binary.AppendUvarint(b, uint64(nrows))
	b = binary.AppendUvarint(b, uint64(len(rowData)))
	b = append(b, rowData...)
	if !more {
		b = appendStr(b, tag)
		b = binary.BigEndian.AppendUint64(b, uint64(affected))
	}
	return b
}

var errReply = errors.New("sql: malformed reply")

type wreader struct {
	b   []byte
	bad bool
}

func (r *wreader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *wreader) bytes() []byte {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.b)) {
		r.bad = true
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *wreader) u8() byte {
	if len(r.b) < 1 {
		r.bad = true
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *wreader) u32() uint32 {
	if len(r.b) < 4 {
		r.bad = true
		return 0
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *wreader) u64() uint64 {
	if len(r.b) < 8 {
		r.bad = true
		return 0
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

// Decode reads the TagReply message being handled. Call it from the handler of the
// isolate that sent the request.
func Decode(g *gina.Ctx, m *gina.Message) (Reply, error) {
	if m.Tag != TagReply {
		return Reply{}, errors.New("sql: not a TagReply message")
	}
	return decodeReply(g.Data(), m.Correlation, m.Source)
}

func decodeReply(data []byte, id uint32, from gina.Handle) (Reply, error) {
	rep := Reply{ID: id, from: from}
	r := wreader{b: data}
	rep.Kind = ReplyKind(r.u8())
	switch rep.Kind {
	case ReplyRows:
		rep.More = r.u8()&1 != 0
		ncols := r.uvarint()
		if r.bad || ncols > 1<<16 {
			return rep, errReply
		}
		cols := make([]Column, ncols)
		for i := range cols {
			cols[i].Name = string(r.bytes())
			cols[i].OID = r.u32()
		}
		nrows := r.uvarint()
		data := r.bytes()
		if r.bad || (ncols > 0 && nrows > uint64(len(data))) || nrows > 1<<24 {
			return rep, errReply
		}
		rows := Rows{cols: cols, data: data, left: int(nrows), cur: make([][]byte, ncols)}
		rep.Rows = rows
		if !rep.More {
			rep.Tag = string(r.bytes())
			rep.RowsAffected = int64(r.u64())
		}
	case ReplyDone:
		rep.Tag = string(r.bytes())
		rep.RowsAffected = int64(r.u64())
	case ReplyError:
		e := &Error{Client: ClientError(r.u8())}
		for _, f := range errFields(e) {
			*f = string(r.bytes())
		}
		rep.Err = e
	case ReplyTx:
		rep.Tx = Tx{Conn: from, ID: r.u32()}
	default:
		return rep, errReply
	}
	if r.bad {
		return rep, errReply
	}
	return rep, nil
}

// Rows iterates over one batch of a result. It is a cursor, like database/sql's
// Rows: Next, then Scan.
type Rows struct {
	cols []Column
	data []byte
	left int
	cur  [][]byte // the current row; a nil entry is NULL
}

// Columns describes the result columns.
func (r *Rows) Columns() []Column { return r.cols }

// Len is how many rows of the batch have not been read yet.
func (r *Rows) Len() int { return r.left }

// Next moves to the next row of the batch and reports whether there is one.
func (r *Rows) Next() bool {
	if r.left == 0 {
		return false
	}
	r.left--
	rd := wreader{b: r.data}
	for i := range r.cur {
		n := rd.uvarint()
		if n == 0 {
			r.cur[i] = nil
			continue
		}
		r.cur[i] = rd.b[: n-1 : n-1]
		rd.b = rd.b[n-1:]
	}
	r.data = rd.b
	return true
}

// Scan copies the columns of the current row into dest, one destination per
// column. Supported destinations: *string, *[]byte, *bool, *int..*int64,
// *uint..*uint64, *float32, *float64, *time.Time, *any, a pointer to any of those
// (nil for NULL), and any value with a Scan(any) error method, such as
// database/sql's NullString and NullInt64. NULL into a plain Go value is an error.
// Values are copied, so they stay valid after the handler returns.
func (r *Rows) Scan(dest ...any) error {
	if len(dest) != len(r.cur) {
		return fmt.Errorf("sql: Scan wants %d destinations, got %d", len(r.cur), len(dest))
	}
	for i, d := range dest {
		if err := convertAssign(d, r.cur[i], r.cols[i].OID); err != nil {
			return fmt.Errorf("sql: column %d (%s): %w", i, r.cols[i].Name, err)
		}
	}
	return nil
}

// Raw returns the text of column i of the current row, nil for NULL. It points
// into the message.
func (r *Rows) Raw(i int) []byte { return r.cur[i] }

// String formats a Tx for logs.
func (t Tx) String() string { return fmt.Sprintf("tx(%d on %#x)", t.ID, uint64(t.Conn)) }

func (k ReplyKind) String() string {
	switch k {
	case ReplyRows:
		return "rows"
	case ReplyDone:
		return "done"
	case ReplyError:
		return "error"
	case ReplyTx:
		return "tx"
	}
	return "unknown"
}
