package sql

// The PostgreSQL frontend/backend protocol, version 3.0: message builders and a
// frame parser. It does no I/O; the connection isolate feeds it bytes and sends
// what it builds.
//
// Only what the extension uses is here: startup and authentication, the simple
// query protocol (BEGIN, COMMIT, scripts without parameters) and the extended
// protocol with the unnamed statement and portal (Parse, Bind, Describe, Execute,
// Sync), all with text-format results. COPY and LISTEN/NOTIFY are not implemented.

import (
	"encoding/binary"
	"errors"
	"strconv"
)

const (
	protoVersion3 = 196608   // 3.0
	sslRequest    = 80877103 // the SSLRequest "version"
	cancelRequest = 80877102
)

// wbuf builds frontend messages. begin returns a mark for end, which fills in the
// length.
type wbuf struct{ b []byte }

func (w *wbuf) begin(t byte) int {
	w.b = append(w.b, t, 0, 0, 0, 0)
	return len(w.b) - 4
}

func (w *wbuf) beginUntyped() int {
	w.b = append(w.b, 0, 0, 0, 0)
	return len(w.b) - 4
}

func (w *wbuf) end(mark int) {
	binary.BigEndian.PutUint32(w.b[mark:], uint32(len(w.b)-mark))
}

func (w *wbuf) i16(v int)      { w.b = binary.BigEndian.AppendUint16(w.b, uint16(v)) }
func (w *wbuf) i32(v int32)    { w.b = binary.BigEndian.AppendUint32(w.b, uint32(v)) }
func (w *wbuf) cstr(s string)  { w.b = append(append(w.b, s...), 0) }
func (w *wbuf) bytes(p []byte) { w.b = append(w.b, p...) }
func (w *wbuf) byte1(c byte)   { w.b = append(w.b, c) }
func (w *wbuf) reset()         { w.b = w.b[:0] }
func (w *wbuf) take() []byte   { return w.b }
func (w *wbuf) lenPrefixed(p []byte) {
	w.i32(int32(len(p)))
	w.bytes(p)
}

// startup is the StartupMessage: the protocol version and name/value pairs.
func (w *wbuf) startup(params [][2]string) {
	m := w.beginUntyped()
	w.i32(protoVersion3)
	for _, kv := range params {
		w.cstr(kv[0])
		w.cstr(kv[1])
	}
	w.byte1(0)
	w.end(m)
}

func (w *wbuf) sslRequest() {
	m := w.beginUntyped()
	w.i32(sslRequest)
	w.end(m)
}

// cancelPacket is the CancelRequest, sent on a connection of its own.
func cancelPacket(pid int32, key []byte) []byte {
	var w wbuf
	m := w.beginUntyped()
	w.i32(cancelRequest)
	w.i32(pid)
	w.bytes(key)
	w.end(m)
	return w.b
}

func (w *wbuf) password(s string) {
	m := w.begin('p')
	w.cstr(s)
	w.end(m)
}

func (w *wbuf) saslInitial(mech string, data []byte) {
	m := w.begin('p')
	w.cstr(mech)
	w.lenPrefixed(data)
	w.end(m)
}

func (w *wbuf) saslResponse(data []byte) {
	m := w.begin('p')
	w.bytes(data)
	w.end(m)
}

func (w *wbuf) query(sql string) {
	m := w.begin('Q')
	w.cstr(sql)
	w.end(m)
}

func (w *wbuf) terminate() {
	m := w.begin('X')
	w.end(m)
}

func (w *wbuf) copyFail(msg string) {
	m := w.begin('f')
	w.cstr(msg)
	w.end(m)
}

// An argument as the extended protocol carries it.
type param struct {
	format int16 // 0 text, 1 binary
	null   bool
	data   []byte
	// kind is the Go type the text came from. PostgreSQL infers the type from the
	// statement and ignores it; SQLite binds by it.
	kind paramKind
}

type paramKind uint8

const (
	kindText paramKind = iota
	kindBinary
	kindInt   // data is a decimal int64
	kindFloat // data is a float64 as appendFloat writes it
	kindBool  // data is "true" or "false"
	kindTime  // data is a time.Time as RFC 3339
)

// extended builds Parse, Bind, Describe(portal), Execute and Sync for the unnamed
// statement. The parameter types are left to the server (OID 0) and every result
// column is requested in text format.
func (w *wbuf) extended(sql string, params []param) error {
	if len(params) > 65535 {
		return errors.New("sql: too many arguments (limit 65535)")
	}
	m := w.begin('P')
	w.cstr("")
	w.cstr(sql)
	w.i16(len(params))
	for range params {
		w.i32(0)
	}
	w.end(m)

	m = w.begin('B')
	w.cstr("")
	w.cstr("")
	w.i16(len(params))
	for _, p := range params {
		w.i16(int(p.format))
	}
	w.i16(len(params))
	for _, p := range params {
		if p.null {
			w.i32(-1)
		} else {
			w.lenPrefixed(p.data)
		}
	}
	w.i16(0) // all results in text format
	w.end(m)

	m = w.begin('D')
	w.byte1('P')
	w.cstr("")
	w.end(m)

	m = w.begin('E')
	w.cstr("")
	w.i32(0)
	w.end(m)

	m = w.begin('S')
	w.end(m)
	return nil
}

// ---- backend frames ----

const maxFrame = 1 << 30 // the protocol's own limit

var errFrame = errors.New("sql: malformed message from the server")

// nextFrame cuts one message off the front of *in. ok is false until a whole
// message is there. limit bounds the size of a message the caller will hold.
func nextFrame(in *[]byte, limit int) (typ byte, body []byte, ok bool, err error) {
	b := *in
	if len(b) < 5 {
		return 0, nil, false, nil
	}
	n := int(binary.BigEndian.Uint32(b[1:]))
	if n < 4 || n > maxFrame {
		return 0, nil, false, errFrame
	}
	if n-4 > limit {
		return 0, nil, false, errors.New("sql: the server sent a message larger than the limit")
	}
	if len(b) < 1+n {
		return 0, nil, false, nil
	}
	typ, body = b[0], b[5:1+n]
	*in = b[1+n:]
	return typ, body, true, nil
}

// reader walks the fields of a message body.
type reader struct {
	b   []byte
	bad bool
}

func (r *reader) u8() byte {
	if len(r.b) < 1 {
		r.bad = true
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) i16() int {
	if len(r.b) < 2 {
		r.bad = true
		return 0
	}
	v := int(int16(binary.BigEndian.Uint16(r.b)))
	r.b = r.b[2:]
	return v
}

func (r *reader) i32() int32 {
	if len(r.b) < 4 {
		r.bad = true
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.b))
	r.b = r.b[4:]
	return v
}

func (r *reader) take(n int) []byte {
	if n < 0 || n > len(r.b) {
		r.bad = true
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) cstr() string {
	for i, c := range r.b {
		if c == 0 {
			s := string(r.b[:i])
			r.b = r.b[i+1:]
			return s
		}
	}
	r.bad = true
	return ""
}

// parseError reads an ErrorResponse or NoticeResponse body.
func parseError(body []byte) *Error {
	e := &Error{}
	r := reader{b: body}
	for {
		c := r.u8()
		if c == 0 || r.bad {
			break
		}
		v := r.cstr()
		switch c {
		case 'S':
			if e.Severity == "" {
				e.Severity = v
			}
		case 'V':
			e.Severity = v // the non-localised name wins
		case 'C':
			e.Code = v
		case 'M':
			e.Message = v
		case 'D':
			e.Detail = v
		case 'H':
			e.Hint = v
		case 'P':
			e.Position = v
		case 'W':
			e.Where = v
		case 's':
			e.Schema = v
		case 't':
			e.Table = v
		case 'c':
			e.Column = v
		case 'd':
			e.DataType = v
		case 'n':
			e.Constraint = v
		}
	}
	return e
}

// column is a result column as RowDescription reports it.
type column struct {
	name string
	oid  uint32
}

func parseRowDescription(body []byte) ([]column, error) {
	r := reader{b: body}
	n := r.i16()
	if r.bad || n < 0 {
		return nil, errFrame
	}
	cols := make([]column, 0, n)
	for i := 0; i < n; i++ {
		name := r.cstr()
		r.i32() // table oid
		r.i16() // attribute number
		oid := uint32(r.i32())
		r.i16() // type size
		r.i32() // type modifier
		r.i16() // format code
		if r.bad {
			return nil, errFrame
		}
		cols = append(cols, column{name, oid})
	}
	return cols, nil
}

// affectedFromTag reads the row count at the end of a command tag ("INSERT 0 5",
// "UPDATE 3", "SELECT 7"); 0 if there is none.
func affectedFromTag(tag string) int64 {
	for i := len(tag) - 1; i >= 0; i-- {
		if tag[i] == ' ' {
			n, err := strconv.ParseInt(tag[i+1:], 10, 64)
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}
