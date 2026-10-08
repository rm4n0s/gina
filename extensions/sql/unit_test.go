package sql

import (
	"database/sql"
	"encoding/base64"
	"errors"
	gtls "github.com/rm4n0s/gina/extensions/tls"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rm4n0s/gina"
)

// RFC 7677 §3: the SCRAM-SHA-256 exchange for user "user", password "pencil". The
// RFC example sends the user name in the message; PostgreSQL sends it empty, which
// changes the messages but not the arithmetic, so this checks the proof against
// the RFC's own client-first-bare.
func TestSCRAMRFC7677(t *testing.T) {
	s := newSCRAMNonce("pencil", "rOprNGfwEbeRWgbNEkqO")
	s.firstBare = "n=user,r=rOprNGfwEbeRWgbNEkqO" // the RFC's message, with its user name
	serverFirst := "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	final, err := s.final([]byte(serverFirst))
	if err != nil {
		t.Fatal(err)
	}
	want := "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	if string(final) != want {
		t.Fatalf("client-final\n got %s\nwant %s", final, want)
	}
	if err := s.verify([]byte("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")); err != nil {
		t.Fatal(err)
	}
	if s.verify([]byte("v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")) == nil || s.verify([]byte("x=")) == nil || s.verify(nil) == nil {
		t.Fatal("accepted a wrong server signature")
	}
}

func TestSCRAMRejectsBadServerFirst(t *testing.T) {
	for name, sf := range map[string]string{
		"nonce does not extend ours": "r=other,s=c2FsdA==,i=4096",
		"nonce is exactly ours":      "r=abc,s=c2FsdA==,i=4096",
		"no salt":                    "r=abcxyz,i=4096",
		"bad salt":                   "r=abcxyz,s=!!!,i=4096",
		"no iterations":              "r=abcxyz,s=c2FsdA==",
		"zero iterations":            "r=abcxyz,s=c2FsdA==,i=0",
		"absurd iterations":          "r=abcxyz,s=c2FsdA==,i=99999999999",
		"empty":                      "",
	} {
		if _, err := newSCRAMNonce("pw", "abc").final([]byte(sf)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if s, err := newSCRAM("pw"); err != nil || len(s.nonce) < 20 || !strings.HasPrefix(string(s.first()), "n,,n=,r=") {
		t.Fatalf("%v %+v", err, s)
	}
}

func TestMD5Password(t *testing.T) {
	// the value libpq computes for user "postgres", password "secret", salt 01 02 03 04
	got := md5Password("postgres", "secret", []byte{1, 2, 3, 4})
	if !strings.HasPrefix(got, "md5") || len(got) != 35 {
		t.Fatalf("%q", got)
	}
	if got == md5Password("postgres", "secret", []byte{1, 2, 3, 5}) || got == md5Password("other", "secret", []byte{1, 2, 3, 4}) {
		t.Fatal("not salted or not keyed by user")
	}
}

func TestFrames(t *testing.T) {
	var w wbuf
	m := w.begin('T')
	w.cstr("hello")
	w.end(m)
	in := append(append([]byte(nil), w.b...), 'Z', 0, 0, 0, 5, 'I', 'D') // a second message and the start of a third
	typ, body, ok, err := nextFrame(&in, 1<<20)
	if err != nil || !ok || typ != 'T' || string(body) != "hello\x00" {
		t.Fatalf("%c %q %v %v", typ, body, ok, err)
	}
	typ, body, ok, err = nextFrame(&in, 1<<20)
	if err != nil || !ok || typ != 'Z' || string(body) != "I" {
		t.Fatalf("%c %q %v %v", typ, body, ok, err)
	}
	if _, _, ok, err = nextFrame(&in, 1<<20); ok || err != nil || len(in) != 1 {
		t.Fatal("a partial message was consumed")
	}
	for name, b := range map[string][]byte{
		"length below 4": {'X', 0, 0, 0, 3},
		"huge length":    {'X', 0xFF, 0xFF, 0xFF, 0xFF},
		"over the limit": {'D', 0, 1, 0, 0},
	} {
		if _, _, _, err := nextFrame(&b, 1000); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseError(t *testing.T) {
	body := []byte("SERROR\x00VERROR\x00C23505\x00Mduplicate key\x00Dkey (id)=(1) exists\x00Hsee docs\x00P12\x00s" + "public\x00tt\x00nt_pkey\x00Zignored\x00\x00")
	e := parseError(body)
	if e.Severity != "ERROR" || e.Code != "23505" || e.Message != "duplicate key" || e.Detail != "key (id)=(1) exists" ||
		e.Hint != "see docs" || e.Position != "12" || e.Schema != "public" || e.Table != "t" || e.Constraint != "t_pkey" {
		t.Fatalf("%+v", e)
	}
	if !strings.Contains(e.Error(), "23505") || !strings.Contains(e.Error(), "duplicate key") {
		t.Fatal(e.Error())
	}
	parseError(body[:7]) // truncated: no panic
	parseError(nil)
}

func TestRowDescriptionAndTags(t *testing.T) {
	var w wbuf
	m := w.begin('T')
	w.i16(2)
	for _, c := range []struct {
		n   string
		oid int32
	}{{"id", OIDInt4}, {"name", OIDText}} {
		w.cstr(c.n)
		w.i32(0)
		w.i16(0)
		w.i32(c.oid)
		w.i16(-1)
		w.i32(-1)
		w.i16(0)
	}
	w.end(m)
	cols, err := parseRowDescription(w.b[5:])
	if err != nil || len(cols) != 2 || cols[1].name != "name" || cols[0].oid != OIDInt4 {
		t.Fatalf("%+v %v", cols, err)
	}
	for i := 0; i < len(w.b)-5; i++ {
		parseRowDescription(w.b[5 : 5+i]) // no panic on any prefix
	}
	for tag, want := range map[string]int64{"INSERT 0 5": 5, "UPDATE 3": 3, "SELECT 7": 7, "CREATE TABLE": 0, "": 0, "BEGIN": 0, "MOVE 2": 2} {
		if got := affectedFromTag(tag); got != want {
			t.Errorf("%q: %d, want %d", tag, got, want)
		}
	}
}

func TestEncodeArgs(t *testing.T) {
	type myInt int
	type myStr string
	var nilp *int
	seven := 7
	when := time.Date(2024, 1, 2, 3, 4, 5, 600000000, time.FixedZone("x", 2*3600))
	for _, c := range []struct {
		in     any
		text   string
		null   bool
		binary bool
	}{
		{nil, "", true, false},
		{"hi", "hi", false, false},
		{[]byte("raw"), "raw", false, true},
		{[]byte(nil), "", true, false},
		{true, "true", false, false},
		{int8(-3), "-3", false, false},
		{uint64(math.MaxUint64), "18446744073709551615", false, false},
		{3.5, "3.5", false, false},
		{float32(0.1), "0.1", false, false},
		{math.NaN(), "NaN", false, false},
		{math.Inf(1), "Infinity", false, false},
		{math.Inf(-1), "-Infinity", false, false},
		{when, "2024-01-02T03:04:05.6+02:00", false, false},
		{myInt(9), "9", false, false},
		{myStr("s"), "s", false, false},
		{&seven, "7", false, false},
		{nilp, "", true, false},
		{sql.NullString{String: "ns", Valid: true}, "ns", false, false},
		{sql.NullString{}, "", true, false},
		{sql.NullInt64{Int64: 5, Valid: true}, "5", false, false},
	} {
		p, err := encodeArg(c.in)
		if err != nil || p.null != c.null || (!c.null && string(p.data) != c.text) || (p.format == 1) != c.binary {
			t.Errorf("%#v: %+v %v", c.in, p, err)
		}
	}
	for _, bad := range []any{struct{}{}, make(chan int), map[string]int{}, []int{1}, func() {}} {
		if _, err := encodeArg(bad); err == nil {
			t.Errorf("%T accepted", bad)
		}
	}
}

func TestConvertAssign(t *testing.T) {
	var (
		s   string
		i   int
		i8  int8
		u16 uint16
		f   float64
		b   bool
		bs  []byte
		tm  time.Time
		a   any
		ps  *string
		ns  sql.NullString
		ni  sql.NullInt64
	)
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check(convertAssign(&s, []byte("héllo"), OIDText))
	check(convertAssign(&i, []byte("-42"), OIDInt4))
	check(convertAssign(&i8, []byte("127"), OIDInt2))
	check(convertAssign(&u16, []byte("65535"), OIDInt4))
	check(convertAssign(&f, []byte("-1.5e3"), OIDFloat8))
	check(convertAssign(&b, []byte("t"), OIDBool))
	check(convertAssign(&bs, []byte(`\xdeadbeef`), OIDBytea))
	check(convertAssign(&tm, []byte("2024-05-06 07:08:09.5+02"), OIDTimestamptz))
	if s != "héllo" || i != -42 || i8 != 127 || u16 != 65535 || f != -1500 || !b || string(bs) != "\xde\xad\xbe\xef" ||
		!tm.Equal(time.Date(2024, 5, 6, 5, 8, 9, 500000000, time.UTC)) {
		t.Fatalf("%q %d %d %d %v %v %x %v", s, i, i8, u16, f, b, bs, tm)
	}
	for _, c := range []struct {
		src string
		oid uint32
		out any
	}{
		{"5", OIDInt8, int64(5)}, {"2.5", OIDFloat8, 2.5}, {"f", OIDBool, false}, {"x", OIDText, "x"}, {"1.50", OIDNumeric, "1.50"},
		{`\x00ff`, OIDBytea, []byte{0, 255}}, {"2024-05-06", OIDDate, time.Date(2024, 5, 6, 0, 0, 0, 0, time.UTC)},
		{"2024-05-06 01:02:03", OIDTimestamp, time.Date(2024, 5, 6, 1, 2, 3, 0, time.UTC)},
	} {
		check(convertAssign(&a, []byte(c.src), c.oid))
		if !reflect.DeepEqual(a, c.out) {
			t.Errorf("%s (%d): %#v, want %#v", c.src, c.oid, a, c.out)
		}
	}
	check(convertAssign(&a, nil, OIDInt4))
	if a != nil {
		t.Fatal(a)
	}
	// NULL: errors for plain values, nil for pointers, Null types and any
	for _, d := range []any{&s, &i, &u16, &f, &b, &tm} {
		if convertAssign(d, nil, OIDText) == nil {
			t.Errorf("NULL into %T accepted", d)
		}
	}
	check(convertAssign(&bs, nil, OIDBytea))
	if bs != nil {
		t.Fatal("NULL bytea should be a nil slice")
	}
	check(convertAssign(&ps, nil, OIDText))
	if ps != nil {
		t.Fatal("NULL should be a nil pointer")
	}
	check(convertAssign(&ps, []byte("p"), OIDText))
	if ps == nil || *ps != "p" {
		t.Fatal("pointer destination")
	}
	check(convertAssign(&ns, []byte("x"), OIDText))
	check(convertAssign(&ni, nil, OIDInt4))
	if !ns.Valid || ns.String != "x" || ni.Valid {
		t.Fatalf("%+v %+v", ns, ni)
	}
	// overflow and garbage
	for _, c := range []struct {
		d   any
		src string
	}{{&i8, "128"}, {&u16, "-1"}, {&i, "abc"}, {&f, "x"}, {&b, "maybe"}, {&tm, "yesterday"}, {&tm, "infinity"}, {new(struct{}), "1"}, {nil, "1"}, {i, "1"}} {
		if convertAssign(c.d, []byte(c.src), OIDText) == nil {
			t.Errorf("%T <- %q accepted", c.d, c.src)
		}
	}
	// a named type
	type level int
	var lv level
	check(convertAssign(&lv, []byte("3"), OIDInt4))
	if lv != 3 {
		t.Fatal(lv)
	}
}

func TestReplyRoundTrip(t *testing.T) {
	cols := []column{{"id", OIDInt4}, {"name", OIDText}, {"gone", OIDText}}
	var rows []byte
	rows = appendLP1(rows, []byte("1"))
	rows = appendLP1(rows, []byte("héllo"))
	rows = append(rows, 0) // NULL
	rows = appendLP1(rows, []byte("2"))
	rows = appendLP1(rows, nil) // empty, not NULL
	rows = appendLP1(rows, []byte("x"))
	enc := encodeRows(encodeColumns(cols), 2, rows, false, "SELECT 2", 2)
	rep, err := decodeReply(enc, 9, gina.Handle(77))
	if err != nil || rep.Kind != ReplyRows || rep.More || rep.Tag != "SELECT 2" || rep.RowsAffected != 2 || rep.ID != 9 {
		t.Fatalf("%+v %v", rep, err)
	}
	var id int
	var name string
	var gone *string
	if !rep.Rows.Next() || rep.Rows.Scan(&id, &name, &gone) != nil || id != 1 || name != "héllo" || gone != nil {
		t.Fatalf("%d %q %v", id, name, gone)
	}
	if !rep.Rows.Next() || rep.Rows.Scan(&id, &name, &gone) != nil || id != 2 || name != "" || gone == nil || *gone != "x" {
		t.Fatalf("%d %q %v", id, name, gone)
	}
	if rep.Rows.Next() || rep.Rows.Len() != 0 {
		t.Fatal("extra row")
	}
	if rep.Rows.Scan(&id) == nil {
		t.Fatal("wrong destination count accepted")
	}
	// more set: no trailer
	enc = encodeRows(encodeColumns(cols), 2, rows, true, "", 0)
	if rep, err = decodeReply(enc, 1, 0); err != nil || !rep.More || rep.Tag != "" {
		t.Fatalf("%+v %v", rep, err)
	}
	// every truncation of every kind of reply fails cleanly
	for _, full := range [][]byte{
		encodeRows(encodeColumns(cols), 2, rows, false, "SELECT 2", 2),
		encodeDone("INSERT 0 1", 1),
		encodeError(&Error{Severity: "ERROR", Code: "42P01", Message: "no such table"}),
		encodeError(clientErr(ClientTimeout, "slow")),
		encodeTx(5),
	} {
		if _, err := decodeReply(full, 1, 0); err != nil {
			t.Fatalf("full reply: %v", err)
		}
		for i := 0; i < len(full); i++ {
			decodeReply(full[:i], 1, 0) // must not panic
		}
	}
	d, _ := decodeReply(encodeDone("DELETE 4", 4), 3, 0)
	e, _ := decodeReply(encodeError(&Error{Severity: "ERROR", Code: "23505", Message: "dup", Constraint: "c"}), 3, 0)
	x, _ := decodeReply(encodeTx(12), 3, gina.Handle(5))
	var pe *Error
	if d.Kind != ReplyDone || d.RowsAffected != 4 || !errors.As(e.Err, &pe) || pe.Constraint != "c" || x.Tx != (Tx{Conn: 5, ID: 12}) {
		t.Fatalf("%+v %+v %+v", d, e, x)
	}
	if _, err := decodeReply([]byte{99}, 1, 0); err == nil {
		t.Fatal("unknown kind accepted")
	}
	// a zero-column result still has its rows
	enc = encodeRows(encodeColumns(nil), 3, nil, false, "SELECT 3", 3)
	if rep, err = decodeReply(enc, 1, 0); err != nil || rep.Rows.Len() != 3 || !rep.Rows.Next() {
		t.Fatalf("%+v %v", rep, err)
	}
}

func TestErrorsIs(t *testing.T) {
	var err error = clientErr(ClientTimeout, "x")
	if !errors.Is(err, ErrTimeout) || errors.Is(err, ErrCanceled) || errors.Is(&Error{Code: "23505"}, ErrTimeout) {
		t.Fatal("errors.Is by kind")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatal(err.Error())
	}
}

func TestRequestRoundTrip(t *testing.T) {
	r := &Request{ID: 7, SQL: "SELECT $1, $2, $3, $4", Args: []any{nil, "s", []byte{1, 2}, 5}, Timeout: 1500 * time.Millisecond, BatchRows: 50,
		ReplyTo: gina.Handle(99), Isolation: IsolationSerializable, ReadOnly: true}
	b, err := r.marshal(opQuery, 4, r.ReplyTo)
	if err != nil {
		t.Fatal(err)
	}
	q, err := parseRequest(b)
	if err != nil || q.op != opQuery || q.id != 7 || q.replyTo != 99 || q.txid != 4 || q.timeout != 1500*time.Millisecond || q.batchRows != 50 ||
		!q.readOnly || q.isolation != IsolationSerializable || q.sql != r.SQL || len(q.params) != 4 {
		t.Fatalf("%+v %v", q, err)
	}
	if !q.params[0].null || string(q.params[1].data) != "s" || q.params[2].format != 1 || string(q.params[3].data) != "5" {
		t.Fatalf("%+v", q.params)
	}
	for i := 0; i < len(b); i++ {
		parseRequest(b[:i]) // truncations never panic
	}
	// bad requests
	bad := append([]byte(nil), b...)
	bad[0] = 9
	if _, err := parseRequest(bad); err == nil {
		t.Fatal("bad version accepted")
	}
	bad = append([]byte(nil), b...)
	bad[1] = 0
	if _, err := parseRequest(bad); err == nil {
		t.Fatal("bad op accepted")
	}
	if _, err := (&Request{Args: []any{struct{}{}}}).marshal(opQuery, 0, 0); err == nil || !strings.Contains(err.Error(), "argument 1") {
		t.Fatalf("%v", err)
	}
	if _, err := (&Request{Timeout: -1}).marshal(opQuery, 0, 0); err == nil {
		t.Fatal("negative timeout accepted")
	}
	if _, err := (&Request{Isolation: 9}).marshal(opBegin, 0, 0); err == nil {
		t.Fatal("bad isolation accepted")
	}
	// Begin carries no SQL
	b, _ = (&Request{ID: 1, ReadOnly: true}).marshal(opBegin, 0, 0)
	if q, err := parseRequest(b); err != nil || q.op != opBegin || !q.readOnly || q.sql != "" {
		t.Fatalf("%+v %v", q, err)
	}
	// a tiny timeout still means "some", not "none"
	b, _ = (&Request{Timeout: time.Microsecond}).marshal(opExec, 0, 0)
	if q, _ := parseRequest(b); q.timeout != time.Millisecond {
		t.Fatalf("%v", q.timeout)
	}
}

func TestExtendedMessageBytes(t *testing.T) {
	var w wbuf
	if err := w.extended("SELECT $1, $2", []param{{data: []byte("a")}, {null: true}}); err != nil {
		t.Fatal(err)
	}
	// Parse, Bind, Describe, Execute, Sync in that order
	var types []byte
	in := w.b
	for len(in) > 0 {
		typ, _, ok, err := nextFrame(&in, 1<<20)
		if err != nil || !ok {
			t.Fatal(err)
		}
		types = append(types, typ)
	}
	if string(types) != "PBDES" {
		t.Fatalf("%s", types)
	}
	if err := w.extended("x", make([]param, 70000)); err == nil {
		t.Fatal("more than 65535 arguments accepted")
	}
	if p := cancelPacket(0x01020304, []byte{9, 8, 7, 6}); base64.StdEncoding.EncodeToString(p) != base64.StdEncoding.EncodeToString([]byte{0, 0, 0, 16, 4, 210, 22, 46, 1, 2, 3, 4, 9, 8, 7, 6}) {
		t.Fatalf("%v", p)
	}
}

func TestConfigDefaults(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no addr":        {User: "u"},
		"no user":        {Addr: mustAddr("127.0.0.1:5432")},
		"port 0":         {Addr: mustAddr("127.0.0.1:0"), User: "u"},
		"negative":       {Addr: mustAddr("127.0.0.1:5432"), User: "u", Queue: -1},
		"type id":        {Addr: mustAddr("127.0.0.1:5432"), User: "u", TypeID: 253},
		"tls incomplete": {Addr: mustAddr("127.0.0.1:5432"), User: "u", TLS: new(gtls.ClientConfig)},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	db, err := New(Config{Addr: mustAddr("127.0.0.1:5432"), User: "u", MaxOpenConns: 4, MaxIdleConns: 9})
	if err != nil {
		t.Fatal(err)
	}
	c := db.cfg
	if c.Database != "u" || c.MaxIdleConns != 4 || c.BatchRows != 128 || c.TypeID != 230 || c.QueueTimeout != 30*time.Second || c.ApplicationName != "gina" {
		t.Fatalf("%+v", c)
	}
	db, _ = New(Config{Addr: mustAddr("127.0.0.1:5432"), User: "u", MaxIdleConns: -1})
	if db.cfg.MaxIdleConns != 0 || db.cfg.MaxOpenConns != 10 {
		t.Fatalf("%+v", db.cfg)
	}
	if err := db.Install(&gina.SystemSpec{}); err == nil {
		t.Fatal("Install accepted a spec without the shard")
	}
	if h := db.Pool(); h.Shard() != 0 || h.Type() != 230 {
		t.Fatalf("%#x", uint64(h))
	}
}
