package sql

// A connection is the isolate that owns one PostgreSQL socket. It is a state
// machine over bytes: the handshake (connect, optional TLS, login), then one
// request at a time. Everything it waits for is a Gina I/O completion or a message,
// so it never blocks the shard, and the engine closes its socket if it dies.
//
// Lifecycle: the pool spawns it and sends tagOpen. Once logged in it tells the pool
// (tagReady) and idles, still reading its socket so that a server that hangs up
// is noticed. The pool leases it a request (tagRun) or a transaction; replies go
// straight to the caller. When the lease ends it says tagFree. A connection that
// fails tells the pool and exits; one that is told to close says goodbye to the
// server first.
//
// Reads are interruptible (WaitIOOrMessage): while a query runs the connection must
// still hear a Cancel, a Continue or one of its timers. Writes are not, but they are
// short and have a timeout.

import (
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

type connPhase uint8

const (
	cpNew connPhase = iota
	cpConnecting
	cpSSL     // SSLRequest sent, waiting for the one-byte answer
	cpTLS     // TLS handshake
	cpStartup // login: authentication, parameter status, ReadyForQuery
	cpReady   // logged in; between requests
	cpRun     // a request is on the wire
	cpReset   // rolling back what a request left open
	cpClosing // Terminate queued: send it, close, exit
)

type ioKind uint8

const (
	ioNone ioKind = iota
	ioConnect
	ioSend
	ioRecv
)

const (
	batchBytes   = 64 << 10 // a batch is closed at about this size
	writeTimeout = 30 * time.Second
	redeliverGap = 2 * time.Millisecond
	redeliverMax = 500 // about a second
)

// what to do once a held reply has been delivered
type thenKind uint8

const (
	thenBlock  thenKind = iota + 1 // it was a batch: wait for Continue
	thenFinish                     // it was the last reply: end the lease
)

// active is the request on the wire.
type active struct {
	id        uint32
	op        opCode
	replyTo   gina.Handle
	batchRows int
	discard   bool // rows are not wanted (Exec, or a cancelled query)

	cols    []column
	colsEnc []byte
	rowbuf  []byte
	nrows   int

	tag      string
	affected int64
	err      *Error

	deadline  uint64 // Request.Timeout; 0 = none
	hard      uint64 // after a cancel: when the connection gives up on the server
	cancelWhy ClientError
	cancelMsg string
	copyOut   bool
}

type conn struct {
	pool  gina.Handle
	phase connPhase
	io    ioKind
	fd    gina.FDHandle
	tc    *gtls.Conn

	hsDeadline uint64
	born       uint64
	rbuf       []byte
	in         []byte // received plain bytes; in[inOff:] is not yet parsed
	inOff      int
	pout       []byte // plain connection: bytes waiting to go out
	w          wbuf

	scram    *scramClient
	pid      int32
	key      []byte
	txStatus byte
	ready    bool // has told the pool it is logged in

	// the lease
	leased  bool
	owner   gina.Handle
	tx      bool
	txid    uint32
	txUntil uint64
	rq      *active

	// streaming
	interrupted bool // the last read was cut short by a message that has yet to be handled
	blocked     bool
	streamUntil uint64
	held        []byte
	heldTo      gina.Handle
	heldID      uint32
	heldThen    thenKind
	heldTries   int
	lifeExpired bool
	idleSince   uint64
	timerLife   gina.TimerID
	timerIdle   gina.TimerID
	timerQuery  gina.TimerID
	timerStream gina.TimerID
	timerTx     gina.TimerID
	timerRetry  gina.TimerID
}

func (d *DB) connHandler(c *conn, g *gina.Ctx, m *gina.Message) gina.Effect {
	if c.step(d, g, m) {
		return gina.Done()
	}
	return c.drive(d, g)
}

// step handles one message. It returns true when the isolate is finished.
func (c *conn) step(d *DB, g *gina.Ctx, m *gina.Message) bool {
	switch m.Tag {
	case tagOpen:
		if c.phase == cpNew {
			c.pool = m.Source
			return c.dial(d, g)
		}
	case gina.TagIOConnect:
		c.io = ioNone
		if res := gina.PayloadAs[gina.IOResult](m).Result; res < 0 {
			return c.die(d, g, clientErr(ClientConnect, "connect: "+syscall.Errno(-res).Error()))
		}
		c.startSession(d)
	case gina.TagIOSend:
		c.io = ioNone
		res := gina.PayloadAs[gina.IOResult](m).Result
		if res < 0 {
			return c.die(d, g, clientErr(c.failKind(), "write: "+syscall.Errno(-res).Error()))
		}
		if c.tc != nil {
			c.tc.ConsumeOut(int(res))
		} else {
			c.pout = c.pout[:copy(c.pout, c.pout[res:])]
		}
	case gina.TagIORecv:
		c.io = ioNone
		return c.received(d, g, gina.PayloadAs[gina.IOResult](m).Result)
	case tagRun:
		return c.onRun(d, g, m)
	case TagTx:
		return c.onTx(d, g, m)
	case TagCancel:
		if c.mine(m.Source) && c.rq != nil && c.rq.id == m.Correlation {
			c.abort(g, d, ClientCanceled, "canceled")
			return c.process(d, g)
		}
	case tagCancelRun:
		if b := g.Data(); len(b) == 8 && c.rq != nil && c.rq.id == m.Correlation && uint64(c.owner) == be64(b) {
			c.abort(g, d, ClientCanceled, "canceled")
			return c.process(d, g)
		}
	case TagContinue:
		if c.blocked && c.rq != nil && c.rq.id == m.Correlation && c.mine(m.Source) {
			c.blocked = false
			g.CancelTimer(c.timerStream)
			return c.process(d, g)
		}
	case tagClose:
		c.closing(g)
	case tagTimerLife:
		return c.onLifetime(d, g)
	case tagTimerIdle:
		if c.phase == cpReady && !c.leased && d.cfg.ConnMaxIdleTime > 0 && g.Now() >= c.idleSince+uint64(d.cfg.ConnMaxIdleTime) {
			c.retire(g)
		}
	case tagTimerQuery:
		if rq := c.rq; rq != nil && rq.deadline != 0 && rq.cancelWhy == 0 && g.Now() >= rq.deadline {
			c.abort(g, d, ClientTimeout, "the request timed out")
			return c.process(d, g)
		}
	case tagTimerStream:
		if c.blocked && g.Now() >= c.streamUntil {
			c.abort(g, d, ClientTimeout, "nobody asked for the rest of the result in time")
			return c.process(d, g)
		}
	case tagTimerTx:
		if c.tx && c.phase == cpReady && c.rq == nil && g.Now() >= c.txUntil {
			c.tx = false
			c.startReset(d)
		}
	case tagTimerRetry:
		return c.redeliver(d, g)
	case gina.TagShutdown:
		c.closeFD(g)
		return true
	}
	return false
}

func be64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 | uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// mine reports whether src may continue, cancel or use the transaction of the
// current lease.
func (c *conn) mine(src gina.Handle) bool {
	if !c.leased {
		return false
	}
	return src == c.owner || (c.rq != nil && src == c.rq.replyTo)
}

func (c *conn) failKind() ClientError {
	if c.ready {
		return ClientConnLost
	}
	return ClientConnect
}

// ---- connecting and logging in ----

func (c *conn) remaining(g *gina.Ctx, deadline uint64) time.Duration {
	return time.Duration(int64(deadline) - int64(g.Now()))
}

func (c *conn) dial(d *DB, g *gina.Ctx) bool {
	cfg := &d.cfg
	c.hsDeadline = g.Now() + uint64(cfg.ConnectTimeout)
	fd, err := g.Dial(gina.DialSpec{IP: cfg.Addr.Addr(), Port: cfg.Addr.Port()})
	if err != nil {
		return c.die(d, g, clientErr(ClientConnect, err.Error()))
	}
	c.fd, c.phase, c.io = fd, cpConnecting, ioConnect
	c.rbuf = make([]byte, 16<<10)
	g.IOConnect(fd, cfg.ConnectTimeout)
	return false
}

// startSession runs when the TCP connection is up: ask for TLS, or log in.
func (c *conn) startSession(d *DB) {
	if d.cfg.TLS != nil {
		c.w.reset()
		c.w.sslRequest()
		c.pout = append(c.pout, c.w.b...)
		c.phase = cpSSL
		return
	}
	c.sendStartup(d)
}

func (c *conn) sendStartup(d *DB) {
	cfg := &d.cfg
	params := [][2]string{{"user", cfg.User}, {"database", cfg.Database}, {"application_name", cfg.ApplicationName},
		{"client_encoding", "UTF8"}, {"DateStyle", "ISO"}}
	keys := make([]string, 0, len(cfg.Params))
	for k := range cfg.Params {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		switch k {
		case "user", "database", "application_name", "client_encoding", "DateStyle", "replication":
			continue // the client depends on these
		}
		params = append(params, [2]string{k, cfg.Params[k]})
	}
	c.w.reset()
	c.w.startup(params)
	c.phase = cpStartup
	c.out(c.w.b)
}

// out queues plain bytes for the server, through TLS when there is any.
func (c *conn) out(b []byte) {
	if c.tc != nil {
		if c.tc.Write(b) != nil {
			return // Err reports it, and drive fails the connection
		}
		return
	}
	c.pout = append(c.pout, b...)
}

func (c *conn) outgoing() []byte {
	if c.tc != nil {
		return c.tc.Outgoing()
	}
	return c.pout
}

// received handles the completion of a read.
func (c *conn) received(d *DB, g *gina.Ctx, res int64) bool {
	switch {
	case res == -int64(syscall.ECANCELED):
		// A message interrupted the read and is next in the mailbox. Reading again at
		// once would interrupt at once again: wait for the message, and read after it.
		c.interrupted = true
		return false
	case res == -int64(syscall.ETIMEDOUT):
		switch c.phase {
		case cpRun, cpReset:
			return c.die(d, g, clientErr(ClientTimeout, "the server did not react to the cancellation"))
		case cpReady:
			return false
		}
		return c.die(d, g, clientErr(ClientConnect, "timed out"))
	case res < 0:
		return c.die(d, g, clientErr(c.failKind(), "read: "+syscall.Errno(-res).Error()))
	case res == 0:
		return c.die(d, g, clientErr(c.failKind(), "the server closed the connection"))
	}
	if c.phase == cpSSL {
		return c.sslAnswer(d, g, c.rbuf[0])
	}
	data := c.rbuf[:res]
	if c.tc != nil {
		if err := c.tc.Feed(data); err != nil {
			return c.die(d, g, clientErr(c.failKind(), "tls: "+err.Error()))
		}
		if n := c.tc.PlainLen(); n > 0 {
			off := len(c.in)
			c.in = append(c.in, make([]byte, n)...)
			c.tc.ReadPlain(c.in[off:])
		}
	} else {
		c.in = append(c.in, data...)
	}
	return c.process(d, g)
}

func (c *conn) sslAnswer(d *DB, g *gina.Ctx, b byte) bool {
	if b != 'S' {
		return c.die(d, g, clientErr(ClientConnect, "the server does not support TLS"))
	}
	tc, err := gtls.NewClient(d.cfg.TLS)
	if err != nil {
		return c.die(d, g, clientErr(ClientConnect, "tls: "+err.Error()))
	}
	c.tc, c.phase = tc, cpTLS
	return false
}

// startupMsg handles a backend message during login.
func (c *conn) startupMsg(d *DB, g *gina.Ctx, typ byte, body []byte) bool {
	switch typ {
	case 'R':
		return c.authenticate(d, g, body)
	case 'K':
		if len(body) < 5 {
			return c.die(d, g, clientErr(ClientProtocol, "short BackendKeyData"))
		}
		r := reader{b: body}
		c.pid = r.i32()
		c.key = slices.Clone(r.b)
	case 'Z':
		if len(body) != 1 {
			return c.die(d, g, clientErr(ClientProtocol, "bad ReadyForQuery"))
		}
		c.txStatus = body[0]
		c.phase, c.ready = cpReady, true
		c.born = g.Now()
		c.scram = nil
		if d.cfg.ConnMaxLifetime > 0 {
			c.timerLife = g.RegisterTimer(d.cfg.ConnMaxLifetime, tagTimerLife)
		}
		if g.SendRaw(c.pool, tagReady, nil) != gina.SendOK {
			c.closeFD(g)
			return true
		}
		c.armIdle(d, g)
	case 'E':
		e := parseError(body)
		e.Client = ClientConnect
		return c.die(d, g, e)
	case 'N', 'S', 'v':
	default:
		return c.die(d, g, clientErr(ClientProtocol, "unexpected message "+strconv.Quote(string(typ))+" during login"))
	}
	return false
}

func (c *conn) authenticate(d *DB, g *gina.Ctx, body []byte) bool {
	r := reader{b: body}
	code := r.i32()
	if r.bad {
		return c.die(d, g, clientErr(ClientProtocol, "short Authentication message"))
	}
	cfg := &d.cfg
	needPassword := func() bool {
		if cfg.Password == "" {
			c.die(d, g, clientErr(ClientConnect, "the server asks for a password and Config.Password is empty"))
			return false
		}
		return true
	}
	c.w.reset()
	switch code {
	case 0: // ok
		return false
	case 3: // cleartext
		if !needPassword() {
			return true
		}
		c.w.password(cfg.Password)
	case 5: // md5
		if !needPassword() {
			return true
		}
		if len(r.b) < 4 {
			return c.die(d, g, clientErr(ClientProtocol, "short MD5 salt"))
		}
		c.w.password(md5Password(cfg.User, cfg.Password, r.b[:4]))
	case 10: // SASL: pick SCRAM-SHA-256
		if !needPassword() {
			return true
		}
		offered := false
		for len(r.b) > 0 && r.b[0] != 0 {
			if r.cstr() == scramMech {
				offered = true
			}
		}
		if !offered {
			return c.die(d, g, clientErr(ClientConnect, "the server offers no SASL mechanism this client supports (SCRAM-SHA-256)"))
		}
		s, err := newSCRAM(cfg.Password)
		if err != nil {
			return c.die(d, g, clientErr(ClientConnect, err.Error()))
		}
		c.scram = s
		c.w.saslInitial(scramMech, s.first())
	case 11:
		if c.scram == nil {
			return c.die(d, g, clientErr(ClientProtocol, "unexpected SASLContinue"))
		}
		resp, err := c.scram.final(r.b)
		if err != nil {
			return c.die(d, g, clientErr(ClientConnect, err.Error()))
		}
		c.w.saslResponse(resp)
	case 12:
		if c.scram == nil || c.scram.verify(r.b) != nil {
			return c.die(d, g, clientErr(ClientConnect, "the server failed to prove it knows the password"))
		}
		return false
	default:
		return c.die(d, g, clientErr(ClientUnsupported, "authentication method "+strconv.Itoa(int(code))+" is not supported"))
	}
	c.out(c.w.b)
	return false
}

// ---- parsing what the server sends ----

// process parses and handles the buffered backend messages until it runs out, or
// the connection has to wait for the caller.
func (c *conn) process(d *DB, g *gina.Ctx) bool {
	for !c.blocked && c.held == nil && c.phase != cpClosing {
		view := c.in[c.inOff:]
		typ, body, ok, err := nextFrame(&view, d.maxMsg)
		if err != nil {
			return c.die(d, g, clientErr(ClientProtocol, err.Error()))
		}
		if !ok {
			break
		}
		c.inOff = len(c.in) - len(view)
		var stop bool
		switch c.phase {
		case cpStartup:
			stop = c.startupMsg(d, g, typ, body)
		case cpRun, cpReset:
			stop = c.runMsg(d, g, typ, body)
		default: // idle, or about to leave
			stop = c.idleMsg(d, g, typ, body)
		}
		if stop {
			return true
		}
	}
	// Keep what has not been parsed at the front of the buffer.
	switch {
	case c.inOff == len(c.in) && cap(c.in) > 1<<20:
		c.in, c.inOff = nil, 0 // do not hold on to the memory of one huge row
	case c.inOff > 0:
		n := copy(c.in, c.in[c.inOff:])
		c.in, c.inOff = c.in[:n], 0
	}
	return false
}

// idleMsg handles what the server says between requests.
func (c *conn) idleMsg(d *DB, g *gina.Ctx, typ byte, body []byte) bool {
	switch typ {
	case 'E': // FATAL: the server is shutting down, or timed this session out
		e := parseError(body)
		return c.die(d, g, clientErr(ClientConnLost, e.Severity+": "+e.Message))
	case 'N', 'S', 'A':
		return false
	}
	return c.die(d, g, clientErr(ClientProtocol, "unexpected message "+strconv.Quote(string(typ))+" while idle"))
}

func (c *conn) runMsg(d *DB, g *gina.Ctx, typ byte, body []byte) bool {
	rq := c.rq
	switch typ {
	case 'T':
		cols, err := parseRowDescription(body)
		if err != nil {
			return c.die(d, g, clientErr(ClientProtocol, err.Error()))
		}
		if rq != nil {
			rq.cols, rq.colsEnc = cols, encodeColumns(cols)
		}
	case 'D':
		if rq == nil || rq.discard {
			return false
		}
		return c.dataRow(d, g, rq, body)
	case 'C':
		if rq != nil {
			rq.tag = string(body[:max(len(body)-1, 0)])
			rq.affected += affectedFromTag(rq.tag)
		}
	case 'I', 'n', '1', '2', '3', 't', 's', 'N', 'S', 'A':
	case 'E':
		e := parseError(body)
		if rq != nil && rq.err == nil {
			rq.err = e
		}
	case 'G': // CopyInResponse: we have nothing to send
		c.w.reset()
		c.w.copyFail("COPY FROM STDIN is not supported by this client")
		c.out(c.w.b)
		if rq != nil && rq.err == nil {
			rq.err = clientErr(ClientUnsupported, "COPY is not supported")
		}
	case 'H': // CopyOutResponse: drain it
		if rq != nil {
			rq.copyOut = true
			if rq.err == nil {
				rq.err = clientErr(ClientUnsupported, "COPY is not supported")
			}
		}
	case 'd', 'c':
		if rq == nil || !rq.copyOut {
			return c.die(d, g, clientErr(ClientProtocol, "unexpected COPY data"))
		}
	case 'Z':
		if len(body) != 1 {
			return c.die(d, g, clientErr(ClientProtocol, "bad ReadyForQuery"))
		}
		c.txStatus = body[0]
		if c.phase == cpReset {
			return c.release(d, g)
		}
		return c.complete(d, g)
	default:
		return c.die(d, g, clientErr(ClientProtocol, "unexpected message "+strconv.Quote(string(typ))+" in a result"))
	}
	return false
}

// dataRow appends a row to the batch and sends the batch when it is full.
func (c *conn) dataRow(d *DB, g *gina.Ctx, rq *active, body []byte) bool {
	r := reader{b: body}
	n := r.i16()
	if r.bad || n != len(rq.cols) {
		return c.die(d, g, clientErr(ClientProtocol, "DataRow does not match the RowDescription"))
	}
	// A full batch is sent when the next row shows that the result goes on, so a
	// result that is an exact multiple of the batch size has no empty batch at its end.
	if rq.nrows >= rq.batchRows || len(rq.rowbuf) >= batchBytes {
		if c.sendBatch(d, g, rq) {
			return true
		}
	}
	for i := 0; i < n; i++ {
		l := r.i32()
		if l < 0 {
			rq.rowbuf = append(rq.rowbuf, 0)
			continue
		}
		v := r.take(int(l))
		if r.bad {
			return c.die(d, g, clientErr(ClientProtocol, "short DataRow"))
		}
		rq.rowbuf = appendLP1(rq.rowbuf, v)
	}
	rq.nrows++
	return false
}

// appendLP1 writes a value as uvarint(len+1) and the bytes; 0 alone is NULL.
func appendLP1(b []byte, v []byte) []byte {
	return append(appendUvarint(b, uint64(len(v))+1), v...)
}

func appendUvarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// sendBatch sends the rows collected so far with More set, then waits for Continue.
func (c *conn) sendBatch(d *DB, g *gina.Ctx, rq *active) bool {
	payload := encodeRows(rq.colsEnc, rq.nrows, rq.rowbuf, true, "", 0)
	rq.rowbuf, rq.nrows = rq.rowbuf[:0], 0
	if len(payload) > d.maxMsg {
		c.abort(g, d, ClientProtocol, "a row is larger than the system's MaxMessageBytes")
		return false
	}
	switch c.deliver(d, g, rq.replyTo, rq.id, payload, thenBlock) {
	case delivered:
		c.block(d, g)
	case gone:
		c.abort(g, d, ClientCanceled, "the caller is gone")
	}
	return false
}

func (c *conn) block(d *DB, g *gina.Ctx) {
	c.blocked = true
	c.streamUntil = g.Now() + uint64(d.cfg.StreamTimeout)
	c.timerStream = g.RegisterTimer(d.cfg.StreamTimeout, tagTimerStream)
}

type deliverResult uint8

const (
	delivered deliverResult = iota
	held
	gone
)

// deliver sends a reply. A mailbox or ring that is full is not the end of the
// world for a reply the caller is waiting for: keep it and try again shortly.
func (c *conn) deliver(d *DB, g *gina.Ctx, to gina.Handle, id uint32, payload []byte, then thenKind) deliverResult {
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
	return gone // the caller no longer exists, or the reply cannot be sent at all
}

func (c *conn) redeliver(d *DB, g *gina.Ctx) bool {
	if c.held == nil {
		return false
	}
	c.heldTries++
	res := gone
	if c.heldTries < redeliverMax {
		res = c.deliver(d, g, c.heldTo, c.heldID, c.held, c.heldThen)
	}
	if res == held {
		return false // deliver armed the timer again
	}
	then := c.heldThen
	c.held = nil
	switch then {
	case thenBlock:
		if res == delivered {
			if c.rq != nil && c.rq.cancelWhy == 0 {
				c.block(d, g)
			}
		} else {
			c.abort(g, d, ClientCanceled, "the caller is gone")
		}
		return c.process(d, g)
	case thenFinish:
		if c.finish(d, g) {
			return true
		}
		return c.process(d, g)
	}
	return false
}

// complete runs at ReadyForQuery: the request is over, whatever the outcome.
func (c *conn) complete(d *DB, g *gina.Ctx) bool {
	rq := c.rq
	if rq == nil {
		return c.die(d, g, clientErr(ClientProtocol, "ReadyForQuery with no request"))
	}
	g.CancelTimer(c.timerQuery)
	var payload []byte
	switch {
	case rq.cancelWhy != 0:
		payload = encodeError(clientErr(rq.cancelWhy, rq.cancelMsg))
	case rq.err != nil:
		payload = encodeError(rq.err)
	case rq.op == opBegin:
		if c.txStatus != 'T' {
			payload = encodeError(clientErr(ClientProtocol, "BEGIN did not start a transaction"))
			break
		}
		c.tx = true
		c.txid++
		payload = encodeTx(c.txid)
	case rq.op == opCommit && rq.tag == "ROLLBACK":
		c.tx = false
		payload = encodeError(clientErr(ClientTxDone, "the transaction had failed and was rolled back instead of committed"))
	case rq.op == opCommit || rq.op == opRollback:
		c.tx = false
		payload = encodeDone(rq.tag, rq.affected)
	case rq.cols != nil && !rq.discard:
		payload = encodeRows(rq.colsEnc, rq.nrows, rq.rowbuf, false, rq.tag, rq.affected)
		if len(payload) > d.maxMsg {
			payload = encodeError(clientErr(ClientProtocol, "the last rows are larger than the system's MaxMessageBytes"))
		}
	default:
		payload = encodeDone(rq.tag, rq.affected)
	}
	if rq.op == opCommit || rq.op == opRollback {
		c.tx = false
	}
	c.rq = nil
	switch c.deliver(d, g, rq.replyTo, rq.id, payload, thenFinish) {
	case held:
		return false
	}
	return c.finish(d, g)
}

// finish ends the lease, unless it is a transaction that goes on.
func (c *conn) finish(d *DB, g *gina.Ctx) bool {
	if c.tx && c.txStatus == 'I' {
		c.tx = false // the server says there is none (a COMMIT sent as a statement)
	}
	if c.tx {
		c.phase = cpReady
		if d.cfg.TxTimeout > 0 {
			c.txUntil = g.Now() + uint64(d.cfg.TxTimeout)
			c.timerTx = g.RegisterTimer(d.cfg.TxTimeout, tagTimerTx)
		}
		return false
	}
	if c.txStatus != 'I' { // a statement left a transaction open: undo it before the next user
		c.startReset(d)
		return false
	}
	return c.release(d, g)
}

func (c *conn) startReset(d *DB) {
	c.rq = nil
	c.phase = cpReset
	c.w.reset()
	c.w.query("ROLLBACK")
	c.out(c.w.b)
}

// release gives the connection back to the pool.
func (c *conn) release(d *DB, g *gina.Ctx) bool {
	c.leased, c.owner, c.tx, c.rq, c.blocked = false, 0, false, nil, false
	c.phase = cpReady
	g.CancelTimer(c.timerTx)
	if c.lifeExpired {
		g.SendRaw(c.pool, tagFree, []byte{1, 0})
		c.closing(g)
		return false
	}
	if g.SendRaw(c.pool, tagFree, []byte{0, 0}) != gina.SendOK {
		c.closeFD(g)
		return true
	}
	c.armIdle(d, g)
	return false
}

func (c *conn) armIdle(d *DB, g *gina.Ctx) {
	c.idleSince = g.Now()
	if d.cfg.ConnMaxIdleTime > 0 {
		c.timerIdle = g.RegisterTimer(d.cfg.ConnMaxIdleTime, tagTimerIdle)
	}
}

func (c *conn) onLifetime(d *DB, g *gina.Ctx) bool {
	if g.Now() < c.born+uint64(d.cfg.ConnMaxLifetime) {
		return false
	}
	c.lifeExpired = true
	if c.phase == cpReady && !c.leased {
		c.retire(g)
	}
	return false
}

// retire asks the pool whether this idle connection may go; the pool answers
// tagClose unless it has just given it work. A pool that no longer exists (it
// crashed and restarted) cannot answer: the connection is an orphan, so it goes.
func (c *conn) retire(g *gina.Ctx) {
	if g.SendRaw(c.pool, tagRetire, nil) == gina.SendStaleHandle {
		c.closing(g)
	}
}

// ---- requests ----

// onRun starts the request the pool leased to this connection.
func (c *conn) onRun(d *DB, g *gina.Ctx, m *gina.Message) bool {
	b := g.Data()
	if c.phase != cpReady || c.leased || len(b) < 8 {
		return false // not ours to run; the pool hears of it when we go
	}
	q, err := parseRequest(b[8:])
	owner := gina.Handle(be64(b))
	if q.replyTo == 0 {
		q.replyTo = owner
	}
	c.leased, c.owner = true, owner
	g.CancelTimer(c.timerIdle)
	if err != nil || (q.op != opQuery && q.op != opExec && q.op != opBegin) {
		c.skip(g, q.replyTo, m.Correlation, clientErr(ClientBadRequest, "malformed request"))
		return c.release(d, g)
	}
	return c.start(d, g, &q)
}

// onTx handles a statement or the end of a transaction from its owner.
func (c *conn) onTx(d *DB, g *gina.Ctx, m *gina.Message) bool {
	q, err := parseRequest(g.Data())
	if err != nil {
		c.skip(g, m.Source, m.Correlation, clientErr(ClientBadRequest, "malformed request"))
		return false
	}
	if !c.leased || !c.tx || q.txid != c.txid || m.Source != c.owner {
		c.skip(g, m.Source, q.id, clientErr(ClientTxDone, "the transaction has ended, or is not yours"))
		return false
	}
	if c.rq != nil || c.phase != cpReady || c.held != nil {
		c.skip(g, m.Source, q.id, clientErr(ClientBadRequest, "another request of this transaction is still running"))
		return false
	}
	if q.replyTo == 0 {
		q.replyTo = m.Source
	}
	g.CancelTimer(c.timerTx)
	return c.start(d, g, &q)
}

func (c *conn) skip(g *gina.Ctx, to gina.Handle, id uint32, e *Error) {
	reply(g, to, id, encodeError(e))
}

// start puts a request on the wire.
func (c *conn) start(d *DB, g *gina.Ctx, q *wireRequest) bool {
	rq := &active{id: q.id, op: q.op, replyTo: q.replyTo, batchRows: q.batchRows, discard: q.op != opQuery}
	if rq.batchRows <= 0 {
		rq.batchRows = d.cfg.BatchRows
	}
	if q.timeout > 0 {
		rq.deadline = g.Now() + uint64(q.timeout)
		c.timerQuery = g.RegisterTimer(q.timeout, tagTimerQuery)
	}
	c.w.reset()
	switch q.op {
	case opQuery, opExec:
		if q.op == opExec && len(q.params) == 0 {
			c.w.query(q.sql) // the simple protocol: several statements are fine
		} else if err := c.w.extended(q.sql, q.params); err != nil {
			c.skip(g, q.replyTo, q.id, clientErr(ClientBadRequest, err.Error()))
			return c.finish(d, g)
		}
	case opBegin:
		sql := "BEGIN"
		if q.isolation != IsolationDefault {
			sql += " ISOLATION LEVEL " + isolationNames[q.isolation]
		}
		if q.readOnly {
			sql += " READ ONLY"
		}
		c.w.query(sql)
	case opCommit:
		c.w.query("COMMIT")
	case opRollback:
		c.w.query("ROLLBACK")
	}
	c.rq, c.phase = rq, cpRun
	c.out(c.w.b)
	return false
}

// abort ends a request early: ask the server to cancel it and throw away what it
// still sends. The request then ends with an error of the given kind.
func (c *conn) abort(g *gina.Ctx, d *DB, why ClientError, msg string) {
	rq := c.rq
	if rq == nil || rq.cancelWhy != 0 {
		return
	}
	rq.cancelWhy, rq.cancelMsg, rq.discard = why, msg, true
	rq.rowbuf, rq.nrows = rq.rowbuf[:0], 0
	rq.hard = g.Now() + uint64(d.cfg.CancelGrace)
	if c.blocked {
		c.blocked = false
		g.CancelTimer(c.timerStream)
	}
	if len(c.key) == 0 {
		return // the server gave no key, so it cannot be cancelled; the grace period ends it
	}
	if h, err := g.Spawn(gina.SpawnSpec{Type: d.cfg.TypeID + typeCancellerOffset, Group: gina.GroupNone, Restart: gina.RestartTemporary}); err == gina.SpawnErrNone {
		g.SendRaw(h, tagCancelGo, cancelPacket(c.pid, c.key))
	}
}

// ---- the socket ----

// drive decides what the isolate waits for next: its own output first, then the
// server.
func (c *conn) drive(d *DB, g *gina.Ctx) gina.Effect {
	if c.io != ioNone {
		return c.waitEffect()
	}
	if c.interrupted {
		c.interrupted = false
		return gina.WaitMessage()
	}
	if c.phase == cpTLS && c.tc != nil && c.tc.HandshakeComplete() {
		c.sendStartup(d)
	}
	if c.tc != nil && c.tc.Err() != nil {
		c.die(d, g, clientErr(c.failKind(), "tls: "+c.tc.Err().Error()))
		return gina.Done()
	}
	if out := c.outgoing(); len(out) > 0 {
		c.io = ioSend
		g.IOSend(c.fd, out, writeTimeout)
		return gina.WaitIO()
	}
	switch {
	case c.phase == cpClosing:
		c.closeFD(g)
		return gina.Done()
	case c.wantRead():
		var timeout time.Duration
		switch {
		case c.phase < cpReady:
			if timeout = c.remaining(g, c.hsDeadline); timeout <= 0 {
				c.die(d, g, clientErr(ClientConnect, "timed out"))
				return gina.Done()
			}
		case c.rq != nil && c.rq.hard != 0:
			if timeout = c.remaining(g, c.rq.hard); timeout <= 0 {
				c.die(d, g, clientErr(ClientTimeout, "the server did not react to the cancellation"))
				return gina.Done()
			}
		}
		buf := c.rbuf
		if c.phase == cpSSL {
			buf = buf[:1] // nothing past the answer may be read before TLS starts
		}
		c.io = ioRecv
		g.IORecv(c.fd, buf, timeout)
		return c.waitEffect()
	}
	return gina.WaitMessage()
}

func (c *conn) waitEffect() gina.Effect {
	if c.io == ioRecv && c.phase >= cpReady {
		return gina.WaitIOOrMessage()
	}
	return gina.WaitIO()
}

func (c *conn) wantRead() bool {
	switch c.phase {
	case cpSSL, cpTLS, cpStartup:
		return true
	case cpReady, cpRun, cpReset:
		return !c.blocked && c.held == nil
	}
	return false
}

// closing says goodbye to the server; drive closes the socket once it is sent.
func (c *conn) closing(g *gina.Ctx) {
	if c.fd == 0 || c.phase == cpClosing {
		return
	}
	if c.phase >= cpReady {
		c.w.reset()
		c.w.terminate()
		c.out(c.w.b)
	}
	c.phase = cpClosing
}

func (c *conn) closeFD(g *gina.Ctx) {
	if c.fd != 0 {
		g.CloseFD(c.fd)
		c.fd = 0
	}
}

// die ends the connection after a failure and tells whoever needs to know: the
// caller whose request was on it, and the pool.
func (c *conn) die(d *DB, g *gina.Ctx, e *Error) bool {
	c.closeFD(g)
	if !c.ready {
		g.SendRaw(c.pool, tagFailed, encodeError(e))
		return true
	}
	if rq := c.rq; rq != nil {
		if rq.cancelWhy != 0 {
			e = clientErr(rq.cancelWhy, rq.cancelMsg)
		}
		reply(g, rq.replyTo, rq.id, encodeError(e))
	}
	// The second byte tells the pool that this connection never began the request it
	// was leased (it was idle when the server hung up): nothing has happened yet, so
	// the pool runs that request on another connection.
	unstarted := byte(0)
	if !c.leased {
		unstarted = 1
	}
	g.SendRaw(c.pool, tagFree, []byte{1, unstarted})
	return true
}
