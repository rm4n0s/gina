// Package sql is a PostgreSQL and SQLite client for a Gina system with the shape of
// database/sql (a pool of connections, statements, transactions, rows) built from
// isolates instead of goroutines.
//
// database/sql cannot run inside an isolate: its drivers block on a socket, and the
// package itself starts goroutines to open connections, to watch every Rows and Tx
// for context cancellation, and to reset connections. Here each of those jobs is an
// isolate, and the socket I/O is Gina's own:
//
//   - pool (boot isolate, replaces DB, connectionOpener and the idle/lifetime
//     bookkeeping): takes requests, hands them to a free connection, opens
//     connections on demand up to Config.MaxOpenConns, queues what does not fit
//     (Config.Queue, Config.QueueTimeout) and keeps Config.MaxIdleConns idle.
//   - conn (one per connection, replaces the driver's connection and the resetter
//     goroutine): owns the socket, speaks the PostgreSQL protocol (startup, TLS 1.3
//     through extensions/tls, MD5/cleartext/SCRAM-SHA-256 login, the extended query
//     protocol), streams rows to the caller, enforces the request timeout, expires
//     itself (Config.ConnMaxLifetime, Config.ConnMaxIdleTime) and rolls back a
//     transaction its owner forgot.
//   - canceller (short-lived, replaces the goroutine that watches a context): sends
//     the protocol's CancelRequest on a connection of its own, for Request.Timeout,
//     Cancel and abandoned streams.
//
// No goroutines, channels or locks are involved: it is all isolates on the shard
// named by Config.Shard. Isolates on any shard talk to the pool by message.
//
// # SQLite
//
// Config.Driver chooses the database: DriverPostgres (the default) or DriverSQLite.
// The pool, the requests, the replies, streaming, transactions and every timeout are
// the same; only the connection isolate differs. A SQLite connection has no socket:
// it opens the database file with github.com/mattn/go-sqlite3 (which needs cgo;
// without it a SQLite pool fails to connect) and runs each statement on the shard
// thread, inside its own turn. See sqlite.go for what that means in practice.
//
//	db, _ := sql.New(sql.Config{Addr: netip.MustParseAddrPort("127.0.0.1:5432"), User: "app", Password: "...", Database: "app", Shard: 1})
//	db.Install(&spec)
//	...
//	db.Query(g, &sql.Request{ID: 1, SQL: "SELECT id, name FROM users WHERE age > $1", Args: []any{30}})
//	// later, in the same isolate's handler:
//	case sql.TagReply:
//		rep, _ := sql.Decode(g, m)
//		switch rep.Kind {
//		case sql.ReplyRows:
//			for rep.Rows.Next() { var id int64; var name string; rep.Rows.Scan(&id, &name) }
//			if rep.More { rep.Continue(g) }
//		case sql.ReplyError: // rep.Err
//		}
//
// # Asynchronous by construction
//
// An isolate cannot block, so a query is a message and its answer is a message:
// Query returns at once, and TagReply arrives later, tagged with the Request.ID.
// Any number of requests may be in flight; each ends with exactly one terminal
// reply (ReplyDone, ReplyError, a ReplyRows with More false, or ReplyTx).
//
// Rows are streamed in batches (Request.BatchRows, about 64 KiB at most). After a
// batch with More set the connection stops reading from the server until the
// consumer calls Reply.Continue, so a slow consumer applies backpressure all the way
// to the server instead of buffering the result. A consumer that goes away is noticed
// after Config.StreamTimeout, and the query is cancelled.
//
// # Transactions
//
// Begin leases a connection to the calling isolate and answers with a Tx, whose
// Query, Exec, Commit and Rollback go straight to that connection. Only the isolate
// that began it can use it, one request at a time. A transaction that stays idle for
// Config.TxTimeout, or whose connection fails, ends: later requests on it get
// ErrTxDone.
//
// # Limits
//
// The address is an IP address: Gina does no name resolution, so resolve a host name
// at start-up (net.LookupIP is fine outside handlers). Text-format results and
// parameters only; the unnamed prepared statement only (every request is one Parse,
// Bind, Execute round trip, with no statement cache); no COPY, no LISTEN/NOTIFY, no
// channel binding (SCRAM-SHA-256-PLUS), no Unix sockets, no GSS/Kerberos login.
// Session state (SET, temporary tables, advisory locks) stays on the pooled
// connection that was used: set per-session defaults with Config.Params. A result
// message (a row, or a batch of rows) cannot exceed the system's MaxMessageBytes.
//
// Replies are ordinary Gina messages, with Gina's rules: a caller on another shard
// whose mailbox is full loses them (Ctx.TakeLost counts), so size the mailbox for
// the requests in flight, one reply each at a time. A reply that finds a same-shard
// caller's mailbox full is retried for about a second instead. A caller that must not
// wait forever for an answer keeps a timer of its own.
package sql

import (
	"errors"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/rm4n0s/gina"
	gtls "github.com/rm4n0s/gina/extensions/tls"
)

// The isolate types a DB registers, as offsets from Config.TypeID.
const (
	typePoolOffset      = 0 // boot isolate
	typeConnOffset      = 1 // one per connection
	typeCancellerOffset = 2 // one per cancel request in flight
	typeCount           = 3
)

// Driver is the database a DB talks to.
type Driver uint8

const (
	// DriverPostgres speaks the PostgreSQL protocol to Config.Addr. It is the zero
	// value.
	DriverPostgres Driver = iota
	// DriverSQLite opens the database file Config.Path in the process.
	DriverSQLite
)

func (d Driver) String() string {
	switch d {
	case DriverPostgres:
		return "postgres"
	case DriverSQLite:
		return "sqlite"
	}
	return "unknown"
}

// Config configures a DB. Zero values take the documented defaults.
type Config struct {
	// Driver is DriverPostgres (the default) or DriverSQLite. The settings below
	// that name a driver apply to that driver only and are ignored by the other.
	Driver Driver

	// Path is the SQLite database: a file name, "file:name?mode=ro" style URI, or
	// ":memory:". Required for DriverSQLite. Every connection of the pool opens it
	// anew, and each ":memory:" connection would get a database of its own, so a
	// ":memory:" pool is limited to one connection; use
	// "file:name?mode=memory&cache=shared" to share one in-memory database between
	// several.
	Path string

	// Addr is the PostgreSQL server: an IP address and port. Required for
	// DriverPostgres.
	Addr netip.AddrPort
	// User is required. Password is used if the server asks for one (cleartext, MD5
	// or SCRAM-SHA-256). Database defaults to User.
	User     string
	Password string
	Database string
	// ApplicationName shows up in pg_stat_activity (default "gina").
	ApplicationName string
	// Params are further start-up parameters sent to the PostgreSQL server
	// (search_path, statement_timeout, TimeZone, ...). They apply to every
	// connection. For SQLite they are PRAGMAs run when a connection opens
	// (journal_mode: "WAL", foreign_keys: "ON", busy_timeout: "2000", ...): names are
	// letters and underscores, values letters, digits and "_.-".
	Params map[string]string
	// TLS, if set, makes every connection start TLS 1.3 (SSLRequest) and verify the
	// server's certificate against TLS.RootCAs for the name TLS.ServerName. There is
	// no fallback to plain text: a server that refuses fails the connection. Nil
	// means no TLS, and a password goes in the clear if the server asks for it.
	TLS *gtls.ClientConfig

	// Shard is the shard that hosts the pool and everything it starts (default 0).
	// It must exist when Install is called.
	Shard int
	// TypeID is the first of three consecutive isolate type ids (default 230).
	TypeID gina.TypeID
	// Mailbox is the pool isolate's mailbox capacity (default 1024): requests that
	// arrive between two of its turns wait there.
	Mailbox int

	// MaxOpenConns limits connections, in use and idle (default 10 for PostgreSQL, 1
	// for SQLite, whose connections share the shard's one thread and the file's one
	// writer, so more of them buy little: see sqlite.go). Each is an isolate with a
	// socket; the shard needs about 2*MaxOpenConns sockets and
	// 6*MaxOpenConns timers, and Install raises SystemSpec.MaxFDs and TimerEntries
	// if they are lower.
	MaxOpenConns int
	// MaxIdleConns is how many idle connections the pool keeps (default
	// MaxOpenConns; negative: none, a connection is closed as soon as it is free).
	// Unlike database/sql's default of 2, an idle connection costs a socket and
	// nothing else, and reconnecting costs a login.
	MaxIdleConns int
	// ConnMaxLifetime closes a connection this long after it was opened, once it is
	// free (default: no limit). ConnMaxIdleTime closes one that sat idle this long
	// (default: no limit).
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// Queue is how many requests may wait for a connection (default 1024); more get
	// ErrOverloaded. QueueTimeout is how long one may wait (default 30s; negative:
	// forever), after which it gets ErrTimeout.
	Queue        int
	QueueTimeout time.Duration
	// ConnectTimeout covers one connection attempt: connect, TLS, login (default 10s).
	// A failed attempt answers the oldest queued request with ErrConnect.
	ConnectTimeout time.Duration
	// StreamTimeout is how long a connection waits for Reply.Continue before it
	// cancels the query (default 30s). TxTimeout is how long a transaction may sit
	// between statements before it is rolled back (default 60s; negative: forever).
	StreamTimeout time.Duration
	TxTimeout     time.Duration
	// BatchRows is the default number of rows per ReplyRows (default 128).
	BatchRows int
	// CancelGrace is how long a connection gives the server to react to a
	// cancellation before it drops the connection (default 5s).
	CancelGrace time.Duration
}

func (c *Config) defaults() error {
	switch c.Driver {
	case DriverPostgres:
		if !c.Addr.IsValid() || c.Addr.Port() == 0 {
			return errors.New("sql: Config.Addr is required (an IP address and port; resolve names yourself)")
		}
		if c.User == "" {
			return errors.New("sql: Config.User is required")
		}
		if c.Database == "" {
			c.Database = c.User
		}
		if c.ApplicationName == "" {
			c.ApplicationName = "gina"
		}
		if c.TLS != nil && (c.TLS.ServerName == "" || c.TLS.RootCAs == nil) {
			return errors.New("sql: Config.TLS needs ServerName and RootCAs")
		}
	case DriverSQLite:
		if err := c.sqliteDefaults(); err != nil {
			return err
		}
	default:
		return errors.New("sql: unknown Config.Driver")
	}
	if c.TypeID == 0 {
		c.TypeID = 230
	}
	if int(c.TypeID)+typeCount > 254 {
		return errors.New("sql: Config.TypeID leaves no room for three isolate types")
	}
	if c.Mailbox == 0 {
		c.Mailbox = 1024
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 10
		if c.Driver == DriverSQLite {
			c.MaxOpenConns = 1
		}
	}
	if c.MaxIdleConns == 0 || c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	} else if c.MaxIdleConns < 0 {
		c.MaxIdleConns = 0
	}
	if c.Queue == 0 {
		c.Queue = 1024
	}
	if c.QueueTimeout == 0 {
		c.QueueTimeout = 30 * time.Second
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.StreamTimeout == 0 {
		c.StreamTimeout = 30 * time.Second
	}
	if c.TxTimeout == 0 {
		c.TxTimeout = time.Minute
	}
	if c.BatchRows == 0 {
		c.BatchRows = 128
	}
	if c.CancelGrace == 0 {
		c.CancelGrace = 5 * time.Second
	}
	if c.Shard < 0 || c.MaxOpenConns < 0 || c.Queue < 0 || c.Mailbox < 0 || c.BatchRows < 0 || c.ConnMaxLifetime < 0 || c.ConnMaxIdleTime < 0 ||
		c.ConnectTimeout < 0 || c.StreamTimeout < 0 || c.CancelGrace < 0 {
		return errors.New("sql: negative size or duration in Config")
	}
	return nil
}

// DB is a pool of PostgreSQL or SQLite connections: a set of isolates on one shard.
type DB struct {
	cfg    Config
	maxMsg int
	pool   atomic.Uint64 // the pool's live handle: requests come from every shard

	open, idle, inUse, waiting atomic.Int64
	waitCount, timedOut        atomic.Uint64
	opened, failed, closed     atomic.Uint64
	requeued, redelivered      atomic.Uint64
}

// New makes a DB. Install it into a SystemSpec.
func New(cfg Config) (*DB, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	d := &DB{cfg: cfg, maxMsg: 16 << 20}
	// Until the isolate starts and says, point at where it will be.
	d.pool.Store(uint64(gina.MakeHandle(uint8(cfg.Shard), cfg.TypeID+typePoolOffset, 0, 1)))
	return d, nil
}

// Install adds the isolate types to spec and starts the pool on Config.Shard.
// Create the shards first. Connections are opened when the first request needs one.
func (d *DB) Install(spec *gina.SystemSpec) error {
	c := &d.cfg
	if c.Shard >= len(spec.Shards) {
		return errors.New("sql: Config.Shard is not a shard of the spec")
	}
	if spec.MaxMessageBytes > 0 {
		d.maxMsg = spec.MaxMessageBytes
	}
	n := c.MaxOpenConns
	spec.Types = append(spec.Types,
		gina.RegisterType(c.TypeID+typePoolOffset, gina.TypeOptions{SlotCount: 1, MailboxCapacity: c.Mailbox}, d.poolInit, d.poolHandler))
	if c.Driver == DriverSQLite {
		// No socket to cancel over: the third type id stays unused.
		spec.Types = append(spec.Types,
			gina.RegisterType(c.TypeID+typeConnOffset, gina.TypeOptions{SlotCount: n, MailboxCapacity: 16}, nil, d.sqliteHandler))
	} else {
		spec.Types = append(spec.Types,
			gina.RegisterType(c.TypeID+typeConnOffset, gina.TypeOptions{SlotCount: n, MailboxCapacity: 16}, nil, d.connHandler),
			gina.RegisterType(c.TypeID+typeCancellerOffset, gina.TypeOptions{SlotCount: n, MailboxCapacity: 4}, nil, d.cancellerHandler))
	}
	if spec.TimerEntries == 0 {
		spec.TimerEntries = 1024
	}
	if spec.MaxFDs == 0 {
		spec.MaxFDs = 4096
	}
	spec.PoolSlots = max(spec.PoolSlots, 2*c.Mailbox, 4096)
	spec.TimerEntries = max(spec.TimerEntries, 6*n+64)
	spec.MaxFDs = max(spec.MaxFDs, 2*n+16)
	spec.Shards[c.Shard].Boot = append(spec.Shards[c.Shard].Boot,
		gina.SpawnSpec{Type: c.TypeID + typePoolOffset, Group: gina.GroupRoot, Restart: gina.RestartPermanent})
	return nil
}

// Pool is the handle of the pool isolate, the target of Query, Exec, Begin and
// Cancel. It follows the isolate through restarts once the system has started.
func (d *DB) Pool() gina.Handle { return gina.Handle(d.pool.Load()) }

func (d *DB) connType() gina.TypeID { return d.cfg.TypeID + typeConnOffset }

// Stats is a snapshot of the pool, like database/sql's DBStats.
type Stats struct {
	Open    int // connections, in use and idle
	InUse   int // connections leased to a request or a transaction
	Idle    int
	Waiting int // requests queued for a connection

	WaitCount uint64 // requests that had to queue
	TimedOut  uint64 // requests that gave up in the queue
	Opened    uint64 // connections made
	Failed    uint64 // connection attempts that failed
	Closed    uint64 // connections closed

	// Requeued counts requests that were handed to a connection which had died
	// without starting them, and ran on another. Redelivered counts replies that found
	// the caller's mailbox full and were sent again.
	Requeued    uint64
	Redelivered uint64
}

// Stats can be called from anywhere.
func (d *DB) Stats() Stats {
	return Stats{
		Open: int(d.open.Load()), InUse: int(d.inUse.Load()), Idle: int(d.idle.Load()), Waiting: int(d.waiting.Load()),
		WaitCount: d.waitCount.Load(), TimedOut: d.timedOut.Load(),
		Opened: d.opened.Load(), Failed: d.failed.Load(), Closed: d.closed.Load(),
		Requeued: d.requeued.Load(), Redelivered: d.redelivered.Load(),
	}
}
