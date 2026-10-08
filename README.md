# Gina

A Go port of [pmbanugo/tina](https://github.com/pmbanugo/tina)'s concurrency model: **thread-per-core shards** (each a goroutine locked to an OS thread, optionally pinned to a core) that own their isolates, memory pool, timers and sockets, and exchange 128-byte messages through **lock-free SPSC rings**. Goroutines and channels are confined to one file (the thread host) and the tests that opt in; everything else is single-threaded per shard. See [SPEC.md](SPEC.md) (the "Implementation status" section lists what exists and what does not).

**Why this model?** Two articles by Peter Mbanugo (Tina's author) explains the reasoning:

- [Why async/await complects concurrency](https://pmbanugo.me/blog/why-async-await-complect-concurrency): why shards that own their state and exchange messages are easier to reason about than async/await.
- [Why queues don't fix overload, and what to do instead](https://pmbanugo.me/blog/why-queues-dont-fix-overload-and-what-to-do-instead): why putting a queue in front of a saturated system doesn't help, and what to do instead.

```go
type counter struct{ n int }

sys, _ := gina.NewSystem(gina.SystemSpec{
	Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 16}, nil,
		func(self *counter, ctx *gina.Ctx, m *gina.Message) gina.Effect { self.n++; return gina.WaitMessage() })},
	Shards: make([]gina.ShardSpec, 2),
}, gina.Options{})
h, _ := sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
sys.Send(h, gina.TagUserBase, nil)
sys.RunUntilIdle(10)                     // single-thread cooperative driver: tests and simulation
// or, in production, one OS thread per shard:
//   sys.Run(gina.RunOptions{Pin: true})  // serves until an isolate calls ctx.StopSystem()
```

```
go test ./...                    # engine, supervision, simulator, linter, allocation checks
go test -race ./...              # also exercises the threaded runtime under the race detector
go run ./examples/pingpong       # two shards on two OS threads, 1M cross-thread messages
go run ./examples/supervised     # panics -> restarts -> budget -> Level-2 reset
go run ./examples/httpserver -port 8080 -shards 8 -pin    # HTTP server, 8 shard threads (one per core), SO_REUSEPORT listeners
go run ./examples/httpserver -port 8443 -tls          # HTTPS with a throwaway certificate
go run ./examples/https                           # HTTPS on :8443 + HTTP on :8080 redirecting to it
go run ./examples/shards -shards 4 -pin           # how to set up shards: a token laps a ring of shard threads, HTTP on all of them
go run ./examples/sse -shards 2                   # Server-Sent Events: a clock isolate on its own shard pushes the time to open /events streams
go run ./examples/http2 -port 8443 -tls -shards 4 # HTTP/2 over TLS 1.3 (drop -tls for cleartext h2c)
go run ./examples/websocket -shards 2             # WebSocket chat: the room is an isolate on its own shard (-tls for wss, -h2 for HTTP/2)
go run ./examples/wsworkers                      # two worker isolates and a browser: one pushes the time, the other prints "hello" when a button is pressed
go run ./examples/webpush                       # Web Push: a browser subscribes and the server sends it notifications (http://localhost:8080)
go run ./examples/sql                           # PostgreSQL pool: workers move money in transactions, a big result is streamed, a slow query times out (needs a server, see the example)
go run ./examples/sql -driver sqlite            # the same on a SQLite file: no server (needs cgo)
```

Simulation: `gina.NewSim(spec, seed, cfg)` runs the same engine cooperatively on one thread with a simulated clock, shuffled shard order, fault injection and invariant checks. The same seed always produces the same `Trace.Hash()`. (Threaded runs are not deterministic.)

**Message size.** A message is a fixed 128-byte envelope with 96 bytes of inline payload, but `ctx.SendRaw` accepts more (up to `SystemSpec.MaxMessageBytes`, default 16 MiB): the data is copied into a buffer the engine owns and delivered beside the envelope, on the same shard or across shards, and the receiver reads all of it with `ctx.Data()` (the envelope's `Payload` then holds only the first 96 bytes, and `msg.IsLarge()` tells). To send one buffer to many isolates without copying it for each, build a `gina.Blob` once and `ctx.SendBlob` it to each; nobody may modify a Blob. Typed payloads (`Send[P]`) stay inline and limited to 96 bytes. Delivery rules are unchanged: a full mailbox or ring drops the message (and the buffer is garbage collected).

While a system runs on threads, only isolates may touch it, and handlers run on several threads at once, so state shared between isolates on different shards must be thread-safe (the HTTP server keeps its state per shard). Threads share one Go heap and garbage collector.

## HTTP

`extensions/http` is an HTTP/1.1 framework built on isolates (no `net/http`):

```go
r := ghttp.NewRouter()
r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "Hello, "+c.Param("name")+"\n") })

srv := ghttp.New(ghttp.Config{Port: 8080, ReusePort: true}, r)
spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 1)}
srv.Install(&spec)                       // adds the listener + connection isolates
sys, _ := gina.NewSystem(spec, gina.Options{})
sys.Run(gina.RunOptions{})                // one OS thread per shard; serves until stopped
```

### HTTPS

```go
cert, _ := gtls.SelfSigned("localhost")            // dev only; or gtls.LoadX509KeyPair("cert.pem", "key.pem")
srv := ghttp.New(ghttp.Config{Port: 8443, TLS: &gtls.Config{Certificates: []ctls.Certificate{cert}}}, r)
```

`extensions/tls` is a sans-I/O **TLS 1.3** server and client written for Gina (the standard `crypto/tls` needs a blocking connection and a goroutine per connection, so it cannot run inside an isolate). It uses stdlib crypto primitives and implements the protocol only. TLS 1.3, AES-GCM, X25519/P-256, ECDSA/Ed25519/RSA-PSS certificates, ALPN, SNI, KeyUpdate. No TLS 1.2, ChaCha20, session resumption or client certificates (see the package comment). The client (`tls.NewClient`) offers X25519 and P-256 key shares, checks the server's chain and name with `crypto/x509` against the roots you give it, and does not follow a HelloRetryRequest. Both sides have been tested against Go's `crypto/tls` (and the server against OpenSSL), but have not had a security audit.

Scaling across cores works like Tina's `SO_REUSEPORT` setup: with `Shards: N` each shard thread binds its own listener on the same port (`ReusePort: true`) and the kernel balances connections across them.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="bench/results-dark.png">
  <img src="bench/results.png" alt="Six charts comparing Gina (shard threads in one process) with Go net/http on 1 to 8 cores. Gina is 1.5 to 1.9 times faster on keep-alive GET over HTTP and 1.5 to 1.8 times over HTTPS, completes 1.6 to 3.0 times more TLS handshakes per second, has a 2.8 to 5.6 times lower p99 latency, and is 3.8 times faster on a 64 KiB echo over HTTP and 2.0 times over HTTPS. Gina uses about as much memory as net/http on HTTP (21 and 20 MB at 8 cores) and about four times as much on HTTPS (103 against 25 MB, a figure that varies between about 50 and 100 MB from run to run).">
</picture>

Benchmarks against `net/http` (same cores, 1 to 8): Gina's shard threads are about 1.5-1.9x faster on keep-alive requests with a 2.6-5.8x lower p99, and complete 1.6-3.0x more TLS handshakes. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for the method, microbenchmarks and caveats; reproduce with `bench/run.sh`.

## HTTP/2

`extensions/http2` serves HTTP/2 the same way: a listener isolate per shard, an isolate per connection, no goroutines and no `net/http`. It takes the same `ghttp.Router`, so a handler cannot tell which protocol carried its request, and `c.TLS()` works unchanged (`ALPN` reports `h2`).

```go
srv := http2.New(http2.Config{
	Port:      8443,
	ReusePort: true,                                                   // one listener per shard, as above
	TLS:       &gtls.Config{Certificates: []ctls.Certificate{cert}},   // nil = cleartext h2c
}, r)                                                                  // r is the ghttp.Router from the HTTP section
spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 4)}
srv.Install(&spec)
```

```
curl -k --http2 https://localhost:8443/                  # -k: self-signed certificate
curl --http2-prior-knowledge http://localhost:8080/      # h2c
```

- **With TLS** it speaks h2 over TLS 1.3 and negotiates `h2` through ALPN. **Without TLS** it speaks h2c with prior knowledge (the client sends the HTTP/2 preface first). The `Upgrade: h2c` handshake was removed from the protocol and is not supported.
- **One protocol per port.** A client that offers only `http/1.1` fails the TLS handshake, and a cleartext connection that does not start with the HTTP/2 preface is closed. Serve HTTP/1.1 from `extensions/http` on another port.
- **Implemented:** every frame type, flow control in both directions (per stream and per connection), HPACK with Huffman strings, `SETTINGS`/`PING`/`GOAWAY`/`RST_STREAM`, `CONTINUATION` and padding, request bodies up to `MaxBodyBytes`, concurrent streams (`MaxConcurrentStreams`), and the request validity rules of RFC 9113 §8. Large responses are sent in fair slices across streams as the peer's windows allow.
- **Defences:** header, URI and body limits (431, 414, 413), a concurrent-stream limit, a token bucket on cheap control frames (rapid reset, `PING`/`SETTINGS`/empty-`DATA` floods get `ENHANCE_YOUR_CALM`), and handshake, read, write and idle timeouts.
- **Not implemented:** server push, prioritisation (`PRIORITY` is accepted and ignored), `CONNECT`, trailers (accepted and dropped), and `EventStream` (a handler that calls it gets a 501: one connection carries many streams, so `SendEvent` cannot name one). Responses are buffered, like requests, and response headers are never added to the HPACK table.
- **Verified** against Go's `net/http` HTTP/2 client (TLS and h2c, bodies up to 5 MiB, 64 concurrent streams on one connection, four shard threads, under `-race`), against `curl`/nghttp2 by hand, with an HPACK cross-check against Go's own implementation, and with frame-level tests of flow control, protocol errors and floods. It has **not** been run through h2spec and has had no security audit.

Against `net/http` serving HTTP/2 only, on the same cores (64 connections x 4 streams, loopback), Gina is **3.9-6.7x** faster on keep-alive GET over h2c and TLS, with a 9-17x lower p99, and **4.0x** (h2c) / **2.9x** (TLS) faster on a 64 KiB echo. The 4- and 8-core Gina rows are probably limited by the load generator, so those ratios are lower bounds, and `net/http` is the more complete server. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md#http2); reproduce with `PROTO=h2 bench/run.sh`.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="bench/results-h2-dark.png">
  <img src="bench/results-h2.png" alt="Six charts comparing Gina (shard threads in one process) with Go net/http serving HTTP/2 on 1 to 8 cores. Gina is 3.9 to 6.7 times faster on keep-alive GET over h2c and 4.0 to 6.6 times over h2 with TLS, 4.0 times faster on a 64 KiB echo over h2c and 2.9 times over TLS, and has a 9 to 17 times lower p99 latency. Gina's 4- and 8-core points are probably limited by the load generator. At 8 cores Gina uses 54 MB against 22 MB over h2c and 83 MB against 22 MB over TLS.">
</picture>

## Operating a server

- **Client address.** `ctx.PeerAddr(fd)` and `Context.RemoteAddr()` return the TCP peer, recorded at accept (no syscall). Behind a proxy that is the proxy: read `X-Forwarded-For` yourself.
- **IPv6.** `ListenSpec.IP` / `Config.IP`; `::` is dual-stack and IPv4 clients are reported as IPv4.
- **Talking to the system from outside.** `sys.SendExternal(handle, tag, payload)` (and `SendExternalTo`) is safe from any goroutine while the system runs. It queues the message on the target shard and wakes it; the result says whether it was queued (it can still be dropped at the mailbox, counted in `Stats().Dropped`).
- **Certificates.** `tlsCfg.SetCertificates(...)` swaps certificates for new handshakes without a restart, and `TLS.GetCertificate` picks one per ClientHello (return a `*gtls.Certificate` made once with `NewCertificate`). List `gtls.ACMETLS1` in `NextProtos` and answer it with `gtls.ALPNChallengeCertificate` to pass TLS-ALPN-01; the HTTP servers close such connections after the handshake.
- **One port for h2 and HTTP/1.1.** `http2.Config{HTTP1Fallback: true}`: ALPN `h2` gets HTTP/2; `http/1.1` or no ALPN gets HTTP/1.1 (without TLS the HTTP/2 preface decides).
- **Bodies.** Chunked request bodies are decoded. `r.POST(...).MaxBody(n).ReadTimeout(d)` sets limits per route (the server default stays small). `.StreamBody()` delivers the body to `c.OnBody(func(c, chunk, last))` as it arrives, in constant memory, with `c.StopBody` to refuse early and `c.BodyAborted()` when the client leaves. `c.SendReader(code, type, size, r)` and `c.ServeFile/ServeFS` stream a response in 32 KiB pieces (known length, or chunked / END_STREAM when unknown), with `Range`/`If-Range` support (`c.ServeBytes` for in-memory bodies). Reads run on the shard thread: fine for the page cache, not for slow sources. Works on HTTP/1.1 and HTTP/2.
- **Slow subscribers.** `ctx.TakeLost()` counts messages dropped for an isolate. A WebSocket connection that missed pushes (or whose unsent output hit `MaxQueued`) is closed with 1013, or `Config.OnOverflow` decides, e.g. `c.Close(CloseTryAgainLater, `{"reconnect":true}`)`.

## WebSocket

`extensions/websocket` serves WebSocket (RFC 6455) over everything above: **ws and wss over HTTP/1.1**, and **ws and wss over HTTP/2** (RFC 8441, one stream of a shared connection per WebSocket). It is a protocol on top of the HTTP servers, not a server of its own: a route hands the request to an `Endpoint`, and the same `Endpoint` and the same callbacks serve all four ways in.

```go
ep := websocket.New(websocket.Config{
	OnOpen:    func(c *websocket.Conn) { c.SendText("welcome") },
	OnMessage: func(c *websocket.Conn, op websocket.Opcode, data []byte) { c.Send(op, data) }, // data is valid during the call
	OnClose:   func(c *websocket.Conn, code uint16, reason []byte) {},
})
r := ghttp.NewRouter()
r.GET("/ws", ep.Serve)                      // the route is the same for HTTP/1.1 and HTTP/2

srv := ghttp.New(ghttp.Config{Port: 8443, TLS: tlsCfg, ConnMailbox: websocket.MailboxCapacity}, r)       // ws/wss over HTTP/1.1
// or: http2.New(http2.Config{Port: 8443, TLS: tlsCfg, ExtendedConnect: true, ConnMailbox: ...}, r)      // over HTTP/2
```

- **Transports.** Over HTTP/1.1 the handshake is the Upgrade of RFC 6455 and `wss` is the server's TLS config. Over HTTP/2 set `http2.Config.ExtendedConnect`: the server then advertises `SETTINGS_ENABLE_CONNECT_PROTOCOL`, a CONNECT with `:protocol: websocket` reaches the same route as a `GET` (`Request.Protocol` is set), and the stream becomes the WebSocket, with HTTP/2 flow control, while other streams of the connection carry on. Browsers use this for `wss://` when the server offers it. As ever with the HTTP/2 server, a port speaks one protocol: browsers fall back to HTTP/1.1 only if you serve it on another port.
- **Server push from other isolates.** A connection is an isolate, and it waits for its peer *and* for messages (the engine's `WaitIOOrMessage`). Any isolate on any shard can send to a connection's `Peer` with `websocket.Push` / `PushText` / `PushClose`: that is how `examples/websocket` makes a chat room out of one isolate on its own shard that knows nothing about HTTP. A push can be any size (Gina messages are no longer limited to 96 bytes, see below), and a broadcast builds its message once with `NewShared` and gives every connection the same one with `PushShared`. Pushes are best effort: a full mailbox or a gone connection drops them.
- **What it checks.** Strict frame validation (masking, reserved bits, opcodes, control-frame limits), UTF-8 of text messages and close reasons *incrementally* (a bad byte is refused as it arrives), message size (`MaxMessageSize`, default 1 MiB) and queued-output limits, a keep-alive ping with a pong deadline, a closing-handshake timeout, a same-origin `CheckOrigin` by default (cross-site hijacking by browsers), and a panic in a callback closes that one connection with 1011.
- **Not implemented:** extensions (permessage-deflate is not negotiated; clients that offer it are answered without it) and sending fragmented messages.
- **Verified** with a frame-parser suite (random split points, every protocol error) and integration tests that run each scenario over ws, wss, h2c and h2 over TLS, under `-race`; by hand against `gorilla/websocket`, `coder/websocket`, `golang.org/x/net/http2`'s extended-CONNECT client and `aiohttp` (see [SPEC.md](SPEC.md#verification-and-what-is-not-verified)). It has **not** been run through the Autobahn suite, tried in a browser, or security-audited.

## Web Push

`extensions/webpush` sends [Web Push](https://www.rfc-editor.org/rfc/rfc8030) messages: the notifications a browser shows from its service worker even when your page is closed. The browser subscribes through its vendor's push service (FCM, Mozilla, Apple) and gives the page an endpoint URL and two keys; the page sends those to you; to notify, the server encrypts the message for that subscription ([RFC 8291](https://www.rfc-editor.org/rfc/rfc8291)), signs a VAPID token that identifies it ([RFC 8292](https://www.rfc-editor.org/rfc/rfc8292)) and POSTs both to the endpoint.

```go
vapid, _ := webpush.GenerateVAPID()   // once: keep vapid.PrivateKey(), give pages vapid.PublicKey()
wp, _ := webpush.New(webpush.Config{VAPID: vapid, Subject: "mailto:ops@example.com", Shard: 2})
wp.Install(&spec)                     // adds the sender and resolver isolates to shard 2
sys, _ := gina.NewSystem(spec, gina.Options{})

// in any isolate, on any shard:
sub, _ := webpush.ParseSubscription(jsonFromTheBrowser)
wp.Send(ctx, &webpush.Notification{ID: 7, Sub: sub, Payload: []byte(`{"title":"Hi"}`)})
// later the isolate that sent it gets TagResult: gina.PayloadAs[webpush.Result](m)
// Outcome Delivered, Gone (404/410: delete the subscription), Rejected, Failed, ...
```

- **How it fits.** `Send` is a message to a *sender isolate*, which starts a *delivery isolate* for the notification (up to `Config.Workers` at once; the rest wait in a queue). The delivery encrypts, asks a *resolver isolate* for the push service's addresses (a DNS cache; a *lookup isolate* per unknown name speaks UDP to the nameservers), connects with `Ctx.Dial`, runs the TLS 1.3 client handshake, POSTs over HTTP/1.1 and reports a `TagResult` to the isolate that sent the notification (or `Notification.ReplyTo`). No goroutines, channels or locks are involved: it is all isolates on one shard, over Gina's sockets. Each notification uses its own connection, so there is no connection reuse and no HTTP/2.
- **Safe by default.** A subscriber chooses the endpoint, so the sender only connects to `https` endpoints and refuses loopback, private, link-local and carrier-grade-NAT addresses after name resolution (SSRF); the check is on the addresses it is about to connect to. It does not follow redirects. `Config.AllowInsecure` and `AllowPrivate` relax this for development.
- **Behaviour.** VAPID tokens are cached per push service (one signature about every 11 hours). 429, 5xx and network errors are retried (`Config.Retries`, default 2, with backoff and `Retry-After`); 404/410 report `OutcomeGone`; other 4xx report `OutcomeRejected` without retrying. Payloads up to 3993 bytes, optional padding, `TTL`, `Urgency` and `Topic`.
- **Not implemented:** push receipts, the legacy `aesgcm` coding and GCM API keys (every current browser takes `aes128gcm` with VAPID). Delivery is best effort and at most once: a full queue, mailbox or ring drops a notification (a full queue says so in the `Result`), and work still queued when the system stops is discarded. `OutcomeDelivered` means the push service accepted the message, not that the browser showed it.
- **Verified** against the worked example of RFC 8291 Appendix A (byte-exact), an RFC 8292 sample token, a round-trip decrypt, and integration tests on a threaded system under `-race` against a fake push service (plain and TLS) that decrypts what it receives and checks the VAPID token, with a fake DNS server (outcomes, retries, `Retry-After`, timeouts, queue overflow, shutdown with a request in flight, name resolution, caching and merging of lookups, server fallback, address fallback, SSRF through DNS, certificate and name checks, malformed responses, redirect refusal). Also run once, by hand, against the real FCM, Mozilla autopush and Apple endpoints with made-up subscriptions (real DNS, system roots, TLS 1.3): each answered with a sensible HTTP status (410, 404, 400). **No real browser has received a notification from it, and no real subscription has been delivered to.**

`go run ./examples/webpush` serves a page with an "Enable notifications" button, the service worker and the subscribe/notify endpoints. Service workers need `localhost` or HTTPS with a certificate the browser trusts.

## SQL

`extensions/sql` is a PostgreSQL and SQLite client with the shape of `database/sql` (a pool of connections, requests, transactions, rows) built from isolates. `database/sql` cannot run inside an isolate: its drivers block on a socket, and the package starts goroutines to open connections, to watch every `Rows` and `Tx` for context cancellation, and to reset connections. Each of those jobs is an isolate here, speaking the PostgreSQL protocol over Gina's sockets. `Config.Driver` picks the database: `sql.DriverPostgres` (the default) or `sql.DriverSQLite`; the pool, requests, replies and everything below are the same for both.

```go
db, _ := sql.New(sql.Config{Addr: netip.MustParseAddrPort("127.0.0.1:5432"), User: "app", Password: "...", Database: "app", Shard: 1, MaxOpenConns: 8})
db.Install(&spec)                     // adds the pool isolate to shard 1; connections are opened on demand
sys, _ := gina.NewSystem(spec, gina.Options{})

// in any isolate, on any shard:
db.Query(g, &sql.Request{ID: 1, SQL: "SELECT id, name FROM users WHERE age > $1", Args: []any{30}, Timeout: time.Second})

// later, in the same isolate's handler:
case sql.TagReply:
	rep, _ := sql.Decode(g, m)               // rep.ID is the Request.ID
	switch rep.Kind {
	case sql.ReplyRows:                      // one batch of rows
		for rep.Rows.Next() { var id int64; var name string; rep.Rows.Scan(&id, &name) }
		if rep.More { rep.Continue(g) }      // the connection waits until you ask for the next batch
	case sql.ReplyDone:                      // rep.RowsAffected, rep.Tag
	case sql.ReplyError:                     // rep.Err is a *sql.Error: errors.Is(err, sql.ErrTimeout), errors.As for the SQLSTATE
	}

// a transaction owns a connection until it ends:
db.Begin(g, &sql.Request{ID: 2})             // -> ReplyTx; rep.Tx.Query / Exec / Commit / Rollback
```

- **How it fits.** The *pool isolate* takes requests as messages, gives each to an idle *connection isolate* or queues it (`Config.Queue`, `QueueTimeout`) and opens connections on demand up to `MaxOpenConns`; `MaxIdleConns`, `ConnMaxLifetime` and `ConnMaxIdleTime` work as in `database/sql`. A connection isolate owns the socket, logs in (cleartext, MD5 or SCRAM-SHA-256; optional TLS 1.3 through `extensions/tls`), runs one request at a time and sends the replies straight to the caller. A short-lived *canceller* isolate sends the protocol's `CancelRequest` on a connection of its own, for `Request.Timeout`, `Cancel` and abandoned streams. All of them live on one shard (`Config.Shard`); callers can be anywhere. No goroutines, channels or locks.
- **Asynchronous by construction.** A query is a message and its answer is a message, so `Query` returns at once and any number of requests may be in flight. Each ends with exactly one terminal reply (`ReplyDone`, `ReplyError`, a `ReplyRows` with `More` false, or `ReplyTx`).
- **Streaming with backpressure.** Rows arrive in batches (`Request.BatchRows`, about 64 KiB at most). After a batch with `More` set the connection stops reading from the server until the consumer calls `Reply.Continue`, so a slow consumer slows the query through TCP instead of filling memory. A consumer that disappears is noticed after `Config.StreamTimeout`, and the query is cancelled.
- **Transactions.** `Begin` leases a connection to the calling isolate; only that isolate can use the `Tx` (one request at a time). A transaction idle for `Config.TxTimeout` is rolled back, and a connection whose lease ends inside an open transaction (a statement like `BEGIN; ...`) is rolled back before reuse.
- **Failure.** A connection that the server drops is forgotten by the pool, and a request that had been handed to a connection which died before starting it runs on another. A request that was on the wire when it died gets `ErrConnLost`. A failed connection attempt answers the oldest queued request with `ErrConnect`. A server that ignores a cancellation is dropped after `Config.CancelGrace`.
- **Types.** Arguments: nil, bool, integers, floats, string, `[]byte` (bytea), `time.Time`, pointers, named types and `driver.Valuer` (so `database/sql`'s `NullString` and friends). `Rows.Scan` converts the text format into `*string`, `*[]byte`, integers, floats, `*bool`, `*time.Time`, `*any`, pointers (nil for NULL) and anything with a `Scan(any) error` method.
- **SQLite.** `sql.Config{Driver: sql.DriverSQLite, Path: "app.db", Params: map[string]string{"journal_mode": "WAL"}}` opens the file with [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) (a **cgo** dependency, the module's only one; built with `CGO_ENABLED=0` the package still compiles, PostgreSQL works, and a SQLite pool answers `ErrConnect`). SQLite is a library, not a server, so a connection isolate has no socket: it **runs each statement on the shard's thread, inside the turn that receives it.** That has consequences. A slow statement, or one waiting for a lock, stops every isolate on the shard (`Config.Shard` can give the database a shard of its own); `Request.Timeout` and `Cancel` cannot interrupt a running statement, only act between batches of a streamed result. Connections of one pool run one after the other on one thread, so the default is **one connection** and `busy_timeout` is 0 (a locked database is `SQLITE_BUSY` at once, never a wait that nothing could end); use WAL when you raise `MaxOpenConns`. Everything else is shared: the queue, streaming with `Continue` (the statement stays open between batches), transactions with owner checks and `TxTimeout`, `ConnMaxLifetime`/`ConnMaxIdleTime`, a leaked transaction rolled back before reuse. `Config.Params` are PRAGMAs run when a connection opens; `:memory:` pools are limited to one connection that never expires. Placeholders are `$1`… (rewritten to `?1`) or SQLite's own; arguments are bound as the Go type they are. Results come as text with PostgreSQL OIDs, so `Scan` works unchanged: the OID is taken from the declared column type, or from the first non-NULL value; `BOOLEAN`, `DATE`, `DATETIME` and `TIMESTAMP` columns scan into `bool` and `time.Time`. Errors are `*sql.Error` with `Code` the SQLite result name (`SQLITE_CONSTRAINT_UNIQUE`, `SQLITE_BUSY`, …). Differences: transactions are always serializable (`Isolation` is accepted and ignored; `ReadOnly` is enforced with `PRAGMA query_only`), a multi-statement `Exec` reports the last statement's row count, and there is no `LastInsertId` (use `RETURNING` or `SELECT last_insert_rowid()`).
- **Not implemented:** names (the address is an IP: Gina does no name resolution, resolve at start-up), Unix sockets, COPY (answered with `ErrUnsupported`, and the connection stays usable), LISTEN/NOTIFY, a prepared-statement cache (every request is one Parse/Bind/Execute round trip on the unnamed statement), binary result formats, channel binding (SCRAM-SHA-256-PLUS), GSS/Kerberos, TLS before PostgreSQL's `SSLRequest` ("direct" TLS), and any database other than PostgreSQL and SQLite (the PostgreSQL items apply to PostgreSQL only). Session state (`SET`, temporary tables) stays on the pooled connection that was used: set defaults with `Config.Params`. Replies are ordinary Gina messages: a caller on *another shard* whose mailbox is full loses them (`Ctx.TakeLost`), so size the mailbox for the requests in flight; same-shard replies that find it full are retried for about a second. `ghttp` handlers cannot wait for a reply yet, so the example drives the database from plain isolates.
- **Verified** against PostgreSQL 17 (SCRAM-SHA-256, and TLS 1.3 against its OpenSSL: types, errors with SQLSTATE, multi-statement scripts, streaming and backpressure, cancellation and timeouts, transactions and their timeout, pool limits, queueing, idle/lifetime expiry, a backend killed while idle and while running, 3 MiB values), against the RFC 7677 SCRAM exchange, and against a fake server (MD5, cleartext, a SCRAM impostor, TLS verification, hang-ups mid-result, protocol violations, COPY, ignored cancels, 1,500 requests while connections are dropped every 2 ms, replies to a busy caller), all under `-race`. The PostgreSQL integration tests need a server and skip themselves without one (`extensions/sql/harness_test.go` says how to start it). The SQLite driver has its own tests, which need only cgo (types and OIDs, errors with result codes, streaming and backpressure, cancellation, timeouts, transactions, read-only transactions, pool queueing, `SQLITE_BUSY` between two connections, WAL, idle/lifetime expiry, persistence, 3 MiB values), also under `-race`; it has not been run against a real workload. **Not tested** against other PostgreSQL versions, PgBouncer or other poolers, or a real workload, and not security-audited.

`go run ./examples/sql` runs 8 workers that move money between accounts in transactions and checks that the total did not change, then streams 200,000 rows with `Continue` and shows a timeout. `-driver sqlite` does the same on a SQLite file.
