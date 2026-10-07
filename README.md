# Gina

A Go port of [pmbanugo/tina](https://github.com/pmbanugo/tina)'s concurrency model: **thread-per-core shards** (each a goroutine locked to an OS thread, optionally pinned to a core) that own their isolates, memory pool, timers and sockets, and exchange 128-byte messages through **lock-free SPSC rings**. Goroutines and channels are confined to one file (the thread host), the tests that opt in, and `extensions/webpush/transport.go` (see Web Push); everything else is single-threaded per shard. See [SPEC.md](SPEC.md) (the "Implementation status" section lists what exists and what does not).

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

`extensions/tls` is a sans-I/O **TLS 1.3** server written for Gina (the standard `crypto/tls` needs a blocking connection and a goroutine per connection, so it cannot run inside an isolate). It uses stdlib crypto primitives and implements the protocol only. TLS 1.3, AES-GCM, X25519/P-256, ECDSA/Ed25519/RSA-PSS certificates, ALPN, SNI, KeyUpdate. No TLS 1.2, ChaCha20, session resumption or client certificates (see the package comment). It has been tested against Go's TLS client and OpenSSL, but has not had a security audit.

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
wp.Install(&spec)                     // adds the sender isolate to shard 2
sys, _ := gina.NewSystem(spec, gina.Options{})
wp.Start(sys)                         // starts the workers that talk to push services

// in any isolate, on any shard:
sub, _ := webpush.ParseSubscription(jsonFromTheBrowser)
wp.Send(ctx, &webpush.Notification{ID: 7, Sub: sub, Payload: []byte(`{"title":"Hi"}`)})
// later the isolate that sent it gets TagResult: gina.PayloadAs[webpush.Result](m)
// Outcome Delivered, Gone (404/410: delete the subscription), Rejected, Failed, ...
```

- **How it fits.** `Send` is a message to a *sender isolate*, which hands the notification to a small pool of worker goroutines without blocking. A worker encrypts, signs and POSTs it, then reports the outcome back with `System.SendExternal` as a `TagResult` to the isolate that sent it (or `Notification.ReplyTo`). The pool exists because Gina's TLS is server-only and its reactor cannot connect out, so the request to the push service uses `net/http`. That is confined to `extensions/webpush/transport.go`; everything else is plain isolate code.
- **Safe by default.** A subscriber chooses the endpoint, so the sender only connects to `https` endpoints and its dialer refuses loopback, private, link-local and carrier-grade-NAT addresses after name resolution (SSRF). It does not follow redirects. `Config.AllowInsecure` and `AllowPrivate` relax this for development.
- **Behaviour.** VAPID tokens are cached per push service (one signature about every 11 hours). 429, 5xx and network errors are retried (`Config.Retries`, default 2, with backoff and `Retry-After`); 404/410 report `OutcomeGone`; other 4xx report `OutcomeRejected` without retrying. Payloads up to 3993 bytes, optional padding, `TTL`, `Urgency` and `Topic`.
- **Not implemented:** push receipts, the legacy `aesgcm` coding and GCM API keys (every current browser takes `aes128gcm` with VAPID). Delivery is best effort and at most once: a full queue, mailbox or ring drops a notification (a full queue says so in the `Result`), and work still queued when the system stops is discarded. `OutcomeDelivered` means the push service accepted the message, not that the browser showed it.
- **Verified** against the worked example of RFC 8291 Appendix A (byte-exact), an RFC 8292 sample token, a round-trip decrypt, and integration tests on a threaded system under `-race` against a fake push service that decrypts what it receives and checks the VAPID token (outcomes, retries, queue overflow, shutdown with a request in flight, SSRF and redirect refusal); and by running `examples/webpush` as a server against such a fake push service. **No real browser and no real push service (FCM, Mozilla, Apple) has been run against it.**

`go run ./examples/webpush` serves a page with an "Enable notifications" button, the service worker and the subscribe/notify endpoints. Service workers need `localhost` or HTTPS with a certificate the browser trusts.
