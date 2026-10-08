# Gina — Specification for a Go Port of Tina

Status: draft v0.4 (design, plus an implementation-status snapshot below) · Date: 2026-10-07 · Upstream: https://github.com/pmbanugo/tina (Odin, Apache-2.0)

"Gina" is the name of the Go port; the Go module path is the placeholder `gina`.

## 0. Basis and caveats

This spec is derived from upstream's README, `docs/concepts/*`, `docs/reference/{the_ctx_api,system_spec}.md`, and the type declarations in `src/api.odin`. I did not read the full source. Where upstream is silent I mark the item **(ours)**: a decision this port makes and is free to change. Names follow upstream where known, in Go style (`ctx_send_raw` → `gina.SendRaw`).

Upstream is Apache-2.0. Reimplementing from documented behaviour is fine. Copying or transliterating source requires keeping the license and NOTICE and marking changes.

## Implementation status (2026-10-07)

Gina is a working Tina-style runtime in Go, plus HTTP/1.1 and HTTP/2 servers, a TLS 1.3 server and client, a WebSocket server and a Web Push sender built on it. It is about 15,300 lines of Go (examples and the benchmark baseline included) plus 10,800 lines of tests: **253 passing top-level tests, 5 benchmarks** (more with subtests). Concurrency is confined to the thread host by convention.

**Architecture as built: thread per core, like Tina.** Each shard is a goroutine locked to its own OS thread (optionally pinned to a core) that owns its isolates, message pool, timers and sockets. Shards exchange 128-byte messages through lock-free SPSC rings in the shared address space (atomic cursors, one publish per tick, one commit per drain). An idle shard spins briefly, then sleeps in its own `epoll`; a peer that publishes into one of its rings wakes it through an `eventfd`. Goroutines and other concurrency primitives are **confined by convention to one file** (`threads.go`, the thread host) and the tests; the rest of the engine is single-threaded per shard (§2). A single-thread cooperative driver (`System.Step`) remains for the deterministic simulator and unit tests.

Linux is the only platform with I/O (other platforms build, without sockets).

### What exists

- **Engine** (package `gina`; see also large messages and `WaitIOOrMessage` under I/O below): isolates returning effects (`Done`, `Yield`, `WaitMessage`, `WaitIO`, `WaitIOOrMessage`, `Crash`); typed chunked slab storage with generational 28-bit handles; the fixed 128-byte `Message` (size asserted at compile time) with pointer-free, padding-free payload validation (cached lock-free); bounded mailboxes; a message pool with a protected system reserve; indexed timer heap with cancel; spawn with args; same-shard attachments (`SendAttach`); ordered shutdown; structural invariant checks.
- **Threaded runtime** (`System.Start/Run/Wait/Stop`, `Ctx.StopSystem`, `RunOptions{Pin, CPUBase, ShutdownGrace, SpinFor}`): one locked thread per shard; N×(N-1) Tina-style SPSC rings (producer and consumer cursors on separate cache-line pairs with cached copies of each other's cursor); wake-up protocol (announce sleep, re-check rings, block; senders publish, then check the flag) with sequentially consistent atomics; per-shard `epoll` + `eventfd`; idle spin before sleeping; graceful stop (`TagShutdown`, then force-free after the grace period); per-goroutine `SetPanicOnFault` so faults in handlers stay contained.
- **Supervision:** one-for-one, one-for-all, rest-for-one; permanent/transient/temporary; sliding restart budget; `TagChildExit` to the spawning isolate; Level-2 shard reset; quarantine and revive; a trap boundary (`recover` + `SetPanicOnFault`) that turns a handler panic or fault into a crash of that isolate only.
- **Simulator** (cooperative driver): seeded PRNG tree (SplitMix64 + xoshiro256**, known-answer tested), simulated clock, shuffled shard order, fault injection (message drop, handler crash, partitions), per-round invariant checks, FNV trace hash. Same seed gives the same hash (tested across 30 seeds with faults). Threaded runs are *not* deterministic.
- **I/O:** edge-triggered epoll reactor presented with completion semantics (`ctx.Listen` with optional `SO_REUSEPORT`, `IOAccept`/`IORecv`/`IOSend` + `WaitIO()`, **outbound sockets** (`ctx.Dial` makes a non-blocking TCP connection or a connected UDP socket to an IP address; `IOConnect` waits for the TCP connect and reports `0` or `-errno`; UDP sockets carry one datagram per `IOSend`/`IORecv`; Gina does no name resolution and no address policy), per-operation timeouts, cancellation when an isolate dies or shutdown starts, sockets owned by isolates and closed when the owner dies). One reactor (epoll instance) per shard. **Large messages** (added for WebSocket): `SendRaw` takes data of any length up to `SystemSpec.MaxMessageBytes` (default 16 MiB); beyond the 96 inline bytes it travels as an engine-owned immutable buffer beside the envelope (`FlagLarge`; the rings carry a parallel slot for it, so it crosses shards with the same ordering and drop rules, nothing is counted or freed by hand, and the GC reclaims it), read with `Ctx.Data()`; `gina.Blob` + `SendBlob` share one buffer among many receivers; `SendCorr` sets the `Correlation` field. `Send[P]` and `SendAttach` are unchanged (inline only). **`WaitIOOrMessage`** (added for WebSocket) parks an isolate on a read that a message may interrupt: the read completes with `-ECANCELED` ahead of the mail, and a read staged while mail is already queued is interrupted at once. Reads only: cancelling a write would leave a partial send. Nothing changes for isolates that use `WaitIO`.
- **`extensions/http`:** HTTP/1.1 server on isolates (listener per shard, isolate per connection): allocation-free strict parser, router, response context, idle/read/write timeouts, connection shedding, graceful shutdown, `Context.TLS()`, Server-Sent Events (`Context.EventStream`, `SendEvent`: any isolate on any shard pushes to an open stream), and **tunnels**: a handler that answers 101 may hand the connection to an `http.Tunnel` (`Context.SetTunnel`), which the connection isolate then drives (bytes in, bytes out, a timer, pushes from other isolates through `SendTunnel`). Server state is **per shard** (counters, buffer pool, date cache) so shard threads never share it.
- **`extensions/http2`:** HTTP/2 server on isolates, structured like `extensions/http` (listener per shard, isolate per connection, per-shard state) and serving the same `http.Router` and `http.Context` through a small exported seam in `extensions/http` (`Router.Dispatch`, `Context.Begin/Result`). h2 over TLS 1.3 (ALPN `h2`) or h2c with prior knowledge. Full frame layer, HPACK (decoder complete; encoder stateless, literals only), two-level flow control, SETTINGS negotiation, RFC 9113 §8 request validation, limits and a control-frame rate limit against rapid-reset-style floods. One protocol per port unless `Config.HTTP1Fallback` is set: then ALPN (or, without TLS, the first bytes) hands HTTP/1.1 connections to an embedded `extensions/http` connection (`Server.Adopt/Handle`). With `Config.ExtendedConnect` it also accepts the extended CONNECT of RFC 8441 and hands the stream to an `http.Tunnel` (WebSocket over h2; other streams of the connection carry on). See the package comment for what is not implemented.
- **`extensions/websocket`:** WebSocket (RFC 6455) server. A route handler calls `Endpoint.Serve` (or `Upgrade`); the connection, once switched, is a `Conn` running inside the connection isolate. One `Endpoint` serves **ws and wss over HTTP/1.1** (Upgrade handshake, TLS from `extensions/http`) and **ws and wss over HTTP/2** (RFC 8441 extended CONNECT, `extensions/http2`), with the same callbacks (`OnOpen/OnMessage/OnPong/OnClose`) and `Conn` API on all of them. Streaming frame parser (messages split anywhere, whole messages delivered without copying), fragmentation, ping/pong with a server keep-alive, closing handshake with timeout, incremental UTF-8 validation, strict header checks, message and queue limits, a same-origin default `CheckOrigin`, subprotocol selection, panic containment in callbacks (close 1011). Other isolates, on any shard, push to a connection by its `Peer` (`Push`, `PushText`, `PushClose`, any size; `NewShared`/`PushShared` to broadcast one message without copying it per connection; best effort). No extensions (permessage-deflate is not negotiated) and no sending of fragmented messages.
- **`extensions/webpush`:** Web Push (RFC 8030, 8291, 8292) sender, entirely isolates on one shard. `Send` is a message to a *sender* isolate, which validates the notification, signs a VAPID token (cached per push service) and starts a *delivery* isolate (up to `Workers` at once, the rest queued). The delivery encrypts (`aes128gcm`), asks the *resolver* isolate (a DNS cache that merges concurrent requests) for the push service's addresses (a *lookup* isolate per unknown name asks the nameservers over UDP for A and AAAA), connects with `Ctx.Dial`, runs the TLS 1.3 client handshake, POSTs over HTTP/1.1 (`Connection: close`) and reads the status line and headers; the outcome is sent to the asking isolate as a `TagResult` message. Retries on 429/5xx/network errors, 404/410 reported as `OutcomeGone`, an SSRF guard on the addresses about to be dialled (https only, no loopback/private/link-local/CGNAT addresses, no redirects), queue-full reported as `OutcomeOverloaded`. See the package comment.
- **`extensions/tls`:** sans-I/O **TLS 1.3 server and client** written for Gina (stdlib `crypto/tls` needs a blocking connection and a goroutine per connection, and its QUIC mode rejects TCP clients), on stdlib primitives. The client (`NewClient`, `client.go`) offers X25519, verifies the chain and name with `crypto/x509` against `ClientConfig.RootCAs` and the CertificateVerify signature itself, and negotiates ALPN. See "Verification" for what is and is not proven.
- **Tooling:** `bench/` (reproducible comparison against `net/http`, results, charts); `docs/BENCHMARKS.md`; examples `pingpong`, `supervised`, `shards`, `httpserver`, `https`, `sse`, `http2`, `websocket` (a chat whose room is an isolate on its own shard), `wsworkers` (a clock worker pushes the time to browsers, a greeter worker prints "hello" when a browser button is pressed).

### Status against this spec

| Spec | Status | Notes |
|---|---|---|
| §2 where concurrency is allowed | **Done** | Concurrency lives in `threads.go` and the tests, by convention (a lint tool existed and was removed). `extensions/webpush` was an exception from 2026-10-07 to 2026-10-08, while Gina could not connect out; it is isolates now. |
| §3 architecture | **Done** | Thread per core with in-process rings, as Tina. |
| §4.1 isolates and effects | **Done (subset)** | No `WaitReply`/`WaitIOOrCrash`; `TypeOptions` has no `BudgetWeight`. **Added:** `WaitIOOrMessage` (a message interrupts a parked read). |
| §4.2 messages, handles, tags | **Done** | Tag values are ours; `TagIOAccept/Recv/Send/Connect`, `TagChildExit` added. **Extended:** data longer than the 96 inline bytes is carried beside the envelope (`SendRaw`, `Blob`, `Ctx.Data`). |
| §4.3 `Ctx` | **Partial** | Done: `SendRaw`, `Send[P]`, `SendAttach`, `Spawn`, `RegisterTimer` (+`CancelTimer`), `StopSystem`, `Listen`, `Dial`, `IOAccept/IORecv/IOSend/IOConnect`, `CloseFD`, `OwnedFD`, `LocalPort`, `IsShuttingDown`, `ShardID`, `Now`. **Missing:** `Call`/`Reply`, logging, `KeyToShard`, `IPv4/IPv6` helpers, socket option/bind/shutdown calls, `IOWrite`/`IOSendTo`/`IOSendFile`, staged send buffers, `SupervisionGroupID`/`TypeConfig`. |
| §4.4 boot and `SystemSpec` | **Partial** | `NewSystem` validates (power-of-two sizes, id and slot limits, ring size, group ids); `Start/Run` host the shards. Added `MaxFDs`, `ResetMax`, `ResetWindow`. **Missing:** `GCConfig`, `ShardMemoryLimit`, `Mode`, `QuarantinePolicy`, `InitTimeout`. |
| §5 memory and GC | **Partial** | Done: no arenas, slab zeroing on teardown, attachments, pointer-free payloads. **Missing:** GC policies, memory limit and pressure shedding, `Development` poisoning. In thread mode **all shards share one heap and one collector** (a GC pause stops every shard). |
| §6 execution engine | **Done (subset)** | Tick phases: socket events, inbound, timers, dispatch, publish+wake. FIFO ready queue (no per-type batching), no log phase. Idle: spin, then block in the shard's own `epoll_wait` with a millisecond timer timeout. |
| §7.1-7.2 rings and wake-ups | **Done, in-process** | Atomic SPSC rings + eventfd wake-ups between shard threads. A shared-memory (`memfd`) variant for cross-process messaging is **not built** and no longer needed for the default design. |
| §7.3 reactor | **Partial** | epoll only. io_uring, fd handoff between shards, reactor-owned slots: not built (`HandoffFD` means *ownership transfer to an isolate*). |
| §8 supervision | **Mostly done** | Flat groups (no tree); budget exhaustion goes straight to a Level-2 reset. **No watchdog** (a handler stuck in a loop blocks its shard thread), no `ginactl`. `System.Stop` is the graceful shutdown; SIGTERM still exits immediately (a signal bridge is now permitted in a marked file but not built). |
| §9 simulation | **Mostly done** | Missing: message delay/duplication/reorder, timer skew, user-registered checkers, generation-monotonicity and fairness checkers, `GINA_SEED`. |
| §10 quality gates | **Partial** | Present: unit, layout, simulation, engine alloc-free, HTTP/TLS integration, **threaded runtime under `-race` (repeated)**. **Missing:** GC soak, escape-analysis diff, long seed sweeps. |
| §11 layout | **Simplified** | The engine is one package, not the planned `internal/*` split (see "As-built layout"). |

### Deviations from the sections below (deliberate)

| Spec | As built |
|---|---|
| (original plan) process per shard, `memfd` rings, launcher, watchdog | Replaced by the thread-per-core design once goroutines were allowed (Tina's own model). Multi-process deployment (a former `gina.Prefork`) was built and then removed: workers shared no memory and could not message each other, which defeats the model. |
| §7.3 reactor-owned I/O slots | Buffers belong to the isolate (`ctx.IORecv(fd, buf)`); the GC keeps them alive while an operation is in flight. A completion is pushed to the *front* of the mailbox (one spare mailbox slot guarantees room) and carries a byte count or `-errno`. |
| §6.1 timer wheel | Indexed binary heap with O(log n) cancel; deterministic tie-break by registration order; I/O timeouts complete operations with `-ETIMEDOUT`. |
| §6.1 batching by type | Plain FIFO ready queue. |
| §8.1 group tree | Flat groups. |
| §4.3 `SendResult` | Adds `RingFull`, `PayloadTooLarge`; `SpawnError` adds `BadFD`. |
| Handles after a Level-2 reset | `System.BootHandle` returns the current handle of boot isolate *i*. |
| Thread-mode GC | One shared heap and collector (Tina has no GC). |
| (not in the original plan) | HTTP, HTTP/2 and TLS extensions; idle spin; `bench/`. TLS is implemented from the protocol because the stdlib cannot run inside an isolate. |

### Measured results (details and caveats: `docs/BENCHMARKS.md`)

| Microbenchmark (one core) | Result |
|---|---|
| Cross-shard round on the cooperative driver (2 messages, atomic rings) | 111 ns, 0 allocs |
| Cross-**thread** hop, two pinned shard threads, idle spin | 0.16 µs (6.2M hops/s); 0.21 µs unpinned; 2.0 µs when every hop must sleep and be woken via the eventfd |
| Isolate spawn + message + exit | 52 ns, 0 allocs |
| HTTP request parse | 193 ns, 0 allocs |
| TLS server flight: ECDSA P-256 / Ed25519 / RSA-2048 | 81 µs / 76 µs / 663 µs |
| TLS record layer, AES-128-GCM / AES-256-GCM | 3.8 GB/s / 3.5 GB/s |

Against Go `net/http` on the same cores (1 to 8, loopback, 256 connections), Gina with **N shard threads in one process**: **1.5-1.9x** keep-alive throughput (HTTP and HTTPS), **2.6-5.8x lower p99**, **1.6-3.0x** TLS handshakes per second, **3.8x** (HTTP) and **2.0x** (HTTPS) on a 64 KiB echo. Memory at 8 cores: 21 MB (Gina) vs 20 MB (net/http) on HTTP, 51-103 MB (it varies run to run) vs 25 MB over HTTPS. Plain connection setup is on par at 1-2 cores, ahead at 4-8, and noisy (Gina's listener accepts one connection per tick per shard). The shared GC did not show up in p99, but the test is weak (small heap, no allocating handlers). `net/http` is a far more complete server; this is not a feature-equal comparison.


### Verification, and what is not verified

- **Automated:** 73 tests, including supervision matrices, deterministic simulation sweeps, backpressure, HTTP parser tables plus a 300k-input seeded mutation test, real-socket HTTP and HTTPS integration tests, HTTP/2 (HPACK vectors from RFC 7541, interop with Go's HTTP/2 client over TLS and h2c, and a raw-frame client that tests flow control, dozens of protocol-error cases and control-frame floods), WebSocket (a frame-parser suite that feeds random split points and checks every protocol error, and integration tests that run each scenario over ws, wss, h2c and h2 with TLS: echo, fragmentation, close handshake, keep-alive, shutdown, pushes across shards, many streams on one h2 connection, flow control with a 1000-byte window), TLS interop with Go's `crypto/tls` client across 4 key types x 2 cipher orders x 2 curves (payloads to 100 KB), and a negative control (a deliberately wrong HKDF label is caught). The TLS client is tested against our server (4 key types x 2 cipher orders x chunked delivery) and Go's `crypto/tls` server (refusals: wrong name, untrusted root, expired, tampered flight, TLS 1.2 only, no common group, HelloRetryRequest). Outbound sockets: TCP and UDP dial, refused connections, bad addresses.
- **Threaded runtime:** cross-thread ping-pong in two modes (always-sleep, which makes every hop use the eventfd wake path, and the default spin-then-sleep), all-to-all ring traffic across 4 threads checking for loss and reordering, timers firing on sleeping shards, stop latency, graceful shutdown, panic containment on a shard thread, and concurrent HTTP and HTTPS clients against 4 `SO_REUSEPORT` shard threads. These pass under **`go test -race`**, repeated 25-40 times, with no data race, hang or lost message.
- **By hand:** `curl` (OpenSSL 3.5) and `openssl s_client` against the HTTPS example (TLS 1.3, 200 KB echo byte-identical, TLS 1.2 refused with alert 70, plain HTTP on the TLS port closed), an HPACK differential test against the `hpack` package vendored in Go's standard library (3,000 random header sequences, encoder and decoder both ways; run once and not kept in the repo), and `curl --http2` / `--http2-prior-knowledge` (nghttp2) against `examples/http2` (h2 over TLS, h2c, parallel streams on one connection, 1 MB echo byte-identical, HTTP/1.1 clients refused).
- **WebSocket interop, by hand, not kept in the repo** (a throwaway module outside it, because the repo takes no dependencies): `gorilla/websocket` and `coder/websocket` over ws and wss (messages from 0 bytes to 5 MiB, streaming writes, ping/pong, close handshake, subprotocol; coder offers permessage-deflate and is correctly answered without it), `golang.org/x/net/http2`'s extended-CONNECT client against the HTTP/2 server (four concurrent streams on one connection, messages to 1 MiB, h2c and TLS), and Python `aiohttp` against `examples/websocket` (four clients on three shards, broadcast). All under `-race`. **No browser and not the Autobahn suite** were run.
- **Not verified:** the TLS code has had **no security audit** and should not guard anything that matters; no GC soak or leak test; the race detector only sees the interleavings that occur; Go's native fuzzer stalled in the sandbox, so parser fuzzing relies on the seeded mutation test; benchmarks are single-machine, loopback, short runs.

### Known limitations

- **Threaded mode:** while the system runs, only isolates may touch it (`Spawn`, `Send`, `Step` panic; external code uses `System.SendExternal`). `SendExternal` returning `SendOK` means the message was queued on the shard, not delivered: the mailbox can still drop it, which is counted in `Stats().Dropped`. There is no `SpawnExternal`. Shard threads run user handlers concurrently, so state shared *between* isolates on different shards must be made thread-safe by the user. One shared heap and GC: a collection pauses every shard. No watchdog: a stuck handler blocks its shard. One accept per tick per listener.
- **TLS:** no TLS 1.2, ChaCha20-Poly1305, HelloRetryRequest, resumption, 0-RTT, client certificates; handshake crypto runs inline on the shard thread (about 82 µs ECDSA, 670 µs RSA-2048). The client offers only an X25519 share, so a server that accepts nothing but P-256 or X448 is refused rather than retried; it does not check revocation (OCSP/CRL) or certificate transparency, and `ClientConfig.RootCAs` has no default (load the system pool at startup).
- **HTTP:** only the `chunked` transfer coding is understood (others get 501). `SendReader`, `ServeFile` and `ServeFS` call `Read` on the shard thread: a file in the page cache costs microseconds, but a cold read from a slow disk stalls every connection of that shard while it lasts, so serve large cold files from a dedicated shard or warm them first. A `Route.StreamBody` route has no body limit unless `MaxBody` sets one. Per-route `ReadTimeout` is a deadline for the whole request, or an inactivity timeout on a `StreamBody` route; it applies to HTTP/1.1 only (HTTP/2 streamed uploads use the connection's inactivity timeout). `OnBody` callbacks run synchronously on the shard thread. Handlers are synchronous (no waiting on other isolates until `Call`/`Reply` exists).
- **WebSocket:** no permessage-deflate or other extensions; pushes from other isolates are best effort (a full mailbox, ring or message pool drops them), so a hub that needs guaranteed delivery needs acknowledgements of its own. A subscriber that missed pushes is closed with 1013 (or `Config.OnOverflow` decides) rather than continuing with a gap, but the sender itself is not told; callbacks are synchronous inside the connection isolate (a slow one stalls that shard); a connection is half duplex like the HTTP servers (a peer that stops reading is dropped by the write timeout). Not run through Autobahn, not security-audited.
- **Web Push:** run once, by hand, against the real FCM, Mozilla autopush and Apple endpoints with made-up subscriptions (real DNS through systemd-resolved, system roots, TLS 1.3; answers 410, 404, 400); never against a real browser or a real subscription. The encryption is byte-exact against the RFC 8291 example, and the rest was tested against a fake push service (plain and TLS) that decrypts and checks the VAPID token and a fake DNS server. Delivery is best effort and at most once (full queue, mailbox or ring drops; queued work is discarded at shutdown); there are no push receipts, no `aesgcm` coding, no GCM keys. One connection and one TLS handshake per notification: no keep-alive, no HTTP/2, so a large broadcast to one vendor is slower than with a pooled client. DNS is UDP only (no TCP retry on truncation, no DNSSEC, no search domains) and its answers are trusted on message id and echoed question alone; the system resolver (`/etc/resolv.conf`) is read once at `New`. The sender and resolver are boot isolates whose handles get a new generation if they restart: `WebPush.Sender()` follows the sender, a handle saved earlier does not. Everything runs on one shard (`Config.Shard`), which needs about `2*Workers` sockets and `4*Workers` timers (`Install` raises `MaxFDs` and `TimerEntries` if they are lower). Not security-audited.
- **HTTP/2:** no server push, prioritisation, plain `CONNECT` (extended CONNECT works), trailers (dropped) or event streams (501); no `Upgrade: h2c`; no HTTP/1.1 on the same port unless `HTTP1Fallback` is set; response headers are not HPACK-indexed; each connection is half duplex (read, answer, write, read); response and request bodies are buffered unless the route streams them (`SendReader`, `Route.StreamBody`); per-route `ReadTimeout` applies to HTTP/1.1 only. Not run through h2spec, not security-audited.
- **API changes in this version** (breaking only for code that calls these hooks directly, as `extensions/http2` does): `http.Context.Begin` takes the socket (`fd`) as a fourth argument; `http.Router.BodyLimits` returns `(maxBody, readTimeout, stream, ok)`; `Router.Handle` and the `GET`/`POST`/... helpers return a `*Route`.
- **Not verified:** no h2spec run, no browser, no security review of the new TLS certificate hooks, ALPN-01 handling, chunked decoder or streaming paths.
- **Platform:** Linux-only I/O.

### Suggested next steps, in rough priority order

1. `Call`/`Reply` with timeouts (unblocks asynchronous HTTP handlers) and `KeyToShard` (now meaningful: shards can really exchange messages across cores).
2. A watchdog (per-shard heartbeat, Tina-style: exit the process so a supervisor restarts it) and a signal bridge for graceful SIGTERM, both now permitted in marked files.
3. ~~External injection~~ done: `System.SendExternal`. (`SpawnExternal`, which needs an asynchronous handle result, is not built.)
4. Determinism rules for the simulator-visible core (§9.2); batched accept.
5. `gctune`: memory limit, pressure shedding, GC policy, a soak test; evaluate whether the shared GC needs per-shard mitigation.
6. TLS hardening: independent review, HelloRetryRequest, tickets; optionally ChaCha20 via `x/crypto`.
7. HTTP: Datastar SDK; the docs set; io_uring; streaming that does not block the shard on cold files. HTTP/2: h2spec conformance run, a way to stream (SSE) over one stream of a multiplexed connection.

---

## 1. Goals and non-goals

### Goals
1. Reproduce Tina's programming model in Go: **isolates** (state machines) that return **effects**, run by **shards** (one per core), exchanging fixed-size messages, supervised Erlang-style, deterministically testable.
2. Keep Tina's constraints where they still make sense on a GC: **bounded resources by count** (slots, mailboxes, pools, rings), no shared mutable state between shards, no raw pointers in user code (generational handles), explicit backpressure (`MailboxFull`, `PoolExhausted`, …), "let it crash", and an allocation-free *engine* hot path. **Memory is managed by the Go garbage collector; there are no arenas** (§5).
3. **Confined concurrency**: goroutines and channels only in the thread host and tests that opt in; the engine is single-threaded per shard (§2).

### Non-goals (v1)
- Windows and macOS backends (Linux only; the backend interface leaves room for kqueue).
- Matching Odin's raw performance or hard real-time latency. We aim for an allocation-free engine hot path and short, per-shard GC pauses, not C parity.
- API compatibility with upstream. The shape is the same, the surface is Go-idiomatic.

## 2. Where concurrency is allowed

### 2.1 History and rule
Gina originally forbade goroutines and channels everywhere. That forced a process-per-shard design, and on 2026-10-07 the project owner relaxed it ("free to use goroutines and chan if needed") so the runtime can follow Tina's thread-per-core model. What remains is a rule about **confinement**:

- The engine (isolates, shard loop, scheduler, supervision, I/O reactor, rings, timers) is **single-threaded per shard**: no `go` statement, no channels, no mutexes.
- Goroutines, channels and `sync` primitives are kept to `threads.go` (the thread host) and the tests that exercise it. This is a convention; nothing enforces it. The one use outside is an `atomic.Uint64` per handle in `extensions/webpush`, because `Send` reads the sender's handle from every shard.
- Shards share **only**: the ring matrix (atomic cursors), each shard's `sleeping` flag and wake eventfd, the `quarantined` flag, and the system stop flag. Every other piece of mutable state belongs to exactly one shard.

### 2.2 What this means for handler code
Handlers run on N threads at once. State inside an isolate is safe. State shared *between* isolates on different shards (a captured counter, map or cache) is not, unless the handler makes it safe (atomics, a lock) or keeps it per shard indexed by `ctx.ShardID()`, as the HTTP server does. Immutable shared state (routes, config) is fine. While a system runs, `System.Spawn`, `Send` and `Step` panic: shards own their state.

### 2.3 Enforcement
None. A syntactic linter (`ginalint`) once checked the confinement rule and was removed; keep goroutines, channels and `sync` out of the engine files by review.

### 2.4 What the Go runtime does anyway
The runtime has its own goroutines and threads (GC workers, `sysmon`). They are out of scope.

## 3. Architecture: thread per core (Tina's model)

Tina runs one OS thread per core, each owning a shard, joined by lock-free SPSC rings in shared memory. Gina does the same:

| Option | Verdict |
|---|---|
| A. All shards cooperatively on one thread (`System.Step`) | Kept for the **simulator** and unit tests: deterministic |
| **B. One goroutine per shard, `LockOSThread`ed and optionally pinned (`sched_setaffinity`)** | **Default.** Tina's thread-per-core model |
| D. Raw `clone(2)` threads | Rejected: corrupts Go runtime assumptions |

```
                                  one process
  ┌── shard thread 0 ─────────┐   ring 0→1 (SPSC, atomic cursors)   ┌── shard thread 1 ─────────┐
  │ isolates, message pool,   │ ──────────────────────────────────► │ isolates, message pool,   │
  │ timers, sockets           │ ◄────────────────────────────────── │ timers, sockets           │
  │ own epoll + wake eventfd  │   ring 1→0        N×(N-1) rings     │ own epoll + wake eventfd  │
  └───────────────────────────┘                                     └───────────────────────────┘
        pinned to core 0                                                   pinned to core 1
```

Each shard thread runs the tick loop of §6.1, owns one `epoll` instance (sockets and timer deadlines) and one `eventfd` (wake-ups), and in the HTTP server owns its own `SO_REUSEPORT` listener. Because shards share one address space, a ring is just a pointer, exactly as in Tina.

## 4. Public API (package `gina`)

### 4.1 Isolates and effects
Isolate state is an ordinary Go struct `T`. It may contain slices, maps, strings and pointers; the GC owns it (§5). Message payload types, by contrast, must be pointer-free (§5.3).

```go
type Handler[T any]     func(self *T, ctx *Ctx, msg *Message) Effect
type InitHandler[T any] func(self *T, ctx *Ctx, args []byte) Effect  // args ≤ 64 bytes

type Effect struct { Kind EffectKind; Fault FaultReason }  // 2 bytes, returned by value
func Done() Effect
func Yield() Effect
func WaitMessage() Effect
func WaitReply() Effect                 // after ctx.Call staged a call
func WaitIO() Effect                    // after ctx.Submit* staged I/O
func Crash(r FaultReason) Effect
func WaitIOOrCrash(r SubmitResult) Effect
```

Principles carried over from upstream:
- An effect is a **state notification, not an action**. Actions (send, spawn, stage I/O, timers) happen through `ctx` during the handler; the return value only says what the isolate does next.
- Handlers are **not coroutines**: no stack is saved; all continuation state lives in `*T`.
- **No selective receive**: the handler always gets the head of its mailbox.
- **One I/O op and one outstanding `Call` per isolate.** Concurrency within a connection is expressed as separate reader/writer isolates.

Registration (generics erase `T` into a descriptor; the wrapper does the single `unsafe.Pointer` cast):

```go
func RegisterType[T any](id TypeID, o TypeOptions, init InitHandler[T], h Handler[T]) TypeDescriptor
type TypeOptions struct {
    SlotCount, MailboxCapacity /*256*/, BudgetWeight /*1*/ int
    ChunkSize int  // slots per allocation chunk, power of two (default 256)
    Eager bool     // allocate all SlotCount slots at boot (default: chunks on demand)
}
```
Type IDs are 0–254; 255 is reserved for supervision subgroups.

### 4.2 Messages, handles, tags
```go
type Handle uint64   // shard:8 | type:8 | slot:20 | generation:28
type Message struct {      // exactly 128 bytes; size asserted at compile time
    Source, Dest  Handle    //  8 + 8
    Correlation   uint32
    _reserved     uint32    // scheduler linkage
    Tag           Tag       //  u16
    Flags         uint16
    PayloadSize   uint16
    _pad          uint16
    Payload       [96]byte
}
```
- User tags ≥ `0x0040`; below that are system tags (`TagShutdown`, `TagTimer`, `TagCallReply`, `TagCallTimeout`, `TagChildExit`, I/O completion tags such as `IOTagRecvComplete`). Numeric values are **(ours)**.
- Typed helpers (functions, since Go methods cannot be generic): `PayloadAs[P](msg) *P`, `Send[P](ctx, to, tag, *P) SendResult`, `SelfAs` is unnecessary (the handler already receives `*T`). `P` must be pointer-free and ≤ 96 bytes (checked at compile time via array-length trick and at registration via `reflect`).
- Generational handle: slot teardown bumps the generation; any later use returns `StaleHandle`.
- A same-shard message may carry an arbitrary Go value as an **attachment** (`ctx.SendAttach` / `ctx.Attachment()`, §5.3).

### 4.3 Context (`*Ctx`), mirrors upstream `ctx_*`
| Area | Go surface |
|---|---|
| Messaging | `ctx.SendRaw(to, tag, payload) SendResult`; `gina.Send[P]` |
| Call/reply | `ctx.Call(to, tag, payload, timeout) CallResult`; `ctx.Reply(payload) ReplyResult` |
| Spawn | `ctx.Spawn(SpawnSpec) (Handle, SpawnError)`; `SpawnSpec{ArgsPayload [64]byte; ArgsSize; GroupID; TypeID; RestartType; HandoffMode; HandoffFD}`; `gina.InitArgs[T](*T)` |
| Timers | `ctx.RegisterTimer(d time.Duration, tag Tag)` (one-shot; delivered as a message) |
| Logging | `ctx.LogRaw/LogTyped` (96-byte payload, ring buffer, flushed once per tick) |
| Memory | none: isolates use ordinary Go allocation (`make`, `append`, `new`); discipline in §5.4 |
| Attachments | `ctx.SendAttach(to, tag, payload, v any) SendResult` (same shard only), `ctx.Attachment() any` (valid for the current turn). Replaces upstream's transfer buffers. |
| I/O control (synchronous) | `Socket`, `Bind`, `Listen`, `SetSockOpt*`, `GetSockOpt`, `Shutdown`, `ReadIOSlot(slot, n)` (valid for one handler call) |
| I/O submit | `ctx.SubmitIO(op) SubmitResult` (committed only if the handler returns `WaitIO`), helpers `IOSend`, `IOWrite`, `IOSendTo`, `IOSendFile` (+ `SendfileAllBytes`); staged-buffer variant `ClaimSendSlot`/`IOSendStaged` |
| Identity | `ctx.IsShuttingDown()`, `ShardID()`, `SupervisionGroupID()`, `RootGroupID()`, `TypeConfig()` |
| Routing | `gina.KeyToShard(key uint64, shards uint8) uint8` (modulo; callers hash non-uniform keys first) |
| Addresses | `gina.IPv4(a,b,c,d,port)`, `gina.IPv6(...)` |

Result enums are copied from upstream: `SendResult{Ok, MailboxFull, PoolExhausted, StaleHandle, AttachNotLocal}`, `ReplyResult`, `CallResult{…, TargetQuarantined}`, `SubmitResult{…, NoStagingSlot, PayloadTooLarge}`, `SpawnError{SlotsFull, GroupFull, GroupNotAllocated, TypeNotAllocated, InitFailed, MemoryPressure}`, `ExitKind{Normal, Crashed, Shutdown}`, `RestartType{Permanent, Transient, Temporary}`. All result-returning functions are annotated in docs as must-check;

### 4.4 Boot
```go
func Run(spec SystemSpec) int   // never returns in a shard; returns exit code in launcher
```
`SystemSpec` fields follow upstream: `ShardCount` (1–255), `Types []TypeDescriptor` (1–254), `ShardSpecs []ShardSpec{ShardID, TargetCore /* -1 = no pin */, RootGroup GroupSpec}`, `TimerResolutionNS`, `PoolSlotCount`, `TimerEntryCount`, `LogRingSize`, `DefaultRingSize` (pow2 ≥ 16), `ShardMemoryLimit` (soft, bytes), `GC GCConfig` (§5.5), `Mode{Production,Development}` (Development poisons stale references, §5.2), `QuarantinePolicy{Quarantine,Abort}`, `InitTimeout`, `ShutdownTimeout`. `GroupSpec{Strategy; RestartCountMax; WindowDurationTicks; Children; ChildCountDynamicMax}`.
`Validate()` runs in **every** process before any isolate exists and again in the launcher before spawning shards; power-of-two, id uniqueness, 20-bit slot limit, `ShardMemoryLimit` above a lower bound estimated from slot counts × `unsafe.Sizeof(T)` plus pools.

## 5. Memory model: the Go heap and the GC (no arenas)

### 5.1 Principles
- **No arenas, no `mmap`'d allocators, no `unsafe` casts for isolate state.** Isolate state, mailboxes, message pools, timer entries and I/O buffers are ordinary Go values owned by their shard process and reclaimed by the Go GC.
- **Bounded by count, softly bounded by bytes.** Slot counts, mailbox capacity, pool size, timer entries and ring size stay fixed and enforced, so overflow still produces explicit results (`MailboxFull`, `PoolExhausted`, `SlotsFull`). Bytes are bounded by `ShardMemoryLimit` through `debug.SetMemoryLimit` plus the pressure policy in §5.5. A GC heap cannot be hard-capped, so this is a soft bound.
- **Shared-nothing is preserved by design.** Shards never touch each other's isolates, pools, timers or sockets; the only shared memory is the ring matrix and a few atomic flags (§2.1). **In thread mode the Go heap and garbage collector are shared by every shard**, so a collection's stop-the-world phase pauses all of them.

### 5.2 Isolate storage
`internal/slab.Store[T]` is a typed, chunked slot store:
- Slots live in `[][]T` chunks of `ChunkSize` (power of two, default 256), allocated on first use up to `SlotCount`. `Eager: true` allocates all chunks at boot. Chunks never move and are never freed while the shard runs, so the `*T` handed to a handler is stable for the turn. Each chunk is one contiguous `[]T`, so same-type isolates are still dense and iterated by type.
- Per-type metadata is struct-of-arrays: `gen []uint32`, `state []uint8`, mailbox rings. A LIFO free list (`[]uint32`) is allocated at boot.
- **Teardown zeroes the slot** (`*p = T{}`) and then bumps `gen`. Zeroing is what lets the GC reclaim everything the isolate referenced; without it a dead isolate would pin its data until the slot is reused.

Isolates may hold any Go data (slices, maps, strings, pointers). Rules:
1. Do not retain `*Message`, `*Ctx`, I/O slot slices, or the attachment beyond the current turn. In `Development` mode the engine zeroes the message and fills released I/O slots with `0xDD` after the turn, so a retained reference fails loudly in tests.
2. Isolates never share Go memory directly. They communicate only through messages. After `SendAttach` the sender must drop its reference to the value (convention, documented; the engine cannot enforce it).

### 5.3 Messages and payloads
- The envelope is still a fixed 128-byte `Message` with a 96-byte inline payload, copied by value. It must stay byte-oriented because it crosses processes through the shm rings (§7.1). **Payload types `P` and spawn args must therefore be pointer-free.** `RegisterType`/`Send[P]` check this with `reflect` at registration time and with a compile-time size assertion. Isolate state `T` has no such restriction.
- The per-shard message pool is a `[]Message` allocated once at boot with `make`, plus a free list (user region and protected system region as in §6.1).
- **Attachments replace transfer buffers.** `ctx.SendAttach(to, tag, payload, v any)` hands an arbitrary Go value to a **same-shard** isolate; the receiver reads it with `ctx.Attachment()` for the duration of one turn. The value lives in a side slice parallel to the pool (`[]any`, cleared when the message is released). Sending an attachment to another shard returns `AttachNotLocal`. Large cross-shard data is decomposed into several messages, as upstream does.

### 5.4 Allocation discipline
- The **engine** hot path (tick, dispatch, mailboxes, pool, timer wheel, ring publish, reactor poll, supervision bookkeeping) allocates nothing. This is test-enforced (§10) with no-op handlers.
- **User handlers may allocate**, but the docs state the cost model: GC work scales with live pointers, not bytes. Guidance: reuse buffers held in isolate state, size slices once, prefer `[]struct` to `[]*struct`, use indexes or `Handle`s instead of pointer graphs, and keep per-connection state small. A `gina/gcstats` helper reports per-type live bytes (sampled via `runtime/metrics`).

### 5.5 GC integration (planned; not built)
In thread mode these knobs are process-wide. Boot-time settings, all overridable in `SystemSpec.GC`:

| Knob | Default | Notes |
|---|---|---|
| `GOMAXPROCS` | 1 | The shard runs on one goroutine; with one P, GC mark work time-slices with it. Set 2 to give the GC worker its own P (and core). To be chosen from M6 measurements. |
| CPU affinity | `TargetCore`, or `CPUSet []int` | Applied with `sched_setaffinity` on the locked main thread. |
| `Policy` | `Auto` | See below. |
| `GCPercent` | 100 | Used by `Auto`. |
| `ShardMemoryLimit` | required | `debug.SetMemoryLimit`; also the denominator for pressure checks. |

`GCPolicy`:
- **`Auto`**: stock Go pacing (`SetGCPercent` + `SetMemoryLimit`).
- **`IdleAssisted`**: `SetGCPercent(-1)` with the memory limit as a backstop. The shard calls `runtime.GC()` only at the idle phase of the tick (just before it would block, §6.1 step 7) when the heap has grown by more than `IdleGCThreshold` since the last cycle, or immediately when heap use passes 80% of `ShardMemoryLimit`. This moves collection into otherwise dead time, and under saturation it falls back to the hard trigger.
- **`Manual`**: the engine never triggers GC. Used by the simulator and tests.

**Heap sampling** uses `runtime/metrics` with a preallocated `[]metrics.Sample`, read at most once per `PressureCheckTicks` ticks (no per-tick allocation).

**Memory pressure** (replaces upstream's "arena exhausted → shed"): after each GC cycle (detected via `/gc/cycles/total:gc-cycles`), compare `/gc/heap/live:bytes` to `ShardMemoryLimit`:
- above `ShedWatermark` (default 85%): `Spawn` returns `MemoryPressure`; sends continue to work;
- above 100% and still so after a forced `runtime.GC()`: **Level 2 reset** (§8.2). The launcher also samples each shard's RSS from `/proc/<pid>/statm` and kills a shard that exceeds 1.5 × the limit (Level 3).

**Honest limitation.** Go's collector is concurrent but still has short stop-the-world phases and mutator assists. Per-shard heaps keep them small and independent, but latency is no longer strictly deterministic as upstream's is. The spec commits to *measuring and reporting* GC pause and tick-latency percentiles (§10), not to hard real-time bounds.

### 5.6 GC and determinism
No behaviour may depend on GC timing. `runtime.SetFinalizer`, `runtime.AddCleanup`, `weak` and `unique` are banned in the engine (§9.2). The simulator runs with `Manual` policy.

## 6. Execution engine

### 6.1 Shard tick (fixed phase order, as upstream)
1. **Inbound**: drain inbound SPSC rings (one acquire-load per ring per tick) into the message pool and target mailboxes.
2. **I/O completions**: ask `Backend` for finished operations; wake isolates in `WaitIO`.
3. **Timers**: advance the wheel, deliver due timer messages (system pool region).
4. **Dispatch**: run ready isolates, batched **by type** (cache locality), each given up to `BudgetWeight`-scaled turns. Per turn: pop one message → call handler → apply effect → release the message and its attachment (in `Development` mode, poison them).
5. **Outbound publish**: write staged cross-shard messages into outbound rings, then **one release-store per ring** (readers never observe partial batches; also what makes a mid-batch process death safe).
6. **Logs**: flush log ring to stderr (or a launcher-owned fd).
7. **Idle**: if nothing ran, run idle-time GC when the `IdleAssisted` policy asks for it (§5.5), spin `IdleSpinTicks`, then block in `epoll_wait` (see §7) with the nearest timer deadline armed on a `timerfd`.

Ready set = bitmap words per type with `math/bits.TrailingZeros64` scan. Mailbox = fixed ring of message-pool indices per isolate. Message pool = `[]Message` preallocated at boot with a free list of 128-byte envelopes, partitioned into a user region and a system region protected by a high-water mark (timers, supervision, and call timeouts must succeed under data-plane saturation).

### 6.2 Effects semantics
`Done` frees the slot and bumps generation; `Crash` reports to the supervisor; `Yield` re-queues for the next tick; `WaitMessage` parks; `WaitReply`/`WaitIO` require a staged call/I/O, else it is a `ContractViolation` crash. `WaitReply` carries a mandatory timeout; the reply (or a timeout message) arrives with the matching `Correlation`.

### 6.3 Backpressure (all bounded, all explicit)
Per-isolate mailbox (default 256) → `MailboxFull`; per-shard pool → `PoolExhausted`; cross-shard ring full → send fails immediately, no overflow buffer; slow consumers cannot cause allocation elsewhere. `Send` is best-effort UDP-like with immediate feedback; `Call` + timeout is the reliable-ish path. Spawn on a full type table/group, or under memory pressure → `SpawnError`.

## 7. Cross-shard transport and I/O (Linux, no cgo)

### 7.1 Rings
One ring per ordered shard pair, N×(N-1) in total, allocated by `NewSystem` before any thread starts. Each ring holds 128-byte `Message` slots (power-of-two capacity, `SystemSpec.RingSize`) and is laid out as in Tina: a **producer** cache-line pair (published cursor, local staged cursor, cached copy of the consumer's cursor), a **consumer** cache-line pair (read cursor, local cursor, cached copy of the producer's cursor), and a cold part (buffer, mask). The producer writes slots with plain stores and publishes the whole tick's batch with **one atomic store**; the consumer sees it with one atomic load and returns the slots with **one atomic store per drain**. A full ring fails the send immediately (`RingFull`); nothing overflows. Go's atomics are sequentially consistent, stronger than the release/acquire Tina needs; the cost was about 18 ns per ping-pong round.

### 7.2 Wake-ups
Each shard owns an `eventfd` registered in its own epoll set. The protocol that cannot lose a wake-up: the sleeper (1) spins briefly checking its rings, (2) stores `sleeping=1`, (3) checks for work **once more**, (4) blocks in `epoll_wait` (with the next timer deadline as timeout), (5) clears the flag. A sender (a) publishes into the ring, then (b) loads the target's `sleeping` flag and writes the eventfd only if it is set. With sequentially consistent atomics at least one side sees the other, so a message is never stranded; under load no syscall is made. `Stop` writes the eventfd unconditionally. *(Tina's published source shows no wake protocol; this one is ours.)* `RunOptions.SpinFor` (default 20 µs) sets how long to spin before blocking; spinning cut a cross-thread hop from about 2.0 µs to 0.21 µs.

### 7.3 Reactor (`Backend` interface)
```go
type Backend interface {
    Submit(op *IOOp) SubmitResult        // non-blocking, allocation-free
    Poll(out []Completion, wait Deadline) int
    Close(fd FDHandle)
}
```
- **v1 `epollBackend`**: completion semantics emulated over readiness. Submit tries the non-blocking syscall immediately; on `EAGAIN`, arm `EPOLLONESHOT`; on readiness, perform the syscall into a preallocated I/O slot (reactor buffer pool) and emit a `Completion`. Regular-file ops execute inline within a per-tick budget (documented stall risk).
- **v2 `uringBackend`**: raw `io_uring_setup/enter/register` syscalls with mmap'd rings, no cgo. Optional, behind the same interface.
- **`simBackend`**: scripted completions and faults (§9).
- `FDHandle` is a generational index into a shard-local FD table; isolates never see raw fds.
- Inbound data lands in shard-owned slots, read via `ReadIOSlot`, reclaimed automatically when the handler returns (safe if the isolate crashes mid-flight). Outbound data is read straight from the parked isolate's state (or a staging slot).
- **FD handoff between shards** (upstream has a handoff table): not built. Within one process a descriptor can be re-registered in another shard's fd table. The default accept strategy needs neither: each shard binds its own listener with `SO_REUSEPORT`.

## 8. Supervision and fault containment

### 8.1 Tree
Groups form a tree built from the boot spec (static children + `ChildCountDynamicMax`). Strategies `OneForOne`, `OneForAll`, `RestForOne`; restart types `Permanent`, `Transient`, `Temporary`; budget `RestartCountMax` within `WindowDurationTicks`. Supervision is done by **direct function calls inside the shard**, not messages.

### 8.2 Escalation levels (mapped to Go)
| Level | Trigger | Action |
|---|---|---|
| 1 | Isolate returns `Crash`, or a **panic/fault** is recovered | Wipe slot, bump generation, supervisor applies strategy |
| 2 | Group restart budget exhausted, or live heap stays above `ShardMemoryLimit` after a forced GC (§5.5) | Tear down all isolates in the shard (zero every slot so references are dropped), reset pools, rebuild the tree from the boot spec, then `runtime.GC()` so memory is actually returned; other shards unaffected |
| 3 | Root budget exhausted, unrecoverable runtime fault (OOM, stack overflow, `fatal error`), or watchdog timeout | **Thread mode:** the process exits (a fatal runtime error cannot be contained per shard) and an external supervisor restarts it. |

**Trap boundary** (replaces `sigaltstack`/`siglongjmp`): the shard loop runs turns inside a function with `defer recover()` and `SetPanicOnFault(true)`, so Go panics *and* faulting `unsafe` accesses are recoverable. The loop records the current (type, slot) in a plain variable before each turn; on recovery it logs, treats that isolate as crashed (Level 1) and re-enters the loop. Messages the crashed turn had already sent stay sent **(ours)**; its staged-but-uncommitted I/O is discarded. Runtime-fatal errors cannot be recovered; those are Level 3 by design. In thread mode they take the whole process down.

**Watchdog (not built).** In thread mode a handler stuck in a loop blocks its shard's thread and Go cannot kill a goroutine. The Tina-style answer is a watchdog (now permitted as a goroutine in a marked file) that watches per-shard heartbeat counters and exits the process after `WatchdogTimeout`, so an external supervisor restarts it.


### 8.3 Shutdown
No framework-ordered shutdown. `System.Stop` (or `Ctx.StopSystem` from an isolate) delivers `TagShutdown` to every isolate and cancels their in-flight I/O so they wake; natural ordering emerges (listeners stop, connections drain). Isolates still alive after `RunOptions.ShutdownGrace` (default 5 s) are force-freed, so an isolate that ignores `TagShutdown` delays the stop by the full grace period. OS signals are not handled yet (SIGTERM exits immediately); a bridge is now permitted in a marked file.

## 9. Deterministic simulation testing

### 9.1 Architecture
`Simulator` runs **all shards on one thread** with the cooperative driver (`System.Step`); threaded runs are not deterministic. The shard engine is written against three seams — `Clock`, `Backend`, `Transport` — so production code paths run unchanged:
- `SimClock`: a tick counter advanced by the harness.
- `simBackend`: scripted I/O completions, injectable errors, delays.
- `simTransport`: the same SPSC ring code over ordinary memory, plus drops, delays, duplication, and partitions.
Each round: advance clock → fault engine → **shuffle shard order** (emulates production phase drift) → tick each shard → run checkers.

### 9.2 Determinism contract (the simulator and `internal/`)
All randomness from a **PRNG tree**: master seed → SplitMix64 → independent child streams per domain (network, scheduling, faults, each isolate type). Generator is our own **xoshiro256\*\*** (not `math/rand`, whose stream is not part of our compatibility contract). Faults are integer `Ratio{Num, Den}`, no floats. Forbidden in the core: `range` over maps, `time.Now`, `math/rand`, `select`, reading env/clock, address-dependent ordering (e.g. sorting by pointer value), and `runtime.SetFinalizer`/`AddCleanup`/`weak`/`unique` (no behaviour may depend on GC timing). User isolates run under the same contract in the simulator: iterating a Go map in a handler makes a run non-reproducible. Same seed + same config ⇒ identical trace hash (FNV-1a over a canonical event stream). Failures print the seed; `GINA_SEED=…` replays.

### 9.3 Checkers
Run at intervals and at end: pool conservation (free + in-use = capacity), generation monotonicity, mailbox bounds, FD-table/handoff invariants, scheduler fairness (no ready isolate starved beyond N ticks), plus user-supplied invariants via `RegisterChecker`.

Because the cooperative driver runs every shard on one thread, simulation needs no virtual-time hacks. Determinism is a property of the design, not a layer on top.

## 10. Testing and quality gates

| Layer | What | Gate |
|---|---|---|
| Unit | slab store, bitmap, ring, pool, wheel, handle packing, PRNG known-answer vectors (xoshiro) | `go test` |
| Layout | `Message` is 128 B; rings/cursors cache-line aligned; handle bit-fields round-trip | compile-time + test |
| Simulation | supervision strategies, budgets/escalation, backpressure, timers, call/reply timeout, FD table, determinism (run twice, compare hashes), multi-seed sweeps in CI | `go test ./sim/...` |
| Engine alloc-free | `testing.AllocsPerRun` on tick/dispatch/send/timer/ring/reactor-poll with no-op handlers; budget 0 | CI |
| GC behaviour | soak: spawn/crash/tear down 10M isolates holding slices and maps; live heap stays flat (no reference leaks through slots, mailboxes, pool entries, attachments, timers). Record `/sched/pauses/total/gc:seconds` and tick-latency percentiles per `GCPolicy` against a stored baseline | CI (nightly) |
| Escape analysis | `go build -gcflags=-m` diffed for `internal/sched` hot functions | CI |
| Threaded runtime | cross-thread ping-pong (always-sleep = lost-wake-up stress; default spin), all-to-all ring traffic with loss/order checks, timers on sleeping shards, stop latency, graceful shutdown, panic containment, concurrent HTTP/HTTPS clients on `SO_REUSEPORT` shard threads | `go test -race`, repeated (**present**) |
| Examples | TCP echo and task dispatcher end-to-end, load tested with an external client | acceptance |

Initial performance targets (to be *measured*, not assumed): 0 allocs/op on engine hot paths; GC pause p99 and tick-latency p99 under a steady spawn/crash churn workload, per `GCPolicy`; same-shard send+dispatch and cross-shard ring round trip benchmarked and tracked per commit. Absolute numbers are set after M3.

## 11. Repository layout
```
gina/
  SPEC.md  LICENSE  NOTICE  README.md
  gina.go api.go ctx.go effect.go handle.go message.go errors.go   # public surface (package gina)
  internal/
    slab/       chunked typed slot store, free lists    (typed arenas in allocator_arena)
    gctune/     GC policy, memory limit, pressure       (new; no upstream equivalent)
    bitmap/     bitmap words                            (bitmap_words)
    ring/       SPSC ring over shared/plain memory      (spsc_ring)
    shm/        memfd region, layout, control blocks    (shard_control_channel, transport)
    pool/       message pool, io slot pool, fd table    (allocator_*)
    mailbox/    per-isolate mailbox                     (mailbox)
    timer/      hashed timer wheel                      (timer)
    sched/      ready set, batching, tick loop          (shard, lifecycle, isolate_lifecycle)
    supervise/  groups, strategies, budgets             (supervision)
    trap/       recover/SetPanicOnFault boundary        (sys_trap_*)
    reactor/    Backend iface, epoll, (uring), fd table (io_backend_*, io_reactor)
    launcher/   re-exec, pidfd monitor, watchdog, ctl   (bootstrap*, watchdog)
    log/        ring + flush                            (logging*)
    prng/       xoshiro256**, SplitMix64                (prng)
    sim/        clock, faults, checkers, harness        (simulat*, sim_network)
  cmd/ginactl/
  examples/echo/  examples/dispatcher/
  extensions/http/  extensions/datastar/
  docs/{concepts,guides,reference}/
```
Dependency direction is strictly downward (`internal/slab` imports nothing of ours). (Planned: `golang.org/x/sys/unix` as the only external dependency. As built there is **no external dependency**: the standard `syscall` package is used.)

### As-built layout
```
gina/                         module "github.com/rm4n0s/gina"; no external dependencies
  types.go spec.go slab.go ring.go timer.go shard.go supervise.go system.go sim.go   # package gina: the engine
  threads.go pin_linux.go pin_other.go            # the thread host: the only engine file with goroutines; CPU pinning
  ctx_io.go reactor_linux.go reactor_other.go     # I/O API and the epoll reactor (stub on other OSes)
  gina_test.go threads_test.go                    # single-thread suite; threaded-runtime suite (-race)
  internal/prng/                                  # PRNG tree
  extensions/http/   parser.go router.go context.go server.go sse.go embed.go (+ tests, incl. threads_test.go)
  extensions/http2/  server.go conn.go request.go frame.go hpack.go huffman_table.go (+ tests: hpack, interop, raw frames)
  extensions/tls/    config.go conn.go keys.go record.go alert.go (+ tests, benchmarks)
  extensions/webpush/ subscription.go encrypt.go vapid.go sender.go delivery.go resolver.go dns.go sysconf.go service.go (+ tests; all isolates)
  examples/{pingpong,supervised,shards,httpserver,https,sse,http2,websocket,wsworkers,webpush}/
  bench/             run.sh summarize.py plot.py results*.csv results*.{png,svg}  nethttp/ (separate module: comparison baseline)
  docs/BENCHMARKS.md
```

## 12. Milestones

| M | Deliverable | Exit criteria | Status |
|---|---|---|---|
| M0 | `go.mod`, `ginalint`, CI, layout tests | Lint rejects `go`/`chan`/denylist fixtures | **Done, then lint removed** (no CI workflow in the repo) |
| M1 | slab store, handle, message, bitmap, ring, pool, wheel, PRNG | Unit tests; zero-alloc benchmarks for engine structures | **Mostly**: all but bitmap and wheel (heap instead) |
| M2 | Single-shard scheduler + effects + ctx send/spawn/timers, `simBackend`, simulator v1 | Determinism test; backpressure tests | **Done** (no `simBackend`: the simulator has no I/O) |
| M3 | Supervision L1-L2, trap boundary, call/reply, attachments | Strategy/budget sims; panic + fault recovery tests | **Mostly**: call/reply missing |
| M4 | Multi-shard in simulator (sim transport, shuffle, checkers, fault injection) | Seed sweep green in CI | **Mostly**: 30-seed sweep; fewer fault kinds than planned |
| M5 | Launcher, shm transport, eventfd wake-ups, multi-process mode, watchdog, L3 | Kill/respawn integration tests | **Replaced** by the thread host (`Start/Run/Stop`), atomic in-process rings and eventfd wake-ups (all done, tested under `-race`). Watchdog not built |
| M6 | epoll reactor, TCP echo example, FD handoff | Echo under external load; engine allocs/op = 0; GC report | **Mostly**: epoll and load tests done (HTTP instead of a TCP echo example); no fd handoff, no GC report |
| M7 | `extensions/http` and Datastar SDK | Conformance tests; dispatcher example | **HTTP done, plus TLS** (not originally planned); Datastar and dispatcher example not built |
| M8 | Optional io_uring backend; docs set; benchmarks | Backend parity suite | **Benchmarks done** (`bench/`, `docs/BENCHMARKS.md`); io_uring and the docs set not built |

## 13. Deviations from upstream (summary)
| Upstream | Gina | Reason |
|---|---|---|
| OS threads per core | **Same**: one goroutine per shard locked to an OS thread (optional `sched_setaffinity` pinning) | Goroutines were allowed on 2026-10-07 |
| `sigaltstack`+`siglongjmp` trap | `recover` + `SetPanicOnFault` (per goroutine); process exit = Level 3 | Idiomatic, safe in Go |
| Watchdog calls `_exit(0)` | Not built; same approach planned (exit and let a supervisor restart) | Go cannot kill a goroutine |
| `SIGUSR2` revives a quarantined shard | `System.Revive` (programmatic, single-thread driver only); no signal or `ginactl` | Not built |
| io_uring / kqueue / IOCP | epoll (v1), io_uring (v2) | Scope; Linux only |
| No GC, arenas, pointer-free state | **GC on, no arenas**; isolate state is ordinary Go, only message payloads are pointer-free | Requested design goal. Thread mode shares one heap and one collector (Tina has none) |
| Hard byte bound from a fixed arena | Bound by counts + soft `ShardMemoryLimit` + pressure shedding | A GC heap cannot be hard-capped; latency is no longer strictly deterministic |
| Odin `rawptr` + `self_as` | Generic `*T` handler param | Type safety by construction |
| Transfer buffers (shard-local) | Same-shard attachments; cross-shard data is still decomposed | GC makes arbitrary local hand-off safe |
| Tina's HTTP extension runs on its shard threads | Same: `extensions/http` runs on isolates on shard threads, one `SO_REUSEPORT` listener per shard | Kernel load balancing across listeners |
| TLS via the platform library | A sans-I/O TLS 1.3 server and client in `extensions/tls` | Stdlib `crypto/tls` cannot run inside an isolate; **unaudited** |

## 14. Decisions and open questions

Resolved:
- **Q1 - OS signals / the no-goroutine rule:** the rule was relaxed (2026-10-07) to confinement (§2). A SIGTERM bridge is now permitted in a marked file; not built, so SIGTERM still exits immediately.
- **Q2 - process vs. thread:** threads, as in Tina (shard goroutines locked to OS threads). A process-per-worker `Prefork` was built and removed (no cross-worker messaging).
- **Q3 - platform scope:** Linux only for I/O; the engine builds elsewhere (cross-compiled for macOS) but has no sockets there.
- **Q4 - name, license:** module `gina`. This repository's `LICENSE` is **MIT** (Manos Ragiadakos); upstream Tina is Apache-2.0. Gina reimplements the documented behaviour and copies no upstream source, so MIT is workable; if upstream code is ever copied or transliterated, Apache-2.0's attribution and NOTICE requirements apply to that code. There is no `NOTICE` file and no CI workflow in the repo yet.
- **Q5 - io_uring:** deferred.

Still open:
- **Q6 - GC defaults.** `gctune` is not built, so Go's defaults apply, and in thread mode the collector is shared by all shards. Decide after GC soak and pause measurements exist, including whether a mitigation is needed.
- **Q8 - TLS ownership.** Keep the in-repo TLS (needs a security review before any real use), or document it as development-only and terminate TLS in front of Gina?
- **Q9 - stuck-handler policy.** Adopt Tina's watchdog behaviour (exit the process and rely on a supervisor), or something gentler?
- **Q10 - external control of a running system.** Add a thread-safe `Inject`/command inbox so non-shard code can send messages and spawn isolates while the system runs?
