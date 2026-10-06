# Gina — Specification for a Go Port of Tina

Status: draft v0.3 (design, plus an implementation-status snapshot below) · Date: 2026-10-06 · Upstream: https://github.com/pmbanugo/tina (Odin, Apache-2.0)

"Gina" is the name of the Go port; the Go module path is the placeholder `gina`.

## 0. Basis and caveats

This spec is derived from upstream's README, `docs/concepts/*`, `docs/reference/{the_ctx_api,system_spec}.md`, and the type declarations in `src/api.odin`. I did not read the full source. Where upstream is silent I mark the item **(ours)**: a decision this port makes and is free to change. Names follow upstream where known, in Go style (`ctx_send_raw` → `gina.SendRaw`).

Upstream is Apache-2.0. Reimplementing from documented behaviour is fine. Copying or transliterating source requires keeping the license and NOTICE and marking changes.

## Implementation status (2026-10-06)

Gina is a working prototype of the Tina model in Go with **no goroutines and no channels**, plus an HTTP/1.1 + TLS 1.3 server built on it. It is about 5,800 lines of Go (examples and the benchmark baseline included) plus 2,300 lines of tests: **63 passing tests, 5 benchmarks**, and a linter that enforces the no-goroutine rule on the whole repo. The engine runs all shards of a `System` cooperatively on **one thread**; multi-core comes from running several independent processes (`gina.Prefork`), not from the shared-memory process-per-shard runtime this spec describes in §3 and §7. Linux is the only platform with I/O (other platforms build, without sockets).

### What exists

- **Engine** (package `gina`): isolates returning effects (`Done`, `Yield`, `WaitMessage`, `WaitIO`, `Crash`); typed chunked slab storage with generational 28-bit handles; the fixed 128-byte `Message` (size asserted at compile time) with pointer-free, padding-free payload validation; bounded mailboxes; a message pool with a protected system reserve; N×(N-1) staged/published rings between shards; indexed timer heap with cancel; spawn with args; same-shard attachments (`SendAttach`); ordered shutdown; structural invariant checks.
- **Supervision:** one-for-one, one-for-all, rest-for-one; permanent/transient/temporary; sliding restart budget; `TagChildExit` to the spawning isolate; Level-2 shard reset; quarantine and revive; a trap boundary (`recover` + `SetPanicOnFault`) that turns a handler panic or fault into a crash of that isolate only.
- **Simulator:** seeded PRNG tree (SplitMix64 + xoshiro256**, known-answer tested), simulated clock, shuffled shard order, fault injection (message drop, handler crash, partitions), per-round invariant checks, FNV trace hash. Same seed gives the same hash (tested across 30 seeds with faults).
- **I/O:** edge-triggered epoll reactor presented with completion semantics (`ctx.Listen` with optional `SO_REUSEPORT`, `IOAccept`/`IORecv`/`IOSend` + `WaitIO()`, per-operation timeouts, cancellation when an isolate dies or shutdown starts, sockets owned by isolates and closed when the owner dies).
- **Multi-core:** `gina.Prefork(n, opts)` re-executes the binary n times (no goroutines, `wait4` supervision, restart budget, `PDEATHSIG`); with `ReusePort` every worker binds the same port and the kernel balances connections.
- **`extensions/http`:** HTTP/1.1 server on isolates (listener per shard, isolate per connection): allocation-free strict parser, router, response context, idle/read/write timeouts, connection shedding, graceful shutdown, `Context.TLS()`.
- **`extensions/tls`:** sans-I/O **TLS 1.3 server** written for Gina (stdlib `crypto/tls` needs a blocking connection and a goroutine per connection, and its QUIC mode rejects TCP clients), on stdlib primitives. See "Verification" for what is and is not proven.
- **Tooling:** `cmd/ginalint`; `bench/` (reproducible comparison against `net/http`, results, charts); `docs/BENCHMARKS.md`; examples `pingpong`, `supervised`, `httpserver`, `https`.

### Status against this spec

| Spec | Status | Notes |
|---|---|---|
| §2 no goroutines/channels | **Done, enforcement narrower than specified** | `ginalint` is syntactic: `go`, channel types/send/receive, `select`, denylisted imports, `time.After/AfterFunc/Tick/NewTimer/NewTicker`, `t.Parallel`, `sync.WaitGroup`. It runs as a test over the repo; nested Go modules are exempt (`bench/nethttp`). **Not built:** `go/types` checks, `go list -deps`, the determinism rules (map iteration, `time.Now`, `math/rand`, finalizers), `-sim`, `//gina:mustcheck`. |
| §3 architecture | **Changed** | Prefork processes instead of shared-memory process-per-shard (see deviations). |
| §4.1 isolates and effects | **Done (subset)** | No `WaitReply`/`WaitIOOrCrash`; `TypeOptions` has no `BudgetWeight`. |
| §4.2 messages, handles, tags | **Done** | Tag values are ours; `TagIOAccept/Recv/Send`, `TagChildExit` added. |
| §4.3 `Ctx` | **Partial** | Done: `SendRaw`, `Send[P]`, `SendAttach`, `Spawn`, `RegisterTimer` (+`CancelTimer`), `Listen`, `IOAccept/IORecv/IOSend`, `CloseFD`, `OwnedFD`, `LocalPort`, `IsShuttingDown`, `ShardID`, `Now`. **Missing:** `Call`/`Reply`, logging, `KeyToShard`, `IPv4/IPv6` helpers, socket option/bind/shutdown calls, `IOWrite`/`IOSendTo`/`IOSendFile`, staged send buffers, `SupervisionGroupID`/`TypeConfig`. |
| §4.4 boot and `SystemSpec` | **Partial** | `NewSystem` validates (power-of-two sizes, id and slot limits, ring size, group ids). Added `MaxFDs`, `ResetMax`, `ResetWindow`. **Missing:** `GCConfig`, `ShardMemoryLimit`, `Mode`, `QuarantinePolicy`, `InitTimeout`/`ShutdownTimeout`. |
| §5 memory and GC | **Partial** | Done: no arenas, slab zeroing on teardown, attachments, pointer-free payloads. **Missing:** GC policies, memory limit and pressure shedding, `Development` poisoning, per-type live-bytes stats. |
| §6 execution engine | **Done (subset)** | Tick phases: inbound, I/O events, timers, dispatch, publish. FIFO ready queue (no per-type batching), no log phase, no idle spin or `timerfd` (idle blocks in `epoll_wait` with a millisecond timeout). |
| §7.1-7.2 shared region, eventfd wake-ups | **Not built** | Rings are in-process. |
| §7.3 reactor | **Partial** | epoll only. io_uring, `SCM_RIGHTS` fd handoff between processes, reactor-owned slots: not built (`HandoffFD` here means *ownership transfer to an isolate*). |
| §8 supervision | **Mostly done** | Flat groups (no tree); budget exhaustion goes straight to a Level-2 reset. Level 3 is only Prefork's parent respawning a dead worker. No watchdog, no `ginactl`; graceful shutdown is `System.Shutdown`, not signal-driven (SIGTERM exits immediately). |
| §9 simulation | **Mostly done** | Missing: message delay/duplication/reorder, timer skew, user-registered checkers, generation-monotonicity and fairness checkers, `GINA_SEED` (the seed is a parameter and is printed on failure). |
| §10 quality gates | **Partial** | Present: unit, layout, simulation, engine alloc-free (`Step`, parser), rule lint, HTTP/TLS integration. **Missing:** GC soak, escape-analysis diff, automated multi-process tests (Prefork kill/respawn was verified by hand), long seed sweeps. |
| §11 layout | **Simplified** | The engine is one package, not the planned `internal/*` split (see "As-built layout"). |

### Deviations from the sections below (deliberate)

| Spec | As built |
|---|---|
| §3, §7, §8.2 L3: process per shard, shm rings, launcher, watchdog | Shards in a `System` share one thread. `gina.Prefork` gives multi-core with independent shared-nothing processes that **cannot message each other**. |
| §7.3 reactor-owned I/O slots | Buffers belong to the isolate (`ctx.IORecv(fd, buf)`); the GC keeps them alive while an operation is in flight. A completion is pushed to the *front* of the mailbox (one spare mailbox slot guarantees room) and carries a byte count or `-errno`. |
| §6.1 timer wheel | Indexed binary heap with O(log n) cancel; deterministic tie-break by registration order; I/O timeouts complete operations with `-ETIMEDOUT`. |
| §6.1 batching by type | Plain FIFO ready queue. |
| §8.1 group tree | Flat groups. |
| §4.3 `SendResult` | Adds `RingFull`, `PayloadTooLarge`; `SpawnError` adds `BadFD`. |
| Handles after a Level-2 reset | `System.BootHandle` returns the current handle of boot isolate *i*. |
| (not in the original plan) | HTTP and TLS extensions; `Prefork`; `bench/`. TLS is implemented from the protocol because the stdlib cannot run inside an isolate. |

### Measured results (details and caveats: `docs/BENCHMARKS.md`)

| Microbenchmark (one core) | Result |
|---|---|
| Cross-shard ping-pong round (2 messages) | 92 ns, 0 allocs |
| Isolate spawn + message + exit | 52 ns, 0 allocs (an init-args escape was found and fixed by the benchmark) |
| HTTP request parse | 199 ns, 0 allocs |
| TLS server flight: ECDSA P-256 / Ed25519 / RSA-2048 | 82 µs / 77 µs / 670 µs |
| TLS record layer, AES-128-GCM / AES-256-GCM | 3.8 GB/s / 3.4 GB/s |

Against Go `net/http` on the same cores (1 to 8, loopback, 256 connections, Gina as N worker processes): **1.5-1.85x** keep-alive throughput (HTTP and HTTPS), **2.8-5.3x lower p99**, **1.6-2.7x** TLS handshakes per second, **3.8x** (HTTP) and **1.9x** (HTTPS) on a 64 KiB echo, but **4-8x more memory** (each worker carries its own runtime, about 7 MB idle). Plain connection setup is roughly on par and noisy (Gina's listener accepts one connection per tick per shard). `net/http` is a far more complete server; this is not a feature-equal comparison.

### Verification, and what is not verified

- **Automated:** 63 tests, including supervision matrices, deterministic simulation sweeps, backpressure, HTTP parser tables plus a 300k-input seeded mutation test, real-socket HTTP and HTTPS integration tests (Go's `crypto/tls` client driven cooperatively on one thread), and TLS interop across 4 key types x 2 cipher orders x 2 curves with payloads to 100 KB. A negative control (a deliberately wrong HKDF label) is caught by the interop tests.
- **By hand:** `curl` (OpenSSL 3.5) and `openssl s_client` against the HTTPS example (TLS 1.3 handshake, 200 KB echo byte-identical, TLS 1.2 refused with alert 70, plain HTTP on the TLS port closed); `Prefork` with 4 and 8 workers (even connection spread, a killed worker respawned, no orphans after the parent dies).
- **Not verified:** the TLS code has had **no security audit** and should not guard anything that matters; no GC soak or leak test over long runs; no automated multi-process tests; Go's native fuzzer stalled in the sandbox, so parser fuzzing relies on the seeded mutation test; benchmarks are single-machine, loopback, short runs.

### Known limitations

TLS: no TLS 1.2, ChaCha20-Poly1305, HelloRetryRequest, resumption, 0-RTT, client certificates; handshake crypto runs inline on the shard thread (about 82 µs ECDSA, 670 µs RSA-2048). HTTP: no chunked request bodies (501), no HTTP/2, handlers are synchronous (no waiting on other isolates until `Call`/`Reply` exists). Engine: one accept per tick per listener; no cross-process messaging; Linux-only I/O.

### Suggested next steps, in rough priority order

1. `Call`/`Reply` with timeouts (unblocks asynchronous HTTP handlers) and `KeyToShard`.
2. Determinism and type-aware lint rules (§2.3, §9.2) so the simulator's guarantees are enforced, not conventional.
3. Batched accept; automated Prefork integration tests (kill/respawn, parent death).
4. `gctune`: memory limit, pressure shedding, GC policy, then a soak test.
5. The shared-memory transport and launcher (§7.1-7.2, §8.2 L3) if cross-process messaging is wanted; `ginactl` and a watchdog with it.
6. TLS hardening: independent review, HelloRetryRequest, tickets; optionally ChaCha20 via `x/crypto`.
7. HTTP: chunked bodies, streaming; Datastar SDK; the docs set; io_uring.

---

## 1. Goals and non-goals

### Goals
1. Reproduce Tina's programming model in Go: **isolates** (state machines) that return **effects**, run by **shards** (one per core), exchanging fixed-size messages, supervised Erlang-style, deterministically testable.
2. Keep Tina's constraints where they still make sense on a GC: **bounded resources by count** (slots, mailboxes, pools, rings), no shared mutable state between shards, no raw pointers in user code (generational handles), explicit backpressure (`MailboxFull`, `PoolExhausted`, …), "let it crash", and an allocation-free *engine* hot path. **Memory is managed by the Go garbage collector; there are no arenas** (§5).
3. **Hard rule: no goroutines and no channels** (§2).

### Non-goals (v1)
- Windows and macOS backends (Linux only; the backend interface leaves room for kqueue).
- Matching Odin's raw performance or hard real-time latency. We aim for an allocation-free engine hot path and short, per-shard GC pauses, not C parity.
- API compatibility with upstream. The shape is the same, the surface is Go-idiomatic.

## 2. The "no goroutines, no channels" rule

### 2.1 Definition (enforceable)
In all code under this repository and in every dependency we link:
- no `go` statement;
- no `chan` type, `make(chan …)`, `<-`, `select`, or `range` over a channel;
- no package whose API requires channels or that starts goroutines on our behalf.

Denylist (checked in CI): `os/signal`, `context`, `net/http`, `net/rpc`, `os/exec` (use `syscall.ForkExec`), `time.After/AfterFunc/Tick/NewTimer/NewTicker`, `sync.WaitGroup`, `golang.org/x/sync/*`. `sync.Mutex`/`sync.Pool` are not needed and are banned by convention (shared-nothing). `sync/atomic`, `unsafe`, `reflect` (boot-time only), `syscall`, `golang.org/x/sys/unix`, `math/bits`, `time.Now`/`time.Duration`, `os` (args/env only) are allowed.

### 2.2 What the rule does not cover
The Go runtime has its own goroutines and threads (GC workers, `sysmon`, finalizers). They cannot be removed and are out of scope. The rule is about *our* program: every line of Gina code runs on the one main goroutine of each process. §5.5 tunes the GC (`GOMAXPROCS`, memory limit, idle-time collection).

### 2.3 Enforcement
- `cmd/ginalint`: a `go/parser` + `go/types` checker that fails on any construct above in this module, plus `go list -deps` against the denylist. Runs in CI and as a `go test` in the root package.
- A second lint bans `range` over maps and `math/rand`, `time.Now` in the deterministic core (§9.2).

## 3. The central design problem and the decision

Tina gets multi-core parallelism from **OS threads pinned to cores**. In Go the only way to get a user thread is a goroutine, which is forbidden. Options:

| Option | Parallelism | Verdict |
|---|---|---|
| A. Single thread, all shards cooperatively in one process | None | Used for the **simulator** and `-shards-inline` dev mode only |
| B. Raw `clone(2)` threads | Yes | Rejected: corrupts Go runtime assumptions |
| **C. One OS process per shard, shared-memory rings** | Yes | **Chosen for production** |

Process-per-shard also keeps Tina's shared-nothing property literal, and gives stronger fault isolation than Tina's `sigaltstack`/`siglongjmp` trap.

```
                 launcher process (role=launcher; runs no isolates)
   ┌─ memfd shared region ──────────────────────────────────────────┐
   │ header │ shard ctl blocks │ N×(N-1) SPSC rings │ log rings      │
   └────────┬──────────────────────┬────────────────────────────────┘
            │ mmap MAP_SHARED      │
     ┌──────┴─────┐          ┌─────┴──────┐
     │ shard proc │ ◄─ring──►│ shard proc │ …   one per core, pinned
     │ (role=shard)│         │            │     own Go heap + own GC
     └────────────┘          └────────────┘     own epoll/timerfd/eventfd
```

The same binary is re-exec'd (`/proc/self/exe`, env `GINA_ROLE`, `GINA_SHARD_ID`) so isolate types registered in `main()` exist in every process. Each shard process has its **own heap and its own garbage collector**; a GC cycle in one shard never pauses another. The only memory shared between processes is the byte-oriented message transport (§7.1), never isolate state.

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

Result enums are copied from upstream: `SendResult{Ok, MailboxFull, PoolExhausted, StaleHandle, AttachNotLocal}`, `ReplyResult`, `CallResult{…, TargetQuarantined}`, `SubmitResult{…, NoStagingSlot, PayloadTooLarge}`, `SpawnError{SlotsFull, GroupFull, GroupNotAllocated, TypeNotAllocated, InitFailed, MemoryPressure}`, `ExitKind{Normal, Crashed, Shutdown}`, `RestartType{Permanent, Transient, Temporary}`. All result-returning functions are annotated in docs as must-check; `ginalint` flags discarded results unless assigned to `_`.

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
- **Shared-nothing is preserved by the process model.** Each shard is its own process with its own heap and its own GC. A cross-shard Go pointer is impossible, and a GC cycle in one shard never pauses another. Small independent heaps are what make the GC cheap here.

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

### 5.5 GC integration (per shard process)
Boot-time settings, all overridable in `SystemSpec.GC`:

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

### 7.1 Shared region
This is the **only** memory shared between processes, and it holds only fixed-layout bytes (rings, control blocks). It is an IPC transport, not an allocator: no isolate state and no Go pointer ever lives in it.
Created by the launcher with `memfd_create`, `ftruncate`, passed to children as inherited fd (`syscall.ForkExec` with `Files`), mapped `MAP_SHARED`. Layout: header (magic, version, **spec hash** — children refuse to start on mismatch), per-shard control block (state, `epoch`, heartbeat counter, `sleeping` flag, pid), `N×(N-1)` SPSC rings of 128-byte slots, optional per-shard log rings.

Ring cursors (`head`, `tail`) live in the shared region on separate cache lines and are accessed only with `sync/atomic` on 8-byte-aligned `*uint64`. Producer caches the consumer's head locally; consumer caches the tail. Slots are written with plain stores, published by the tail store.

### 7.2 Wake-ups
Each shard owns an `eventfd` registered in its epoll set. Before blocking, a shard sets `sleeping=1`, re-checks inbound rings (closing the race), then waits. A producer that published a batch checks the target's `sleeping` flag and writes the eventfd only if set — no syscall on the hot path under load.

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
- **FD handoff between shards** (upstream has a handoff table): since shards are processes, hand off with `SCM_RIGHTS` over a per-pair `socketpair` (`unix.Sendmsg` + `UnixRights`), registered in the receiving shard's handoff table. Default accept strategy is simpler: each shard binds its own listener with `SO_REUSEPORT`.

## 8. Supervision and fault containment

### 8.1 Tree
Groups form a tree built from the boot spec (static children + `ChildCountDynamicMax`). Strategies `OneForOne`, `OneForAll`, `RestForOne`; restart types `Permanent`, `Transient`, `Temporary`; budget `RestartCountMax` within `WindowDurationTicks`. Supervision is done by **direct function calls inside the shard**, not messages.

### 8.2 Escalation levels (mapped to Go)
| Level | Trigger | Action |
|---|---|---|
| 1 | Isolate returns `Crash`, or a **panic/fault** is recovered | Wipe slot, bump generation, supervisor applies strategy |
| 2 | Group restart budget exhausted, or live heap stays above `ShardMemoryLimit` after a forced GC (§5.5) | Tear down all isolates in the shard (zero every slot so references are dropped), reset pools, rebuild the tree from the boot spec, then `runtime.GC()` so memory is actually returned; other shards unaffected |
| 3 | Root budget exhausted, unrecoverable runtime fault (OOM, stack overflow, `fatal error`), or watchdog timeout | Shard process exits/is killed; **launcher** applies `QuarantinePolicy` |

**Trap boundary** (replaces `sigaltstack`/`siglongjmp`): the shard loop runs turns inside a function with `defer recover()` and `SetPanicOnFault(true)`, so Go panics *and* faulting `unsafe` accesses are recoverable. The loop records the current (type, slot) in a plain variable before each turn; on recovery it logs, treats that isolate as crashed (Level 1) and re-enters the loop. Messages the crashed turn had already sent stay sent **(ours)**; its staged-but-uncommitted I/O is discarded. Runtime-fatal errors cannot be recovered; those are Level 3 by design, and process isolation makes that safe for other shards.

**Watchdog** (no goroutines, no signals): each shard increments a heartbeat counter in its control block once per tick. The launcher checks heartbeats; a shard that stalls past `WatchdogTimeout` (a handler stuck in a loop) is `SIGKILL`ed.

**Launcher** (single-threaded loop on `epoll`): one `pidfd` per shard; on exit it respawns with a fresh heap and a bumped `epoch` (new generation floor so old handles to that shard go stale), subject to its own restart budget. Budget exhausted → `Quarantine`: shard stays down, cross-shard sends to it return `StaleHandle`/`TargetQuarantined`; revived by `ginactl revive <id>`. Under `Abort` the launcher exits non-zero so systemd/Kubernetes restarts the whole service.
Residual risk: generation floors are `epoch<<16`; a slot recycled >65 535 times in one lifetime could theoretically collide with a prior epoch's handle. Documented, accepted for v1.

### 8.3 Shutdown
No framework-ordered shutdown. `TagShutdown` is delivered to every isolate; natural ordering emerges (listeners stop, connections drain). `ShutdownTimeout` is the safety net, after which the launcher kills remaining shards. Trigger: `ginactl shutdown` over a Unix control socket owned by the launcher. See open question Q1 for OS signals.

## 9. Deterministic simulation testing

### 9.1 Architecture
`Simulator` runs **all shards in one process on one thread**. The shard engine is written against three seams — `Clock`, `Backend`, `Transport` — so production code paths run unchanged:
- `SimClock`: a tick counter advanced by the harness.
- `simBackend`: scripted I/O completions, injectable errors, delays.
- `simTransport`: the same SPSC ring code over ordinary memory, plus drops, delays, duplication, and partitions.
Each round: advance clock → fault engine → **shuffle shard order** (emulates production phase drift) → tick each shard → run checkers.

### 9.2 Determinism contract (lint-enforced in `internal/` and the simulator)
All randomness from a **PRNG tree**: master seed → SplitMix64 → independent child streams per domain (network, scheduling, faults, each isolate type). Generator is our own **xoshiro256\*\*** (not `math/rand`, whose stream is not part of our compatibility contract). Faults are integer `Ratio{Num, Den}`, no floats. Forbidden in the core: `range` over maps, `time.Now`, `math/rand`, `select`, reading env/clock, address-dependent ordering (e.g. sorting by pointer value), and `runtime.SetFinalizer`/`AddCleanup`/`weak`/`unique` (no behaviour may depend on GC timing). User isolates run under the same contract in the simulator: iterating a Go map in a handler makes a run non-reproducible, so `ginalint -sim` flags it in user packages. Same seed + same config ⇒ identical trace hash (FNV-1a over a canonical event stream). Failures print the seed; `GINA_SEED=…` replays.

### 9.3 Checkers
Run at intervals and at end: pool conservation (free + in-use = capacity), generation monotonicity, mailbox bounds, FD-table/handoff invariants, scheduler fairness (no ready isolate starved beyond N ticks), plus user-supplied invariants via `RegisterChecker`.

Because the engine is single-threaded and goroutine-free, simulation needs no virtual-time hacks. Determinism is a property of the design, not a layer on top.

## 10. Testing and quality gates

| Layer | What | Gate |
|---|---|---|
| Unit | slab store, bitmap, ring, pool, wheel, handle packing, PRNG known-answer vectors (xoshiro) | `go test` |
| Layout | `Message` is 128 B; rings/cursors cache-line aligned; handle bit-fields round-trip | compile-time + test |
| Simulation | supervision strategies, budgets/escalation, backpressure, timers, call/reply timeout, FD table, determinism (run twice, compare hashes), multi-seed sweeps in CI | `go test ./sim/...` |
| Engine alloc-free | `testing.AllocsPerRun` on tick/dispatch/send/timer/ring/reactor-poll with no-op handlers; budget 0 | CI |
| GC behaviour | soak: spawn/crash/tear down 10M isolates holding slices and maps; live heap stays flat (no reference leaks through slots, mailboxes, pool entries, attachments, timers). Record `/sched/pauses/total/gc:seconds` and tick-latency percentiles per `GCPolicy` against a stored baseline | CI (nightly) |
| Escape analysis | `go build -gcflags=-m` diffed for `internal/sched` hot functions | CI |
| Rule | `ginalint` (§2.3) | CI, blocking |
| Multi-process | launcher + 2–4 shards, ring ping-pong, kill -9 a shard and verify respawn + stale handles, stalled-handler watchdog | integration, Linux only |
| Race | `-race` is meaningless per-process; instead run the ring against a concurrent torture harness **in separate processes** over the shared memfd | integration |
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
  cmd/ginalint/  cmd/ginactl/
  examples/echo/  examples/dispatcher/
  extensions/http/  extensions/datastar/
  docs/{concepts,guides,reference}/
```
Dependency direction is strictly downward (`internal/slab` imports nothing of ours). (Planned: `golang.org/x/sys/unix` as the only external dependency. As built there is **no external dependency**: the standard `syscall` package is used.)

### As-built layout
```
gina/                         module "gina"; no external dependencies
  types.go spec.go slab.go ring.go timer.go shard.go supervise.go system.go sim.go   # package gina: the engine
  ctx_io.go reactor_linux.go reactor_other.go     # I/O API and the epoll reactor (stub on other OSes)
  prefork_linux.go prefork_other.go               # multi-process launcher
  gina_test.go
  internal/prng/  internal/lint/                  # PRNG tree; the no-goroutine/no-channel linter
  cmd/ginalint/
  extensions/http/   parser.go router.go context.go server.go (+ tests)
  extensions/tls/    config.go conn.go keys.go record.go alert.go (+ tests, benchmarks)
  examples/{pingpong,supervised,httpserver,https}/
  bench/             run.sh summarize.py plot.py results.csv results*.{png,svg}  nethttp/ (separate module: comparison baseline)
  docs/BENCHMARKS.md
```

## 12. Milestones

| M | Deliverable | Exit criteria | Status |
|---|---|---|---|
| M0 | `go.mod`, `ginalint`, CI, layout tests | Lint rejects `go`/`chan`/denylist fixtures | **Done** (lint is syntactic; no CI workflow in the repo) |
| M1 | slab store, handle, message, bitmap, ring, pool, wheel, PRNG | Unit tests; zero-alloc benchmarks for engine structures | **Mostly**: all but bitmap and wheel (heap instead) |
| M2 | Single-shard scheduler + effects + ctx send/spawn/timers, `simBackend`, simulator v1 | Determinism test; backpressure tests | **Done** (no `simBackend`: the simulator has no I/O) |
| M3 | Supervision L1-L2, trap boundary, call/reply, attachments | Strategy/budget sims; panic + fault recovery tests | **Mostly**: call/reply missing |
| M4 | Multi-shard in simulator (sim transport, shuffle, checkers, fault injection) | Seed sweep green in CI | **Mostly**: 30-seed sweep; fewer fault kinds than planned |
| M5 | Launcher, shm transport, eventfd wake-ups, multi-process mode, watchdog, L3 | Kill/respawn integration tests | **Replaced** by `gina.Prefork` (independent processes); the shm design is not built |
| M6 | epoll reactor, TCP echo example, FD handoff | Echo under external load; engine allocs/op = 0; GC report | **Mostly**: epoll and load tests done (HTTP instead of a TCP echo example); no fd handoff, no GC report |
| M7 | `extensions/http` and Datastar SDK | Conformance tests; dispatcher example | **HTTP done, plus TLS** (not originally planned); Datastar and dispatcher example not built |
| M8 | Optional io_uring backend; docs set; benchmarks | Backend parity suite | **Benchmarks done** (`bench/`, `docs/BENCHMARKS.md`); io_uring and the docs set not built |

## 13. Deviations from upstream (summary)
| Upstream | Gina | Reason |
|---|---|---|
| OS threads per core | Processes per core | Threads need goroutines |
| `sigaltstack`+`siglongjmp` trap | `recover` + `SetPanicOnFault`; process death = Level 3 | Idiomatic, safe in Go |
| Watchdog calls `_exit(0)` | Launcher kills/respawns the shard | Process isolation |
| `SIGUSR2` revives a quarantined shard | `ginactl revive` | Go's signal handling needs `os/signal` (banned) |
| io_uring / kqueue / IOCP | epoll (v1), io_uring (v2) | Scope; Linux only |
| No GC, arenas, pointer-free state | **GC on, no arenas**; isolate state is ordinary Go, only message payloads are pointer-free | Requested design goal; process-per-shard keeps heaps small and independent |
| Hard byte bound from a fixed arena | Bound by counts + soft `ShardMemoryLimit` + pressure shedding | A GC heap cannot be hard-capped; latency is no longer strictly deterministic |
| Odin `rawptr` + `self_as` | Generic `*T` handler param | Type safety by construction |
| Transfer buffers (shard-local) | Same-shard attachments; cross-shard data is still decomposed | GC makes arbitrary local hand-off safe |
| Tina's HTTP extension runs on its shard threads | `extensions/http` runs on isolates in one `System`; multi-core via `Prefork` + `SO_REUSEPORT` | No threads; same kernel load balancing |
| TLS via the platform library | A sans-I/O TLS 1.3 server in `extensions/tls` | Stdlib `crypto/tls` cannot run inside an isolate; **unaudited** |

## 14. Decisions and open questions

Resolved by what was built:
- **Q1 - OS signals:** the rule was kept absolute (no `os/signal`). SIGTERM/SIGINT therefore terminate the process immediately; graceful shutdown is the programmatic `System.Shutdown`. `ginactl` is not built.
- **Q2 - process vs. thread:** accepted in the form of `Prefork` (independent processes). Shards inside a process share one thread.
- **Q3 - platform scope:** Linux only for I/O and `Prefork`; the engine builds elsewhere (cross-compiled for macOS) but has no sockets there.
- **Q4 - name, license:** module `gina`. This repository's `LICENSE` is **MIT** (Manos Ragiadakos); upstream Tina is Apache-2.0. Gina reimplements the documented behaviour and copies no upstream source, so MIT is workable; if upstream code is ever copied or transliterated, Apache-2.0's attribution and NOTICE requirements apply to that code. There is no `NOTICE` file and no CI workflow in the repo yet.
- **Q5 - io_uring:** deferred.

Still open:
- **Q6 - GC defaults.** `gctune` is not built, so Go's defaults apply (each Prefork worker's runtime sizes its own `GOMAXPROCS`). Decide after the GC soak and pause measurements exist.
- **Q7 - cross-process messaging.** Is `Prefork` (no messaging between workers) enough, or is the shared-memory transport (§7.1-7.2) wanted? That choice sets how much of §3/§7/§8.2 is built next.
- **Q8 - TLS ownership.** Keep the in-repo TLS (needs a security review before any real use), or document it as development-only and terminate TLS in front of Gina?
