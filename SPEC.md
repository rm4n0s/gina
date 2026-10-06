# Gina — Specification for a Go Port of Tina

Status: draft v0.2 (arenas removed; the Go GC owns memory, see §5) · Date: 2026-10-06 · Upstream: https://github.com/pmbanugo/tina (Odin, Apache-2.0)

"Gina" is a working name for the Go port. The module path and package name are placeholders.

## 0. Basis and caveats

This spec is derived from upstream's README, `docs/concepts/*`, `docs/reference/{the_ctx_api,system_spec}.md`, and the type declarations in `src/api.odin`. I did not read the full source. Where upstream is silent I mark the item **(ours)**: a decision this port makes and is free to change. Names follow upstream where known, in Go style (`ctx_send_raw` → `gina.SendRaw`).

Upstream is Apache-2.0. Reimplementing from documented behaviour is fine. Copying or transliterating source requires keeping the license and NOTICE and marking changes.

## MVP status (implemented in this repo)

The MVP implements the programming model and the deterministic core on **one thread**: all shards are driven cooperatively by `System.Step`. It contains no goroutines and no channels, enforced by `cmd/ginalint` (also run as a `go test`).

Implemented: isolates and effects (`Done`, `Yield`, `WaitMessage`, `Crash`); typed slab storage with generational 28-bit handles; the 128-byte `Message` with compile-time size check and pointer-free, padding-free payload validation; mailboxes, a message pool with a protected system reserve; N×(N-1) staged/published SPSC rings between shards; timers; spawn with args; same-shard attachments; supervision (one-for-one, one-for-all, rest-for-one, permanent/transient/temporary, sliding restart budget, `TagChildExit`, Level-2 shard reset, quarantine and revive); a panic/fault trap boundary (`recover` + `SetPanicOnFault`); ordered shutdown; invariant checkers; and the simulator (seeded PRNG tree, simulated clock, shuffled shard order, drop/crash/partition faults, FNV trace hash).

**Deviations from the sections below (deliberate, for the MVP):**

| Spec | MVP |
|---|---|
| §3, §7, §8.2 L3: process per shard, shm rings, launcher, watchdog | Not built as specified. Shards inside a `System` share one thread. Multi-core is provided by `gina.Prefork` instead: N independent shared-nothing processes (re-exec of the binary, `wait4` supervision, `PDEATHSIG`), which cannot message each other. The ring code keeps the staged/published discipline so it can move to shared memory later. |
| §7.3 epoll/io_uring reactor, `WaitIO`, FD handoff | epoll reactor and `WaitIO` are built (Linux; other OSes build without I/O). io_uring and fd handoff are not. |
| §7.3 reactor-owned I/O slots | Buffers are owned by the isolate (`ctx.IORecv(fd, buf)`), since the GC keeps them alive while an operation is in flight. A completion is pushed to the *front* of the mailbox (one spare mailbox slot guarantees room) and carries only the byte count or `-errno`. |
| Timers | Indexed heap with O(log n) cancel; I/O operations can carry a timeout that completes them with `-ETIMEDOUT`. |
| `WaitReply`, `Call`/`Reply` | Not built. |
| §6.1 batching by type | Plain FIFO ready queue. |
| §6.1 timer wheel | Binary heap (deterministic tie-break by registration order). |
| §8.1 group tree | Flat groups; budget exhaustion escalates straight to a Level-2 shard reset. |
| §5.5 GC policies / memory pressure | Go defaults only; no `gctune`. |
| §5.2 `Development` poisoning, logging ring, `ginactl` | Not built. |
| §4.3 `SendResult` | Adds `RingFull` and `PayloadTooLarge`. |
| Handles after a Level-2 reset | `System.BootHandle` returns the current handle of boot isolate *i*. |

**I/O and HTTP (added after the first MVP):**

- `ctx.Listen` (optional `SO_REUSEPORT`), `ctx.IOAccept/IORecv/IOSend` + `WaitIO()`, fd ownership (`SpawnSpec.HandoffFD`; a socket is closed when its owner dies), per-operation timeouts, cancellation of in-flight operations when an isolate dies or shutdown starts. Readiness-based epoll (edge-triggered) presented with completion semantics: an operation is attempted immediately and only parked on `EAGAIN`.
- `gina.Prefork(n, opts)`: re-executes the binary n times; combined with `ReusePort` every worker binds the same port and the kernel balances connections (the deployment Tina's HTTP extension uses). The parent runs no isolates; it restarts a dead worker (budgeted) and workers die with the parent.
- `extensions/http`: HTTP/1.1 server on isolates (one listener per shard, one isolate per connection). Strict allocation-free parser (smuggling, bare-LF, obs-fold, oversize and duplicate-header defences, `Content-Length` bodies, pipelining, keep-alive), router (literals, `:params`, trailing `*`, 405 + `Allow`, HEAD via GET), response context, idle/read/write timeouts that a slow client cannot extend, connection shedding at `MaxConns`, handler panics answered with 500, graceful shutdown. **Not supported:** chunked request bodies (501), TLS, HTTP/2, handlers that wait on other isolates (handlers are synchronous; `Context.Gina()` allows fire-and-forget messages).

Measured on this machine (`go test -bench`): a two-shard ping-pong runs at about 87 ns per round (two messages) with 0 allocs/op, i.e. roughly 11M cross-shard messages/s on one thread. This is an in-process figure, not a multi-process one.

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
Dependency direction is strictly downward (`internal/slab` imports nothing of ours). `golang.org/x/sys/unix` is the only external dependency.

## 12. Milestones

| M | Deliverable | Exit criteria |
|---|---|---|
| M0 | `go.mod`, `ginalint`, CI, layout tests | Lint rejects `go`/`chan`/denylist fixtures |
| M1 | slab store, handle, message, bitmap, ring, pool, wheel, PRNG | Unit tests; zero-alloc benchmarks for engine structures |
| M2 | Single-shard scheduler + effects + ctx send/spawn/timers, `simBackend`, simulator v1 | Determinism test; backpressure tests |
| M3 | Supervision L1–L2, trap boundary, call/reply, attachments | Strategy/budget sims; panic + fault recovery tests |
| M4 | Multi-shard in simulator (sim transport, shuffle, checkers, fault injection) | Seed sweep green in CI |
| M5 | Launcher, shm transport, eventfd wake-ups, multi-process mode, watchdog, L3 | Kill/respawn integration tests |
| M6 | epoll reactor, TCP echo example, FD handoff | Echo under external load; engine allocs/op = 0; GC pause and latency report per policy, with `GOMAXPROCS`/`GCPolicy` defaults chosen from the data |
| M7 | `extensions/http` (HTTP/1.1, zero-copy parse on I/O slots, keep-alive isolates) and Datastar SDK (SSE + signal parsing) | Conformance tests; dispatcher example |
| M8 | Optional io_uring backend; docs set; benchmarks | Backend parity suite |

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

## 14. Open questions (need your decision)
- **Q1 – OS signals.** Without `os/signal`, SIGTERM/SIGINT take Go's default (immediate exit), so graceful shutdown is only via `ginactl`. Option: a build tag `gina_ossignal` that adds a tiny bridge using `os/signal` and violates the rule, off by default. Allow it, or keep the rule absolute?
- **Q2 – Process vs. thread expectation.** Is "one process per shard" acceptable as the meaning of "shard", given Go cannot create threads without goroutines? If not, the only compliant alternative is single-threaded (no multi-core).
- **Q3 – Platform scope.** Linux-only for v1 OK? (macOS needs a kqueue backend and no `memfd`/`eventfd`/`pidfd`; Windows needs a different shared-memory/process design.)
- **Q4 – Name, module path, license.** Keep "gina"? Apache-2.0 to match upstream?
- **Q5 – io_uring.** In scope for v1 (M8) or deferred?
- **Q6 – GC defaults.** One P pinned to one core (simplest; GC time-slices with the shard) versus two Ps per shard (GC gets its own core, double the cores). I propose deciding from M6 measurements. OK?
