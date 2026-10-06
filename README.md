# Gina

A Go port of [pmbanugo/tina](https://github.com/pmbanugo/tina)'s concurrency model: **thread-per-core shards** (each a goroutine locked to an OS thread, optionally pinned to a core) that own their isolates, memory pool, timers and sockets, and exchange 128-byte messages through **lock-free SPSC rings**. Goroutines and channels are confined to one file (the thread host) and the tests that opt in; everything else is single-threaded per shard. See [SPEC.md](SPEC.md) (the "Implementation status" section lists what exists and what does not).

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
```

Simulation: `gina.NewSim(spec, seed, cfg)` runs the same engine cooperatively on one thread with a simulated clock, shuffled shard order, fault injection and invariant checks. The same seed always produces the same `Trace.Hash()`. (Threaded runs are not deterministic.)

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
  <img src="bench/results.png" alt="Six charts comparing Gina (shard threads in one process, and worker processes) with Go net/http on 1 to 8 cores. Gina is 1.6 to 1.9 times faster on keep-alive GET over HTTP and HTTPS, completes 1.7 to 2.9 times more TLS handshakes per second, has a 2.6 to 5.6 times lower p99 latency, and is 4.0 times faster on a 64 KiB echo over HTTP and 2.2 times over HTTPS. Threads and processes perform the same; threads use about as much memory as net/http on HTTP and about three times as much on HTTPS, processes four to six times as much.">
</picture>

Benchmarks against `net/http` (same cores, 1 to 8): Gina's shard threads are about 1.6-1.9x faster on keep-alive requests with a 2.6-5.6x lower p99, and complete 1.7-2.9x more TLS handshakes. One process with N threads performs the same as N worker processes while using far less memory (21 vs 74 MB at 8 cores on HTTP). See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for the method, microbenchmarks and caveats; reproduce with `bench/run.sh`.
