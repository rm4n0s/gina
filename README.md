# Gina

A Go port of [pmbanugo/tina](https://github.com/pmbanugo/tina)'s concurrency model, built **without goroutines or channels**. See [SPEC.md](SPEC.md) (the "MVP status" section lists what exists).

```go
type counter struct{ n int }

sys, _ := gina.NewSystem(gina.SystemSpec{
	Types:  []gina.TypeDesc{gina.RegisterType(1, gina.TypeOptions{SlotCount: 16}, nil,
		func(self *counter, ctx *gina.Ctx, m *gina.Message) gina.Effect { self.n++; return gina.WaitMessage() })},
	Shards: make([]gina.ShardSpec, 2),
}, gina.Options{})
h, _ := sys.Spawn(0, gina.SpawnSpec{Type: 1, Group: gina.GroupNone})
sys.Send(h, gina.TagUserBase, nil)
sys.RunUntilIdle(10)
```

```
go test ./...                    # engine, supervision, simulator, linter, allocation checks
go run ./cmd/ginalint ./...      # fails on go statements, channels, select, denylisted imports
go run ./examples/pingpong       # two shards, 1M cross-shard messages
go run ./examples/supervised     # panics -> restarts -> budget -> Level-2 reset
go run ./examples/httpserver -port 8080 -workers 8   # HTTP server, 8 processes on one port
go run ./examples/httpserver -port 8443 -tls          # HTTPS with a throwaway certificate
go run ./examples/https                           # HTTPS on :8443 + HTTP on :8080 redirecting to it
```

Simulation: `gina.NewSim(spec, seed, cfg)` runs the same engine on a simulated clock with shuffled shard order, fault injection and invariant checks. The same seed always produces the same `Trace.Hash()`.

## HTTP

`extensions/http` is an HTTP/1.1 framework built on isolates (no `net/http`):

```go
r := ghttp.NewRouter()
r.GET("/hello/:name", func(c *ghttp.Context) { c.String(200, "Hello, "+c.Param("name")+"\n") })

srv := ghttp.New(ghttp.Config{Port: 8080, ReusePort: true}, r)
spec := gina.SystemSpec{Shards: make([]gina.ShardSpec, 1)}
srv.Install(&spec)                       // adds the listener + connection isolates
sys, _ := gina.NewSystem(spec, gina.Options{})
sys.RunUntilIdle(1 << 62)                // serves until killed
```

### HTTPS

```go
cert, _ := gtls.SelfSigned("localhost")            // dev only; or gtls.LoadX509KeyPair("cert.pem", "key.pem")
srv := ghttp.New(ghttp.Config{Port: 8443, TLS: &gtls.Config{Certificates: []ctls.Certificate{cert}}}, r)
```

`extensions/tls` is a sans-I/O **TLS 1.3** server written for Gina (the standard `crypto/tls` needs a blocking connection and a goroutine per connection, so it cannot run inside an isolate). It uses stdlib crypto primitives and implements the protocol only. TLS 1.3, AES-GCM, X25519/P-256, ECDSA/Ed25519/RSA-PSS certificates, ALPN, SNI, KeyUpdate. No TLS 1.2, ChaCha20, session resumption or client certificates (see the package comment). It has been tested against Go's TLS client and OpenSSL, but has not had a security audit.

Scaling across cores works like Tina's `SO_REUSEPORT` setup: `gina.Prefork(n, ...)` re-executes the binary n times, every worker binds the same port with `ReusePort: true`, and the kernel balances connections. Shards inside one process also each bind their own listener. Workers are shared-nothing processes, so they cannot message each other.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="bench/results-dark.png">
  <img src="bench/results.png" alt="Six charts comparing Gina with Go net/http on 1 to 8 cores. Gina is 1.5 to 1.8 times faster on keep-alive GET over HTTP and HTTPS, completes 1.6 to 2.7 times more TLS handshakes per second, has a 2.8 to 5.3 times lower p99 latency, is 3.8 times faster on a 64 KiB echo over HTTP and 1.9 times over HTTPS, but uses 4 to 8 times more memory at 8 cores.">
</picture>

Benchmarks against `net/http` (same cores, 1 to 8): Gina is about 1.5-1.85x faster on keep-alive requests with a 3-6x lower p99, 1.6-2.7x on TLS handshakes, but uses more memory. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for the method, microbenchmarks and caveats; reproduce with `bench/run.sh`.
