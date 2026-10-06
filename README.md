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

Scaling across cores works like Tina's `SO_REUSEPORT` setup: `gina.Prefork(n, ...)` re-executes the binary n times, every worker binds the same port with `ReusePort: true`, and the kernel balances connections. Shards inside one process also each bind their own listener. Workers are shared-nothing processes, so they cannot message each other.

Measured on a 32-thread box with the load generator on the same machine (so treat as indicative): 1 process about 150-295k req/s; 4 workers about 460k; 8 workers about 1.09M, 0 failures.
