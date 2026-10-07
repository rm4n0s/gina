# Benchmarks

Measured 2026-10-07. Reproduce with `bench/run.sh` (about 10 minutes for the full matrix; `PROTO=h2 bench/run.sh` for the HTTP/2 section) and `bench/summarize.py bench/results.csv`; raw data is in [bench/results.csv](../bench/results.csv), the figure is rebuilt by `python3 bench/plot.py` (`python3 bench/plot.py h2` for the HTTP/2 one).

**Read the caveats at the bottom before quoting any number.**

![Gina vs net/http: throughput, TLS handshakes, p99 latency, 64 KiB echo and memory](../bench/results.png)

*The tables below are the table view of the same data.*

## Setup

| | |
|---|---|
| Machine | AMD Ryzen AI MAX+ 395, 16 cores / 32 threads (SMT), two core complexes, Linux 7.2, Go 1.26, CPU governor `powersave` |
| Server | confined with `taskset` to physical cores `0..N-1` (SMT siblings left idle) |
| Load generator | `oha` 1.16, confined to physical cores 8-15 (the other complex), so it never shares a core with the server |
| **Gina** | `examples/httpserver -shards N -pin`: **one process, N shard threads** pinned to cores, one `SO_REUSEPORT` listener each, cross-shard rings. Tina's model |
| Baseline | Go `net/http` (`bench/nethttp`, a separate module because it uses goroutines), `GOMAXPROCS=N`, same routes and bodies, HTTP/1.1 only, TLS 1.3 only, ECDSA P-256, session tickets disabled |
| All servers get | the same N cores. 256 concurrent connections (64 for the echo test), 1 s warm-up, then 2 runs of 4 s; the **median** is reported |
| Traffic | loopback, HTTP/1.1, `GET /hello/bench` (14-byte body); TLS rows use `--insecure` against a self-signed certificate |

## Microbenchmarks (one core, `go test -bench`, 2 runs, spread under 2%)

| Benchmark | Result |
|---|---|
| Cross-shard round, cooperative driver (2 messages, atomic rings) | 111 ns, **0 allocs** (92 ns before the rings used atomics) |
| Cross-**thread** hop, two pinned shard threads, default idle spin (`examples/pingpong`) | **0.16 µs** per hop (6.2M hops/s); 0.21 µs unpinned |
| Cross-thread hop when every hop must sleep and be woken through the eventfd (`SpinFor < 0`) | 2.0 µs |
| Isolate spawn + message + exit | 52 ns, **0 allocs** |
| HTTP request parse (6 headers) | 193 ns, 700 MB/s, **0 allocs** |
| TLS server flight, ECDSA P-256 / Ed25519 / RSA-2048 | 81 µs / 76 µs / 663 µs |
| TLS record layer, AES-128-GCM / AES-256-GCM, 16 KiB seal + open | 3.8 GB/s / 3.5 GB/s |

Benchmarks have already paid for themselves twice: one found a hidden allocation per spawn (fixed), another showed that the first cross-thread numbers were really measuring a 5 s shutdown grace period, not the hops.

## Gina vs net/http

#### Keep-alive GET (14-byte response) - HTTP

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 171,934 | 110,136 | 1.56x | 1.78 / 4.65 | 10 / 17 |
| 2 | 367,016 | 194,432 | 1.89x | 0.87 / 4.38 | 12 / 18 |
| 4 | 695,561 | 392,882 | 1.77x | 0.48 / 2.47 | 15 / 16 |
| 8 | 1,211,112 | 643,714 | 1.88x | 0.35 / 1.95 | 21 / 19 |

#### Keep-alive GET (14-byte response) - HTTPS (TLS 1.3)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 152,642 | 97,409 | 1.57x | 1.99 / 5.13 | 26 / 22 |
| 2 | 328,616 | 177,526 | 1.85x | 0.95 / 5.41 | 39 / 23 |
| 4 | 624,862 | 364,238 | 1.72x | 0.54 / 2.52 | 52 / 24 |
| 8 | 1,060,734 | 618,704 | 1.71x | 0.41 / 1.88 | 76 / 25 |

#### New connection per request (connection setup / TLS handshake bound) - HTTP

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 47,396 | 46,907 | 1.01x | 12.46 / 10.07 | 14 / 14 |
| 2 | 79,282 | 86,878 | 0.91x | 8.65 / 8.19 | 20 / 15 |
| 4 | 214,118 | 128,844 | 1.66x | 4.55 / 5.08 | 34 / 16 |
| 8 | 283,410 | 141,886 | 2.00x | 1.65 / 3.38 | 46 / 17 |

#### New connection per request (connection setup / TLS handshake bound) - HTTPS (TLS 1.3)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 8,720 | 5,196 | 1.68x | 30.70 / 122.04 | 32 / 18 |
| 2 | 16,609 | 9,314 | 1.78x | 32.12 / 64.55 | 39 / 16 |
| 4 | 31,021 | 13,912 | 2.23x | 28.23 / 57.20 | 61 / 16 |
| 8 | 50,350 | 17,426 | 2.89x | 17.46 / 45.66 | 111 / 18 |

#### POST /echo with a 64 KiB body, keep-alive - HTTP

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 215,599 | 53,806 | 4.01x | 0.44 / 5.80 | 34 / 18 |

#### POST /echo with a 64 KiB body, keep-alive - HTTPS (TLS 1.3)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 81,111 | 37,475 | 2.16x | 1.06 / 8.91 | 68 / 20 |

Failed requests across all 72 measured runs: 0

Memory is the resident memory of the server process at the end of the run.

## What the numbers say

- **Memory:** versus net/http, Gina is about equal on plain HTTP (21 vs 19 MB at 8 cores) and about 3x on HTTPS (76 vs 25 MB; TLS connections hold a 16 KiB read buffer each plus handshake garbage). All shard threads share one Go heap, so memory grows slowly with cores (10 MB on 1 core, 21 MB on 8).
- **Versus net/http, keep-alive throughput is 1.6-1.9x** at every core count, over HTTP and HTTPS, and scales almost linearly (172k to 1.21M req/s from 1 to 8 cores). **p99 is 2.6-5.6x lower** (0.35 ms vs 1.95 ms at 8 cores).
- **TLS handshakes:** 1.7-2.9x as many as net/http, about 8.7k/s per core and 50k/s on 8 cores. This is mostly ECDSA signing plus key exchange (81 µs measured above); net/http stops scaling near 17k/s here.
- **Large bodies:** a 64 KiB echo is **4.0x** faster over HTTP and **2.2x** over TLS, with a far better p99 (0.37 ms vs 5.8 ms over HTTP).
- **Plain connection setup:** roughly on par and **noisy** (up to 1.22x run-to-run). This workload is dominated by the kernel's accept/SYN path, and Gina's listener accepts one connection per scheduler tick per shard, a known limit.
- **The shared garbage collector did not show up.** In thread mode all shards share one heap and collector, so a collection pauses every shard, but p99 stayed low in these runs (0.35 ms at 8 cores). That is a weak test (tiny responses, little live data, no allocation-heavy handlers); see the caveats.

## HTTP/2

Measured 2026-10-07 with `PROTO=h2 bench/run.sh` (raw data: [bench/results-h2.csv](../bench/results-h2.csv); tables from `python3 bench/summarize.py bench/results-h2.csv HTTP/2`). Same machine, core pinning, `oha`, run lengths (1 s warm-up, 2 runs of 4 s, median) and routes as above. **Gina** is `examples/http2 -shards N -pin`: one process, N shard threads, one `SO_REUSEPORT` listener each. The **baseline** is the same `bench/nethttp` server started with `-h2`: Go `net/http` serving HTTP/2 only (h2c with prior knowledge, or h2 over TLS 1.3 with ECDSA P-256 and tickets disabled), `GOMAXPROCS=N`. Both speak HTTP/2 only, so each connection is HTTP/2 from its first byte.

The load is **64 connections with 4 concurrent streams each** (`oha -p 4`), i.e. the same 256 requests in flight as the HTTP/1.1 runs but over a quarter as many sockets, and 16 connections x 4 streams for the 64 KiB echo. Do not compare these rows with the HTTP/1.1 tables above: the load shape is different.

![Gina vs net/http over HTTP/2: throughput, 64 KiB echo, p99 latency and memory](../bench/results-h2.png)

*The tables below are the table view of the same data.*

#### Keep-alive GET (14-byte response) - HTTP/2 (h2c)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 544,790 | 83,264 | 6.54x | 0.55 / 5.73 | 12 / 17 |
| 2 | 1,053,649 | 155,310 | 6.78x | 0.34 / 5.35 | 19 / 20 |
| 4 | 1,792,738 | 299,698 | 5.98x | 0.23 / 3.30 | 26 / 20 |
| 8 | 1,894,464 | 477,462 | 3.97x | 0.17 / 2.61 | 41 / 20 |


#### Keep-alive GET (14-byte response) - HTTP/2 over TLS 1.3

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 497,266 | 79,350 | 6.27x | 0.63 / 6.14 | 18 / 18 |
| 2 | 985,658 | 151,770 | 6.49x | 0.38 / 5.52 | 25 / 21 |
| 4 | 1,637,422 | 289,810 | 5.65x | 0.26 / 3.55 | 39 / 22 |
| 8 | 1,769,469 | 445,908 | 3.97x | 0.21 / 2.90 | 62 / 22 |


#### POST /echo with a 64 KiB body, keep-alive - HTTP/2 (h2c)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 146,409 | 33,882 | 4.32x | 0.67 / 6.86 | 26 / 22 |


#### POST /echo with a 64 KiB body, keep-alive - HTTP/2 over TLS 1.3

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 76,556 | 26,802 | 2.86x | 1.28 / 8.40 | 50 / 24 |

Failed requests across all 40 measured runs: 0. Run-to-run spread over the 20 configurations: median 1.007x, worst 1.06x. The server's own request counter matched `oha`'s total to within the handful of probe requests in a cross-check.

**What the numbers say**

- **Keep-alive GET: 4.0-6.8x the throughput of `net/http` over h2c and 4.0-6.5x over TLS**, with a p99 10x lower at one core (0.55 ms vs 5.7 ms) and 15x lower at eight (0.17 ms vs 2.6 ms). One shard thread serves about 545k req/s.
- **Why so large a gap is not measured here.** HTTP/2 in `net/http` costs more per request than its HTTP/1.1 path (it dropped from 110k to 83k req/s per core), while Gina's h2 path gets faster per request than its HTTP/1.1 path, because each read and write carries several streams. The likely causes on Go's side (goroutine per stream, framer and writer scheduling, allocation) were not profiled.
- **64 KiB echo:** 4.3x over h2c, 2.9x over TLS, with the p99 about 10x (h2c) and 6.6x (TLS) lower.
- **Memory:** at or below `net/http` at one core and higher as cores grow: 12-41 MB (h2c) and 18-62 MB (TLS) for Gina against 17-20 MB and 18-22 MB. Every shard thread holds its own connection buffers, and TLS connections hold a 16 KiB ciphertext buffer each.

**HTTP/2 caveats** (the general ones below apply too)

- **Gina's 4- and 8-core rows are probably limited by the load generator.** Going from 4 to 8 cores adds only 6% (1.79M to 1.89M req/s) after near-linear scaling up to 4, so the 8-core ratios (4.0x) are lower bounds. The `net/http` rows are not affected.
- **New-connection-per-request is not measured.** `oha` ignores `--disable-keepalive` over HTTP/2 (verified: no `TIME_WAIT` sockets after 300k requests), so that scenario would just repeat the GET one. TLS handshake cost is the same code path as in the HTTP/1.1 tables.
- **Not a feature-equal comparison.** Gina's HTTP/2 has no server push, prioritisation, trailers or event streams, never indexes response headers, and has not had a security audit or an h2spec run. `net/http` is the more complete and hardened server, and some of the speed is the missing generality.
- **One workload:** a 14-byte response, no handler work, loopback, one machine, small headers.

## Caveats

- **Not a feature-equal comparison.** `net/http` is a hardened, complete server (chunked bodies, `Expect: 100-continue`, HTTP/2 elsewhere, years of edge-case fixes). Gina's HTTP is deliberately minimal: no chunked request bodies, HTTP/2 only in a separate server, a hand-written parser and a TLS 1.3 implementation that has had no security audit and lacks resumption and ChaCha20. Some of the speed is the missing generality.
- **Different concurrency models.** Gina's shards never share isolate state and talk through rings; `net/http` is one multi-threaded runtime with a goroutine per connection. Equal cores, not equal architecture.
- **Short, single-machine, loopback runs.** Two 4-second runs per configuration (the first dataset used three 5-second runs). Across 54 configurations the run-to-run spread had a median of 0.8% and a worst case of 1.22x (plain connection setup); other scenarios stayed within 4%. The CPU governor was `powersave` and not controlled, so absolute numbers will differ on other machines.
- **The load generator is near, not at, its limit.** At 1.2M req/s `oha` used about 9 of its 16 threads, so the highest Gina rows may be slightly client-limited (a lower bound).
- **One workload.** Tiny responses, 256 connections, ECDSA certificates, no application work in the handler, no real network, small live heap. The shared-GC comparison in particular needs a handler that allocates and a larger heap before it says anything about GC pauses under thread mode.
