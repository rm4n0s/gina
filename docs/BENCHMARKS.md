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
| All servers get | the same N cores. 256 concurrent connections (64 for the echo test), 1 s warm-up, then 3 runs of 5 s; the **median** is reported |
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
| 1 | 169,445 | 110,331 | 1.54x | 1.67 / 4.60 | 11 / 17 |
| 2 | 361,223 | 189,638 | 1.90x | 0.87 / 4.89 | 12 / 18 |
| 4 | 701,630 | 398,755 | 1.76x | 0.47 / 2.40 | 15 / 18 |
| 8 | 1,215,246 | 662,155 | 1.84x | 0.36 / 1.89 | 21 / 20 |

#### Keep-alive GET (14-byte response) - HTTPS (TLS 1.3)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 152,394 | 99,410 | 1.53x | 1.90 / 4.99 | 30 / 22 |
| 2 | 335,369 | 184,346 | 1.82x | 0.90 / 5.26 | 42 / 23 |
| 4 | 618,854 | 374,758 | 1.65x | 0.55 / 2.40 | 63 / 24 |
| 8 | 1,021,465 | 611,415 | 1.67x | 0.44 / 1.92 | 103 / 25 |

#### New connection per request (connection setup / TLS handshake bound) - HTTP

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 53,153 | 49,222 | 1.08x | 11.83 / 8.97 | 17 / 13 |
| 2 | 85,155 | 82,680 | 1.03x | 8.06 / 7.66 | 28 / 15 |
| 4 | 198,477 | 143,797 | 1.38x | 4.24 / 4.84 | 43 / 15 |
| 8 | 286,359 | 150,046 | 1.91x | 1.62 / 3.09 | 71 / 17 |

#### New connection per request (connection setup / TLS handshake bound) - HTTPS (TLS 1.3)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 8,811 | 5,431 | 1.62x | 31.63 / 117.55 | 32 / 20 |
| 2 | 16,661 | 9,349 | 1.78x | 32.86 / 65.62 | 41 / 16 |
| 4 | 32,074 | 14,353 | 2.23x | 24.93 / 56.91 | 62 / 16 |
| 8 | 51,874 | 17,253 | 3.01x | 20.83 / 44.16 | 112 / 17 |

#### POST /echo with a 64 KiB body, keep-alive - HTTP

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 190,852 | 50,589 | 3.77x | 0.46 / 5.86 | 31 / 14 |

#### POST /echo with a 64 KiB body, keep-alive - HTTPS (TLS 1.3)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 76,551 | 37,534 | 2.04x | 1.09 / 8.73 | 80 / 15 |

Failed requests across all 108 measured runs: 0

Failed requests across all 108 measured runs: 0

Memory is the resident memory of the server process at the end of each run, and it is **noisy**: with TLS at 8 cores the same configuration ends at either about 50 or about 100 MB depending on where the garbage collector is (both appear in the raw data), so the median of three can land on either.

## What the numbers say

- **Memory:** versus net/http, Gina is about equal on plain HTTP (21 vs 20 MB at 8 cores) and 2-4x on HTTPS (51-103 MB, see the note above, vs 25 MB; TLS connections hold a 16 KiB read buffer each plus handshake garbage). All shard threads share one Go heap, so memory grows slowly with cores (11 MB on 1 core, 21 MB on 8).
- **Versus net/http, keep-alive throughput is 1.5-1.9x** at every core count, over HTTP and HTTPS, and scales almost linearly (169k to 1.22M req/s from 1 to 8 cores). **p99 is 2.6-5.8x lower** (0.36 ms vs 1.89 ms at 8 cores).
- **TLS handshakes:** 1.6-3.0x as many as net/http, about 8.8k/s per core and 52k/s on 8 cores. This is mostly ECDSA signing plus key exchange (81 µs measured above); net/http stops scaling near 17k/s here.
- **Large bodies:** a 64 KiB echo is **3.8x** faster over HTTP and **2.0x** over TLS, with a far better p99 (0.46 ms vs 5.9 ms over HTTP).
- **Plain connection setup:** on par at 1-2 cores (1.03-1.08x) and ahead at 4-8 (1.4-1.9x), and **noisy** (up to 1.23x run-to-run). This workload is dominated by the kernel's accept/SYN path, and Gina's listener accepts one connection per scheduler tick per shard, a known limit.
- **The shared garbage collector did not show up.** In thread mode all shards share one heap and collector, so a collection pauses every shard, but p99 stayed low in these runs (0.36 ms at 8 cores). That is a weak test (tiny responses, little live data, no allocation-heavy handlers); see the caveats.
- **Re-measured after the WebSocket work** (which changed the engine: `WaitIOOrMessage`, large messages, a parallel slot in the rings, and the HTTP servers' connection handlers). Against the previous dataset, keep-alive throughput moved by at most a few percent in either direction, and by the same amount for `net/http` (it is the machine's noise: another VM and a browser were running). No row regressed beyond that noise.

## HTTP/2

Measured 2026-10-07 with `PROTO=h2 bench/run.sh` (raw data: [bench/results-h2.csv](../bench/results-h2.csv); tables from `python3 bench/summarize.py bench/results-h2.csv HTTP/2`). Same machine, core pinning, `oha`, run lengths (1 s warm-up, 3 runs of 5 s, median) and routes as above. **Gina** is `examples/http2 -shards N -pin`: one process, N shard threads, one `SO_REUSEPORT` listener each. The **baseline** is the same `bench/nethttp` server started with `-h2`: Go `net/http` serving HTTP/2 only (h2c with prior knowledge, or h2 over TLS 1.3 with ECDSA P-256 and tickets disabled), `GOMAXPROCS=N`. Both speak HTTP/2 only, so each connection is HTTP/2 from its first byte.

The load is **64 connections with 4 concurrent streams each** (`oha -p 4`), i.e. the same 256 requests in flight as the HTTP/1.1 runs but over a quarter as many sockets, and 16 connections x 4 streams for the 64 KiB echo. Do not compare these rows with the HTTP/1.1 tables above: the load shape is different.

![Gina vs net/http over HTTP/2: throughput, 64 KiB echo, p99 latency and memory](../bench/results-h2.png)

*The tables below are the table view of the same data.*

#### Keep-alive GET (14-byte response) - HTTP/2 (h2c)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 534,605 | 79,483 | 6.73x | 0.55 / 6.08 | 15 / 16 |
| 2 | 1,012,457 | 153,839 | 6.58x | 0.33 / 5.18 | 24 / 19 |
| 4 | 1,581,926 | 285,582 | 5.54x | 0.28 / 3.38 | 34 / 20 |
| 8 | 1,759,326 | 446,645 | 3.94x | 0.19 / 2.82 | 54 / 22 |


#### Keep-alive GET (14-byte response) - HTTP/2 over TLS 1.3

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 1 | 496,782 | 79,120 | 6.28x | 0.67 / 6.23 | 19 / 19 |
| 2 | 955,556 | 145,835 | 6.55x | 0.35 / 6.10 | 29 / 21 |
| 4 | 1,504,662 | 276,715 | 5.44x | 0.27 / 3.87 | 48 / 21 |
| 8 | 1,694,034 | 427,552 | 3.96x | 0.21 / 3.03 | 83 / 22 |


#### POST /echo with a 64 KiB body, keep-alive - HTTP/2 (h2c)

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 134,040 | 33,264 | 4.03x | 0.76 / 6.93 | 30 / 21 |


#### POST /echo with a 64 KiB body, keep-alive - HTTP/2 over TLS 1.3

| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |
|---:|---:|---:|---:|---|---|
| 4 | 77,313 | 26,319 | 2.94x | 1.18 / 8.59 | 60 / 24 |

Failed requests across all 60 measured runs: 0

Failed requests across all 60 measured runs: 0. Run-to-run spread over the 20 configurations: median 1.014x, worst 1.06x. The server's own request counter matched `oha`'s total to within the handful of probe requests in a cross-check.

**What the numbers say**

- **Keep-alive GET: 3.9-6.7x the throughput of `net/http` over h2c and 4.0-6.6x over TLS**, with a p99 11x lower at one core (0.55 ms vs 6.1 ms) and 15x lower at eight (0.19 ms vs 2.8 ms). One shard thread serves about 535k req/s.
- **Why so large a gap is not measured here.** HTTP/2 in `net/http` costs more per request than its HTTP/1.1 path (it dropped from 110k to 79k req/s per core), while Gina's h2 path gets faster per request than its HTTP/1.1 path, because each read and write carries several streams. The likely causes on Go's side (goroutine per stream, framer and writer scheduling, allocation) were not profiled.
- **64 KiB echo:** 4.0x over h2c, 2.9x over TLS, with the p99 about 9x (h2c) and 7x (TLS) lower.
- **Memory:** at or below `net/http` at one core and higher as cores grow: 15-54 MB (h2c) and 19-83 MB (TLS) for Gina against 16-22 MB and 19-22 MB. Every shard thread holds its own connection buffers, and TLS connections hold a 16 KiB ciphertext buffer each.

**HTTP/2 caveats** (the general ones below apply too)

- **Gina's 4- and 8-core rows are probably limited by the load generator.** Going from 4 to 8 cores adds only 11% (1.58M to 1.76M req/s) after near-linear scaling up to 4, so the 8-core ratios (3.9x) are lower bounds. The `net/http` rows are not affected.
- **New-connection-per-request is not measured.** `oha` ignores `--disable-keepalive` over HTTP/2 (verified: no `TIME_WAIT` sockets after 300k requests), so that scenario would just repeat the GET one. TLS handshake cost is the same code path as in the HTTP/1.1 tables.
- **Not a feature-equal comparison.** Gina's HTTP/2 has no server push, prioritisation, trailers or event streams, never indexes response headers, and has not had a security audit or an h2spec run. `net/http` is the more complete and hardened server, and some of the speed is the missing generality.
- **One workload:** a 14-byte response, no handler work, loopback, one machine, small headers.

## Caveats

- **Not a feature-equal comparison.** `net/http` is a hardened, complete server (chunked bodies, `Expect: 100-continue`, HTTP/2 elsewhere, years of edge-case fixes). Gina's HTTP is deliberately minimal: no chunked request bodies, HTTP/2 only in a separate server, a hand-written parser and a TLS 1.3 implementation that has had no security audit and lacks resumption and ChaCha20. Some of the speed is the missing generality.
- **Different concurrency models.** Gina's shards never share isolate state and talk through rings; `net/http` is one multi-threaded runtime with a goroutine per connection. Equal cores, not equal architecture.
- **Short, single-machine, loopback runs.** Three 5-second runs per configuration. Across the 36 HTTP/1.1 configurations the run-to-run spread had a median of 2% and a worst case of 1.23x (plain connection setup); other scenarios stayed within 9% (HTTP/2: median 1.4%, worst 6%). The CPU governor was `powersave` and not controlled, so absolute numbers will differ on other machines.
- **The load generator is near, not at, its limit.** At 1.2M req/s `oha` used about 9 of its 16 threads, so the highest Gina rows may be slightly client-limited (a lower bound).
- **One workload.** Tiny responses, 256 connections, ECDSA certificates, no application work in the handler, no real network, small live heap. The shared-GC comparison in particular needs a handler that allocates and a larger heap before it says anything about GC pauses under thread mode.
