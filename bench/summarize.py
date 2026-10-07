#!/usr/bin/env python3
"""Turn bench/results.csv into Markdown tables (median of the runs per configuration).

Servers: gina = Gina, N shard threads in ONE process; nethttp = Go net/http.
"""
import csv, statistics, sys, collections

# usage: summarize.py [results.csv [protocol label, e.g. HTTP/2]]
proto = sys.argv[2] if len(sys.argv) > 2 else None
rows = list(csv.DictReader(open(sys.argv[1] if len(sys.argv) > 1 else "bench/results.csv")))
groups = collections.defaultdict(list)
for r in rows:
    groups[(r["scenario"], r["tls"], int(r["cores"]), r["server"])].append(r)

med = lambda rs, k: statistics.median(float(r[k]) for r in rs)
titles = {
    "get": "Keep-alive GET (14-byte response)",
    "newconn": "New connection per request (connection setup / TLS handshake bound)",
    "echo64k": "POST /echo with a 64 KiB body, keep-alive",
}
for scenario in ("get", "newconn", "echo64k"):
    for tls in ("plain", "tls"):
        cores = sorted({k[2] for k in groups if k[0] == scenario and k[1] == tls})
        if not cores:
            continue
        print(f"\n#### {titles[scenario]} - {(proto + ' over TLS 1.3' if proto else 'HTTPS (TLS 1.3)') if tls == 'tls' else (proto + ' (h2c)' if proto else 'HTTP')}\n")
        print("| cores | Gina req/s | net/http req/s | Gina / net/http | p99 ms (Gina / net/http) | RSS MB (Gina / net/http) |")
        print("|---:|---:|---:|---:|---|---|")
        for c in cores:
            t, n = (groups.get((scenario, tls, c, s)) for s in ("gina", "nethttp"))
            if not (t and n):
                continue
            print(f"| {c} | {med(t,'rps'):,.0f} | {med(n,'rps'):,.0f} | {med(t,'rps')/med(n,'rps'):.2f}x | "
                  f"{med(t,'p99_ms'):.2f} / {med(n,'p99_ms'):.2f} | "
                  f"{med(t,'rss_mb'):.0f} / {med(n,'rss_mb'):.0f} |")
fails = sum(int(r["errors"]) for r in rows)
print(f"\nFailed requests across all {len(rows)} measured runs: {fails}")
