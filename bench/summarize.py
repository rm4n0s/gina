#!/usr/bin/env python3
"""Turn bench/results.csv into Markdown tables (median of the runs per configuration)."""
import csv, statistics, sys, collections

rows = list(csv.DictReader(open(sys.argv[1] if len(sys.argv) > 1 else "bench/results.csv")))
groups = collections.defaultdict(list)
for r in rows:
    groups[(r["scenario"], r["tls"], int(r["cores"]), r["server"])].append(r)

def med(rs, k):
    return statistics.median(float(r[k]) for r in rs)

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
        print(f"\n#### {titles[scenario]} - {'HTTPS (TLS 1.3)' if tls == 'tls' else 'HTTP'}\n")
        print("| cores | Gina req/s | net/http req/s | Gina / net/http | Gina p50 / p99 (ms) | net/http p50 / p99 (ms) | Gina RSS | net/http RSS | failed (G / N) |")
        print("|---:|---:|---:|---:|---|---|---:|---:|---|")
        for c in cores:
            g, n = groups.get((scenario, tls, c, "gina")), groups.get((scenario, tls, c, "nethttp"))
            if not g or not n:
                continue
            gr, nr = med(g, "rps"), med(n, "rps")
            fails = lambda rs: sum(int(r["errors"]) for r in rs)
            print(f"| {c} | {gr:,.0f} | {nr:,.0f} | {gr / nr:.2f}x | {med(g,'p50_ms'):.2f} / {med(g,'p99_ms'):.2f} | "
                  f"{med(n,'p50_ms'):.2f} / {med(n,'p99_ms'):.2f} | {med(g,'rss_mb'):.0f} MB | {med(n,'rss_mb'):.0f} MB | {fails(g)} / {fails(n)} |")
