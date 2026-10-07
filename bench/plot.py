#!/usr/bin/env python3
"""Render bench/results.csv as bench/results.svg + results.png (and a dark variant).

    python3 bench/plot.py            # HTTP/1.1; needs rsvg-convert for the PNGs
    python3 bench/plot.py h2         # HTTP/2: bench/results-h2.csv -> results-h2{,-dark}.{svg,png}

Colors follow the validated reference palette (categorical slots, light and dark steps).
Color follows the entity: Gina is always blue, net/http orange; shapes (circle / diamond)
repeat the identity so it never rests on color alone. Thin marks, hairline solid grid, a 2px surface
ring on markers.
"""
import csv, os, statistics, subprocess, sys
from collections import defaultdict
from xml.sax.saxutils import escape

HERE = os.path.dirname(os.path.abspath(__file__))
FONT = "system-ui, 'Noto Sans', 'Liberation Sans', sans-serif"

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", grid="#e1e0d9", axis="#c3c2b7",
                  mt="#2a78d6", nethttp="#eb6834"),
    "dark": dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", grid="#2c2c2a", axis="#383835",
                 mt="#3987e5", nethttp="#d95926"),
}
# csv server key -> (theme color key, legend label, marker shape)
SERVERS = {
    "gina": ("mt", "Gina, shard threads (one process)", "circle"),
    "nethttp": ("nethttp", "net/http (GOMAXPROCS = N)", "diamond"),
}
ORDER = ["nethttp", "gina"]  # draw order: the headline series ends up on top

H2 = len(sys.argv) > 1 and sys.argv[1] == "h2"
rows = list(csv.DictReader(open(os.path.join(HERE, "results-h2.csv" if H2 else "results.csv"))))
g = defaultdict(list)
for r in rows:
    g[(r["scenario"], r["tls"], int(r["cores"]), r["server"])].append(r)


def med(scenario, tls, cores, server, key):
    return statistics.median(float(r[key]) for r in g[(scenario, tls, cores, server)])


CORES = [1, 2, 4, 8]


def series(scenario, tls, key, scale=1.0):
    return {s: [med(scenario, tls, c, s, key) * scale for c in CORES] for s in SERVERS}


def span(a, b, invert=False):
    ratios = [(y / x if invert else x / y) for x, y in zip(a, b)]
    return f"{min(ratios):.1f}–{max(ratios):.1f}×"


W, H = 1800, 1214
M, GUT = 56, 48
PW = (W - 2 * M - 2 * GUT) / 3
PH, HEAD = 452, 168


def fk(v, dec=0):
    return f"{v:,.{dec}f}k"


def text(x, y, s, size=14, fill="ink", weight=400, anchor="start"):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
            f'text-anchor="{anchor}" fill="{{{fill}}}">{escape(s)}</text>')


def marker(shape, cx, cy, col, r=5, ring=True):
    """A filled marker with a 2px surface ring so it stays legible where marks overlap."""
    def one(rr, fill):
        if shape == "circle":
            return f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{rr}" fill="{{{fill}}}"/>'
        if shape == "square":
            return f'<rect x="{cx - rr:.1f}" y="{cy - rr:.1f}" width="{2 * rr}" height="{2 * rr}" fill="{{{fill}}}"/>'
        d = rr * 1.42  # diamond
        return (f'<polygon points="{cx:.1f},{cy - d:.1f} {cx + d:.1f},{cy:.1f} {cx:.1f},{cy + d:.1f} {cx - d:.1f},{cy:.1f}" '
                f'fill="{{{fill}}}"/>')
    return (one(r + 2, "surface") if ring else "") + one(r, col)


def grid(out, px, pw, ypix, ticks, fmt_tick):
    for t in ticks:  # hairline solid; the zero line is the axis
        out.append(f'<line x1="{px:.1f}" x2="{px + pw:.1f}" y1="{ypix(t):.1f}" y2="{ypix(t):.1f}" '
                   f'stroke="{{{"axis" if t == 0 else "grid"}}}" stroke-width="1"/>')
        out.append(text(px - 10, ypix(t) + 4.5, fmt_tick(t), 13, "ink2", anchor="end"))


def line_panel(x0, title, sub, ser, ymax, ticks, fmt_tick, fmt_end):
    left, right = 54, 140
    px, py = x0 + left, 92
    pw, ph = PW - left - right, 292
    out = [text(x0, 22, title, 19, "ink", 600), text(x0, 46, sub, 14, "ink2")]
    ypix = lambda v: py + ph - v / ymax * ph
    grid(out, px, pw, ypix, ticks, fmt_tick)
    xs = [px + 22 + i * (pw - 44) / 3 for i in range(4)]
    for x, c in zip(xs, CORES):
        out.append(text(x, py + ph + 24, str(c), 13, "ink2", anchor="middle"))
    out.append(text(px + pw / 2, py + ph + 48, "server cores", 13, "ink2", anchor="middle"))
    for name in ORDER:
        col, label, shape = SERVERS[name]
        vals = ser[name]
        pts = " ".join(f"{x:.1f},{ypix(v):.1f}" for x, v in zip(xs, vals))
        out.append(f'<polyline points="{pts}" fill="none" stroke="{{{col}}}" stroke-width="2" '
                   f'stroke-linejoin="round" stroke-linecap="round"/>')
        for x, v in zip(xs, vals):
            out.append(marker(shape, x, ypix(v), col))
        short = "Gina" if name == "gina" else "net/http"
        out.append(f'<text x="{xs[-1] + 15:.1f}" y="{ypix(vals[-1]) + 5:.1f}" font-size="14" fill="{{ink2}}">'
                   f'{short} <tspan font-weight="600" fill="{{ink}}">{escape(fmt_end(vals[-1]))}</tspan></text>')
    return "\n".join(out)


def bar_panel(x0, title, sub, cats, ser, ymax, ticks, fmt_tick, fmt_val, notes):
    left = 54
    px, py = x0 + left, 92
    pw, ph = PW - left - 20, 292
    out = [text(x0, 22, title, 19, "ink", 600), text(x0, 46, sub, 14, "ink2")]
    ypix = lambda v: py + ph - v / ymax * ph
    grid(out, px, pw, ypix, ticks, fmt_tick)
    BW, STEP, R = 22, 40, 4  # thin bars (<= 24px) with air between them so the value labels never collide; 4px rounded data end
    keys = ["gina", "nethttp"]
    for ci, cat in enumerate(cats):
        cx = px + pw * (0.25 + 0.5 * ci)
        for si, name in enumerate(keys):
            v = ser[name][ci]
            col = SERVERS[name][0]
            bx = cx - STEP / 2 + si * STEP - BW / 2
            top, bot = ypix(v), ypix(0)
            out.append(f'<path d="M{bx:.1f},{bot:.1f} V{top + R:.1f} A{R},{R} 0 0 1 {bx + R:.1f},{top:.1f} '
                       f'H{bx + BW - R:.1f} A{R},{R} 0 0 1 {bx + BW:.1f},{top + R:.1f} V{bot:.1f} Z" fill="{{{col}}}"/>')
            out.append(text(bx + BW / 2, top - 8, fmt_val(v), 13, "ink", 600, "middle"))
        out.append(text(cx, py + ph + 24, cat, 14, "ink", 500, "middle"))
        out.append(text(cx, py + ph + 44, notes[ci], 12, "ink2", anchor="middle"))
    return "\n".join(out)


def legend(x_right, y):
    out, x = [], x_right
    for name in reversed(list(SERVERS)):
        col, label, shape = SERVERS[name]
        w = 7.3 * len(label) + 30
        x -= w
        out.append(marker(shape, x + 7, y - 5, col, r=5, ring=False))
        out.append(text(x + 22, y, label, 14, "ink2"))
        x -= 14
    return "\n".join(out)


def build_h2():
    ka_http, ka_tls = series("get", "plain", "rps", 1e-3), series("get", "tls", "rps", 1e-3)
    p99_http, p99_tls = series("get", "plain", "p99_ms"), series("get", "tls", "p99_ms")
    echo = {s: [med("echo64k", t, 4, s, "rps") * 1e-3 for t in ("plain", "tls")] for s in SERVERS}
    mem = {s: [med("get", t, 8, s, "rss_mb") for t in ("plain", "tls")] for s in SERVERS}
    er = [a / b for a, b in zip(echo["gina"], echo["nethttp"])]
    mt = [a / b for a, b in zip(mem["gina"], mem["nethttp"])]
    thr = lambda t: f"{t:,.0f}"

    panels = [
        line_panel(0, f"Keep-alive GET, h2c: {span(ka_http['gina'], ka_http['nethttp'])} throughput",
                   "Requests per second (thousands) · higher is better", ka_http, 2000, [0, 500, 1000, 1500, 2000],
                   thr, lambda v: fk(v)),
        line_panel(0, f"Keep-alive GET, h2 over TLS: {span(ka_tls['gina'], ka_tls['nethttp'])} throughput",
                   "Requests per second (thousands) · higher is better", ka_tls, 2000, [0, 500, 1000, 1500, 2000],
                   thr, lambda v: fk(v)),
        bar_panel(0, f"64 KiB echo, 4 cores: {er[0]:.1f}× h2c, {er[1]:.1f}× TLS",
                  "POST /echo, requests per second (thousands) · higher is better", ["h2c", "h2 over TLS 1.3"], echo,
                  200, [0, 50, 100, 150, 200], thr, lambda v: fk(v),
                  [f"Gina {er[0]:.1f}× net/http", f"Gina {er[1]:.1f}× net/http"]),
        line_panel(0, f"p99 latency, h2c: {span(p99_http['gina'], p99_http['nethttp'], invert=True)} lower",
                   "Keep-alive GET, milliseconds · lower is better", p99_http, 8, [0, 2, 4, 6, 8],
                   thr, lambda v: f"{v:.2f} ms"),
        line_panel(0, f"p99 latency, TLS: {span(p99_tls['gina'], p99_tls['nethttp'], invert=True)} lower",
                   "Keep-alive GET, milliseconds · lower is better", p99_tls, 8, [0, 2, 4, 6, 8],
                   thr, lambda v: f"{v:.2f} ms"),
        bar_panel(0, f"Memory at 8 cores: {min(mt):.1f}–{max(mt):.1f}× net/http",
                  "Resident memory of the server process (MB) · lower is better", ["h2c", "h2 over TLS 1.3"], mem,
                  80, [0, 20, 40, 60, 80], thr, lambda v: f"{v:,.0f}",
                  [f"Gina {mt[0]:.1f}× net/http", f"Gina {mt[1]:.1f}× net/http"]),
    ]
    body = []
    for i, p in enumerate(panels):
        col, row = i % 3, i // 3
        x, y = M + col * (PW + GUT), HEAD + row * (PH + 36)
        body.append(f'<g transform="translate({x:.1f},{y:.1f})">\n{p}\n</g>')

    head = [
        text(M, 62, "Gina vs Go net/http, HTTP/2", 34, "ink", 700),
        text(M, 94, "HTTP/2 only on both sides. Same cores, same routes, same bodies. Loopback, 64 connections x 4 streams, median of three 5 s runs per point.", 15, "ink2"),
        text(M, 116, "Server pinned to N physical cores; the oha load generator runs on separate cores. Gina: one process, N pinned shard threads (Tina's model).", 15, "ink2"),
        legend(W - M, 62),
    ]
    fy = H - 76
    foot = [
        text(M, fy, f"Gina's 4- and 8-core points are probably limited by the load generator (4 to 8 cores adds only {100 * (med('get', 'plain', 8, 'gina', 'rps') / med('get', 'plain', 4, 'gina', 'rps') - 1):.0f}%), so those ratios are lower bounds.", 13, "ink2"),
        text(M, fy + 20, "Not a feature-equal comparison: net/http is a complete, hardened server; Gina's HTTP/2 has no push, priorities or trailers, and its TLS 1.3 is unaudited.", 13, "ink2"),
        text(M, fy + 40, "Shard threads share one Go heap and GC. One machine (AMD Ryzen AI MAX+ 395), Go 1.26, oha 1.16, 2026-10-07. Tables and caveats: docs/BENCHMARKS.md.", 13, "ink2"),
    ]
    return "\n".join(head + body + foot)


def build():
    if H2:
        return build_h2()
    ka_http, ka_tls = series("get", "plain", "rps", 1e-3), series("get", "tls", "rps", 1e-3)
    hs = series("newconn", "tls", "rps", 1e-3)
    p99 = series("get", "plain", "p99_ms")
    echo = {s: [med("echo64k", t, 4, s, "rps") * 1e-3 for t in ("plain", "tls")] for s in SERVERS}
    mem = {s: [med("get", t, 8, s, "rss_mb") for t in ("plain", "tls")] for s in SERVERS}
    er = [a / b for a, b in zip(echo["gina"], echo["nethttp"])]
    mt = [a / b for a, b in zip(mem["gina"], mem["nethttp"])]

    panels = [
        line_panel(0, f"Keep-alive GET, HTTP: {span(ka_http['gina'], ka_http['nethttp'])} throughput",
                   "Requests per second (thousands) · higher is better", ka_http, 1500, [0, 500, 1000, 1500],
                   lambda t: f"{t:,.0f}", lambda v: fk(v)),
        line_panel(0, f"Keep-alive GET, HTTPS: {span(ka_tls['gina'], ka_tls['nethttp'])} throughput",
                   "Requests per second (thousands) · higher is better", ka_tls, 1250, [0, 250, 500, 750, 1000, 1250],
                   lambda t: f"{t:,.0f}", lambda v: fk(v)),
        line_panel(0, f"TLS handshakes: {span(hs['gina'], hs['nethttp'])} more per second",
                   "New TLS 1.3 connection per request (thousands/s) · higher is better", hs, 60, [0, 20, 40, 60],
                   lambda t: f"{t:,.0f}", lambda v: fk(v, 1)),
        line_panel(0, f"p99 latency, HTTP: {span(p99['gina'], p99['nethttp'], invert=True)} lower",
                   "Keep-alive GET, milliseconds · lower is better", p99, 6, [0, 2, 4, 6],
                   lambda t: f"{t:,.0f}", lambda v: f"{v:.2f} ms"),
        bar_panel(0, f"64 KiB echo, 4 cores: {er[0]:.1f}× HTTP, {er[1]:.1f}× HTTPS",
                  "POST /echo, requests per second (thousands) · higher is better", ["HTTP", "HTTPS (TLS 1.3)"], echo,
                  250, [0, 50, 100, 150, 200, 250], lambda t: f"{t:,.0f}", lambda v: fk(v),
                  [f"Gina {er[0]:.1f}× net/http", f"Gina {er[1]:.1f}× net/http"]),
        bar_panel(0, f"Memory at 8 cores: {min(mt):.1f}–{max(mt):.1f}× net/http",
                  "Resident memory of the server process (MB) · lower is better", ["HTTP", "HTTPS (TLS 1.3)"], mem,
                  200, [0, 50, 100, 150, 200], lambda t: f"{t:,.0f}", lambda v: f"{v:,.0f}",
                  [f"Gina {mt[0]:.1f}× net/http", f"Gina {mt[1]:.1f}× net/http"]),
    ]
    body = []
    for i, p in enumerate(panels):
        col, row = i % 3, i // 3
        x, y = M + col * (PW + GUT), HEAD + row * (PH + 36)
        body.append(f'<g transform="translate({x:.1f},{y:.1f})">\n{p}\n</g>')

    head = [
        text(M, 62, "Gina vs Go net/http", 34, "ink", 700),
        text(M, 94, "Same cores, same routes, same bodies. Loopback HTTP/1.1, 256 connections, median of three 5 s runs per point.", 15, "ink2"),
        text(M, 116, "Server pinned to N physical cores; the oha load generator runs on separate cores. Gina: one process, N pinned shard threads (Tina's model).", 15, "ink2"),
        legend(W - M, 62),
    ]
    fy = H - 56
    foot = [
        text(M, fy, "Not a feature-equal comparison: net/http is a complete, hardened server; Gina's HTTP is minimal and its TLS 1.3 is unaudited"
                    " (no resumption, no ChaCha20).", 13, "ink2"),
        text(M, fy + 20, "Shard threads share one Go heap and GC. One machine (AMD Ryzen AI MAX+ 395),"
                         " Go 1.26, oha 1.16, 2026-10-07. Tables and caveats: docs/BENCHMARKS.md.", 13, "ink2"),
    ]
    return "\n".join(head + body + foot)


def render(theme):
    t = THEMES[theme]
    inner = build().format(**t)
    desc = (("Six small charts comparing Gina (shard threads in one process) with Go net/http serving HTTP/2 on 1 to 8 cores: "
             "keep-alive throughput over h2c and h2 over TLS, a 64 KiB echo test, p99 latency over both, and memory use. "
             "Gina is faster on every throughput and latency measure; its 4- and 8-core points are probably limited by the load generator. "
             "Gina uses more memory than net/http at 8 cores. The same numbers are tabulated in docs/BENCHMARKS.md.") if H2 else
            ("Six small charts comparing Gina (shard threads in one process) with Go net/http on 1 to 8 cores: "
             "keep-alive throughput over HTTP and HTTPS, TLS handshakes per second, p99 latency, a 64 KiB echo test and memory use. "
             "Gina is faster on every throughput and latency measure; Gina uses about the same memory as net/http on HTTP and about three times as much on HTTPS. The same numbers are tabulated in docs/BENCHMARKS.md."))
    title = "Gina vs Go net/http HTTP/2 benchmark results" if H2 else "Gina vs Go net/http benchmark results"
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
           f'font-family="{FONT}" role="img" aria-labelledby="t d">\n'
           f'<title id="t">{title}</title>\n'
           f'<desc id="d">{desc}</desc>\n'
           f'<rect width="{W}" height="{H}" fill="{t["surface"]}"/>\n{inner}\n</svg>\n')
    suffix = ("-h2" if H2 else "") + ("" if theme == "light" else "-dark")
    svg_path, png_path = (os.path.join(HERE, f"results{suffix}.{e}") for e in ("svg", "png"))
    open(svg_path, "w").write(svg)
    try:
        subprocess.run(["rsvg-convert", "-z", "2", "-o", png_path, svg_path], check=True)
    except FileNotFoundError:
        print("rsvg-convert not found: wrote the SVG only", file=sys.stderr)
    print("wrote", svg_path, "and", png_path)


if __name__ == "__main__":
    for theme in THEMES:
        render(theme)
