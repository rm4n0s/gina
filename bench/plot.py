#!/usr/bin/env python3
"""Render bench/results.csv as bench/results.svg + results.png (and a dark variant).

    python3 bench/plot.py            # needs rsvg-convert for the PNGs

Colors follow the validated reference palette (categorical slots 1 and 2, light and
dark steps; both pairs pass validate_palette.js). Color follows the entity: Gina is
always blue, net/http always orange. A legend is always shown; thin marks, hairline
grid, 2px surface ring on markers, 2px surface gap between bars.
"""
import csv, os, statistics, subprocess, sys
from collections import defaultdict
from xml.sax.saxutils import escape

HERE = os.path.dirname(os.path.abspath(__file__))
FONT = "system-ui, 'Noto Sans', 'Liberation Sans', sans-serif"

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", grid="#e1e0d9", axis="#c3c2b7",
                  gina="#2a78d6", nethttp="#eb6834"),
    "dark": dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", grid="#2c2c2a", axis="#383835",
                 gina="#3987e5", nethttp="#d95926"),
}

# ---------------------------------------------------------------- data
rows = list(csv.DictReader(open(os.path.join(HERE, "results.csv"))))
g = defaultdict(list)
for r in rows:
    g[(r["scenario"], r["tls"], int(r["cores"]), r["server"])].append(r)


def med(scenario, tls, cores, server, key):
    return statistics.median(float(r[key]) for r in g[(scenario, tls, cores, server)])


CORES = [1, 2, 4, 8]


def series(scenario, tls, key, scale=1.0):
    return {s: [med(scenario, tls, c, s, key) * scale for c in CORES] for s in ("gina", "nethttp")}


def span(a, b, fmt="{:.1f}", invert=False):
    ratios = [(y / x if invert else x / y) for x, y in zip(a, b)]
    lo, hi = min(ratios), max(ratios)
    return (fmt + "–" + fmt + "×").format(lo, hi)


# ---------------------------------------------------------------- svg helpers
W, H = 1800, 1196
M = 56
GUT = 48
PW = (W - 2 * M - 2 * GUT) / 3
PH = 452
HEAD = 150


def fk(v, dec=0):
    return f"{v:,.{dec}f}k"


def text(x, y, s, size=14, fill="ink", weight=400, anchor="start", extra=""):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
            f'text-anchor="{anchor}" fill="{{{fill}}}" {extra}>{escape(s)}</text>')


def line_panel(x0, y0, title, sub, ser, ymax, ticks, fmt_tick, fmt_end):
    left, right = 54, 134
    px, py = x0 + left, y0 + 92
    pw, ph = PW - left - right, 292
    out = [text(x0, y0 + 22, title, 19, "ink", 600), text(x0, y0 + 46, sub, 14, "ink2")]
    ypix = lambda v: py + ph - v / ymax * ph
    for t in ticks:  # hairline solid grid; the zero line is the axis
        out.append(f'<line x1="{px:.1f}" x2="{px + pw:.1f}" y1="{ypix(t):.1f}" y2="{ypix(t):.1f}" '
                   f'stroke="{{{"axis" if t == 0 else "grid"}}}" stroke-width="1"/>')
        out.append(text(px - 10, ypix(t) + 4.5, fmt_tick(t), 13, "ink2", anchor="end"))
    xs = [px + 22 + i * (pw - 44) / 3 for i in range(4)]
    for x, c in zip(xs, CORES):
        out.append(text(x, py + ph + 24, str(c), 13, "ink2", anchor="middle"))
    out.append(text(px + pw / 2, py + ph + 48, "server cores", 13, "ink2", anchor="middle"))
    for name, vals in ser.items():
        col = "gina" if name == "gina" else "nethttp"
        pts = " ".join(f"{x:.1f},{ypix(v):.1f}" for x, v in zip(xs, vals))
        out.append(f'<polyline points="{pts}" fill="none" stroke="{{{col}}}" stroke-width="2" '
                   f'stroke-linejoin="round" stroke-linecap="round"/>')
        for x, v in zip(xs, vals):  # 2px surface ring keeps markers legible where they overlap the line
            out.append(f'<circle cx="{x:.1f}" cy="{ypix(v):.1f}" r="7" fill="{{surface}}"/>'
                       f'<circle cx="{x:.1f}" cy="{ypix(v):.1f}" r="5" fill="{{{col}}}"/>')
        label = "Gina" if name == "gina" else "net/http"
        out.append(f'<text x="{xs[-1] + 15:.1f}" y="{ypix(vals[-1]) + 5:.1f}" font-size="14" fill="{{ink2}}">'
                   f'{label} <tspan font-weight="600" fill="{{ink}}">{escape(fmt_end(vals[-1]))}</tspan></text>')
    return "\n".join(out)


def bar_panel(x0, y0, title, sub, cats, ser, ymax, ticks, fmt_tick, fmt_val, notes):
    left = 54
    px, py = x0 + left, y0 + 92
    pw, ph = PW - left - 20, 292
    out = [text(x0, y0 + 22, title, 19, "ink", 600), text(x0, y0 + 46, sub, 14, "ink2")]
    ypix = lambda v: py + ph - v / ymax * ph
    for t in ticks:
        out.append(f'<line x1="{px:.1f}" x2="{px + pw:.1f}" y1="{ypix(t):.1f}" y2="{ypix(t):.1f}" '
                   f'stroke="{{{"axis" if t == 0 else "grid"}}}" stroke-width="1"/>')
        out.append(text(px - 10, ypix(t) + 4.5, fmt_tick(t), 13, "ink2", anchor="end"))
    BW, GAP, R = 24, 2, 4  # <= 24px bars, 2px surface gap, 4px rounded data end, square at the baseline
    for ci, cat in enumerate(cats):
        cx = px + pw * (0.25 + 0.5 * ci)
        for si, name in enumerate(("gina", "nethttp")):
            v = ser[name][ci]
            bx = cx - BW - GAP / 2 + si * (BW + GAP)
            top, bot = ypix(v), ypix(0)
            col = "gina" if name == "gina" else "nethttp"
            out.append(f'<path d="M{bx:.1f},{bot:.1f} V{top + R:.1f} A{R},{R} 0 0 1 {bx + R:.1f},{top:.1f} '
                       f'H{bx + BW - R:.1f} A{R},{R} 0 0 1 {bx + BW:.1f},{top + R:.1f} V{bot:.1f} Z" fill="{{{col}}}"/>')
            out.append(text(bx + BW / 2, top - 8, fmt_val(v), 14, "ink", 600, "middle"))
        out.append(text(cx, py + ph + 24, cat, 14, "ink", 500, "middle"))
        out.append(text(cx, py + ph + 44, notes[ci], 13, "ink2", anchor="middle"))
    return "\n".join(out)


def legend(x_right, y):
    items = [("gina", "Gina (N worker processes, one thread each)"), ("nethttp", "net/http (GOMAXPROCS = N)")]
    out, x = [], x_right
    for key, label in reversed(items):
        w = 7.4 * len(label) + 26
        x -= w
        out.append(f'<circle cx="{x + 6:.1f}" cy="{y - 5:.1f}" r="6" fill="{{{key}}}"/>')
        out.append(text(x + 20, y, label, 14, "ink2"))
        x -= 18
    return "\n".join(out)


# ---------------------------------------------------------------- the figure
def build():
    ka_http, ka_tls = series("get", "plain", "rps", 1e-3), series("get", "tls", "rps", 1e-3)
    hs = series("newconn", "tls", "rps", 1e-3)
    p99 = series("get", "plain", "p99_ms")
    echo = {s: [med("echo64k", t, 4, s, "rps") * 1e-3 for t in ("plain", "tls")] for s in ("gina", "nethttp")}
    mem = {s: [med("get", t, 8, s, "rss_mb") for t in ("plain", "tls")] for s in ("gina", "nethttp")}
    echo_ratio = [a / b for a, b in zip(echo["gina"], echo["nethttp"])]
    mem_ratio = [a / b for a, b in zip(mem["gina"], mem["nethttp"])]

    panels = [
        line_panel(0, 0, f"Keep-alive GET, HTTP: {span(ka_http['gina'], ka_http['nethttp'])} throughput",
                   "Requests per second (thousands) · higher is better", ka_http, 1500, [0, 500, 1000, 1500],
                   lambda t: f"{t:,.0f}", lambda v: fk(v)),
        line_panel(0, 0, f"Keep-alive GET, HTTPS: {span(ka_tls['gina'], ka_tls['nethttp'])} throughput",
                   "Requests per second (thousands) · higher is better", ka_tls, 1250, [0, 250, 500, 750, 1000, 1250],
                   lambda t: f"{t:,.0f}", lambda v: fk(v)),
        line_panel(0, 0, f"TLS handshakes: {span(hs['gina'], hs['nethttp'])} more per second",
                   "New TLS 1.3 connection per request (thousands/s) · higher is better", hs, 60, [0, 20, 40, 60],
                   lambda t: f"{t:,.0f}", lambda v: fk(v, 1)),
        line_panel(0, 0, f"p99 latency, HTTP: {span(p99['gina'], p99['nethttp'], '{:.1f}', invert=True)} lower",
                   "Keep-alive GET, milliseconds · lower is better", p99, 6, [0, 2, 4, 6],
                   lambda t: f"{t:,.0f}", lambda v: f"{v:.2f} ms"),
        bar_panel(0, 0, f"64 KiB echo, 4 cores: {echo_ratio[0]:.1f}× HTTP, {echo_ratio[1]:.1f}× HTTPS",
                  "POST /echo, requests per second (thousands) · higher is better", ["HTTP", "HTTPS (TLS 1.3)"], echo,
                  200, [0, 50, 100, 150, 200], lambda t: f"{t:,.0f}", lambda v: fk(v),
                  [f"Gina {echo_ratio[0]:.1f}× faster", f"Gina {echo_ratio[1]:.1f}× faster"]),
        bar_panel(0, 0, f"Memory at 8 cores: Gina uses {min(mem_ratio):.0f}–{max(mem_ratio):.0f}× more",
                  "Resident memory, all server processes (MB) · lower is better", ["HTTP", "HTTPS (TLS 1.3)"], mem,
                  200, [0, 50, 100, 150, 200], lambda t: f"{t:,.0f}", lambda v: f"{v:,.0f}",
                  [f"Gina {mem_ratio[0]:.1f}× more", f"Gina {mem_ratio[1]:.1f}× more"]),
    ]
    body = []
    for i, p in enumerate(panels):
        col, row = i % 3, i // 3
        x = M + col * (PW + GUT)
        y = HEAD + row * (PH + 36)
        body.append(f'<g transform="translate({x:.1f},{y:.1f})">\n{p}\n</g>')

    head = [
        text(M, 62, "Gina vs Go net/http", 34, "ink", 700),
        text(M, 94, "Same cores, same routes, same bodies. Loopback HTTP/1.1, 256 connections, median of three 5 s runs per point.",
             15, "ink2"),
        text(M, 116, "Server pinned to N physical cores; the oha load generator runs on separate cores.", 15, "ink2"),
        legend(W - M, 62),
    ]
    foot_y = H - 56
    foot = [
        text(M, foot_y, "Not a feature-equal comparison: net/http is a complete, hardened server; Gina's HTTP is minimal and its TLS 1.3 is unaudited"
                         " (no resumption, no ChaCha20).", 13, "ink2"),
        text(M, foot_y + 20, "Gina scales with N shared-nothing processes, net/http with one multi-threaded process. One machine (AMD Ryzen AI MAX+ 395),"
                              " Go 1.26, oha 1.16, 2026-10-06. Tables and caveats: docs/BENCHMARKS.md.", 13, "ink2"),
    ]
    return "\n".join(head + body + foot)


def render(theme):
    t = THEMES[theme]
    inner = build().format(**t)
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
           f'font-family="{FONT}" role="img" aria-labelledby="t d">\n'
           f'<title id="t">Gina vs Go net/http benchmark results</title>\n'
           f'<desc id="d">Six small charts comparing Gina and net/http on 1 to 8 cores: keep-alive throughput over HTTP and HTTPS, '
           f'TLS handshakes per second, p99 latency, a 64 KiB echo test and memory use. Gina is faster on every measure except memory. '
           f'The same numbers are tabulated in docs/BENCHMARKS.md.</desc>\n'
           f'<rect width="{W}" height="{H}" fill="{t["surface"]}"/>\n{inner}\n</svg>\n')
    suffix = "" if theme == "light" else "-dark"
    svg_path = os.path.join(HERE, f"results{suffix}.svg")
    png_path = os.path.join(HERE, f"results{suffix}.png")
    open(svg_path, "w").write(svg)
    try:
        subprocess.run(["rsvg-convert", "-z", "2", "-o", png_path, svg_path], check=True)
    except FileNotFoundError:
        print("rsvg-convert not found: wrote the SVG only", file=sys.stderr)
    print("wrote", svg_path, "and", png_path)


if __name__ == "__main__":
    for theme in THEMES:
        render(theme)
