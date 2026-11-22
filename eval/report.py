#!/usr/bin/env python3
"""Builds a self-contained HTML report (no dependencies) from a results dir.

    python3 eval/report.py results/<timestamp>

Reads every <scenario>.json written by kvbench plus failover.csv and writes
report.html next to them.
"""
import csv
import glob
import html
import json
import os
import statistics
import sys

BLUE, ORANGE = "var(--s1)", "var(--s2)"


def load(dirpath):
    runs = []
    for p in sorted(glob.glob(os.path.join(dirpath, "*.json")), key=os.path.getmtime):
        try:
            r = json.load(open(p))
        except Exception:
            continue
        if "label" in r:
            runs.append(r)
    failover = []
    fp = os.path.join(dirpath, "failover.csv")
    if os.path.exists(fp):
        failover = list(csv.DictReader(open(fp)))
    return runs, failover


def esc(s):
    return html.escape(str(s))


def hbar_chart(rows, unit, fmt="{:,.0f}"):
    """rows: [(label, value)] -> horizontal bar chart, single series."""
    if not rows:
        return ""
    w, lw, bh, gap = 760, 210, 18, 8
    vmax = max(v for _, v in rows) or 1
    h = len(rows) * (bh + gap) + 10
    out = [f'<svg viewBox="0 0 {w} {h}" class="chart" role="img">']
    for i, (label, v) in enumerate(rows):
        y = 5 + i * (bh + gap)
        bw = max(2, (w - lw - 90) * v / vmax)
        out.append(f'<text x="{lw - 8}" y="{y + bh - 5}" class="lab" text-anchor="end">{esc(label)}</text>')
        out.append(f'<g class="hov"><rect x="{lw}" y="{y}" width="{bw:.1f}" height="{bh}" rx="4" fill="{BLUE}">'
                   f'<title>{esc(label)}: {fmt.format(v)} {unit}</title></rect></g>')
        out.append(f'<text x="{lw + bw + 6:.1f}" y="{y + bh - 5}" class="val">{fmt.format(v)}</text>')
    out.append("</svg>")
    return "".join(out)


def latency_chart(runs):
    """Grouped dot plot: p50 and p99 latency (ms) per scenario, log-free linear axis."""
    rows = []
    for r in runs:
        lat = r["read"] if r["read"]["count"] else r["write"]
        if lat["count"]:
            rows.append((r["label"], lat["p50_ms"], lat["p99_ms"]))
    if not rows:
        return ""
    w, lw, rh = 760, 210, 24
    vmax = max(p99 for _, _, p99 in rows) * 1.1 or 1
    h = len(rows) * rh + 40
    pw = w - lw - 30
    x = lambda v: lw + pw * v / vmax
    out = [f'<svg viewBox="0 0 {w} {h}" class="chart" role="img">']
    for t in range(0, 5):
        v = vmax * t / 4
        out.append(f'<line x1="{x(v):.1f}" x2="{x(v):.1f}" y1="0" y2="{h - 25}" class="grid"/>'
                   f'<text x="{x(v):.1f}" y="{h - 10}" class="axis" text-anchor="middle">{v:.1f} ms</text>')
    for i, (label, p50, p99) in enumerate(rows):
        y = 12 + i * rh
        out.append(f'<text x="{lw - 8}" y="{y + 4}" class="lab" text-anchor="end">{esc(label)}</text>')
        out.append(f'<line x1="{x(p50):.1f}" x2="{x(p99):.1f}" y1="{y}" y2="{y}" class="range"/>')
        for v, c, n in ((p50, BLUE, "p50"), (p99, ORANGE, "p99")):
            out.append(f'<g class="hov"><circle cx="{x(v):.1f}" cy="{y}" r="5" fill="{c}" class="ring">'
                       f'<title>{esc(label)} {n}: {v:.2f} ms</title></circle></g>')
    out.append("</svg>")
    legend = (f'<div class="legend"><span><i style="background:{BLUE}"></i>p50</span>'
              f'<span><i style="background:{ORANGE}"></i>p99</span></div>')
    return legend + "".join(out)


def timeline_chart(r):
    tl = r.get("timeline") or []
    if len(tl) < 3:
        return ""
    tl = tl[:-1]  # last bucket is partial
    w, h, pl, pb, pt = 360, 150, 46, 22, 8
    n = len(tl)
    vmax = max(max(s["ok"] for s in tl), 1) * 1.1
    x = lambda i: pl + (w - pl - 8) * i / max(n - 1, 1)
    y = lambda v: pt + (h - pt - pb) * (1 - v / vmax)
    out = [f'<svg viewBox="0 0 {w} {h}" class="chart" role="img">']
    # Fault window: scenarios inject at D/3 and heal at 2D/3.
    if any(k in r["label"] for k in ("failover", "partition")):
        a, b = x(n / 3), x(2 * n / 3)
        out.append(f'<rect x="{a:.1f}" y="{pt}" width="{b - a:.1f}" height="{h - pt - pb}" class="band"><title>fault injected</title></rect>')
    for t in (0, 0.5, 1):
        v = vmax / 1.1 * t
        out.append(f'<line x1="{pl}" x2="{w - 8}" y1="{y(v):.1f}" y2="{y(v):.1f}" class="grid"/>'
                   f'<text x="{pl - 4}" y="{y(v) + 4:.1f}" class="axis" text-anchor="end">{v / 1000:.0f}k</text>')
    pts = " ".join(f"{x(i):.1f},{y(s['ok']):.1f}" for i, s in enumerate(tl))
    out.append(f'<polyline points="{pts}" fill="none" stroke="{BLUE}" stroke-width="2"/>')
    for i, s in enumerate(tl):
        out.append(f'<rect x="{x(i) - 4:.1f}" y="{pt}" width="8" height="{h - pt - pb}" class="hit">'
                   f'<title>t={s["t"]}s: {s["ok"]:,} ok, {s["errors"]} errors</title></rect>')
        if s["errors"]:
            out.append(f'<circle cx="{x(i):.1f}" cy="{h - pb - 3}" r="3" fill="var(--crit)"><title>{s["errors"]} errors at t={s["t"]}s</title></circle>')
    out.append(f'<text x="{pl}" y="{h - 4}" class="axis">0s</text><text x="{w - 8}" y="{h - 4}" class="axis" text-anchor="end">{n}s</text>')
    out.append("</svg>")
    return (f'<figure><figcaption><b>{esc(r["label"])}</b> — successful ops/s; max gap '
            f'{r["max_unavailability_ms"]:.0f} ms</figcaption>{"".join(out)}</figure>')


def badge(c):
    cls = {"OK": "good", "VIOLATION": "crit", "SKIPPED": "muted", "UNKNOWN": "warn"}.get(c, "muted")
    icon = {"good": "✓", "crit": "✕", "warn": "?", "muted": "–"}[cls]
    return f'<span class="badge {cls}">{icon} {esc(c)}</span>'


def main(dirpath):
    runs, failover = load(dirpath)
    kv = [r for r in runs if r["workload"] == "kv"]
    corr = [r for r in runs if r["workload"] in ("linear", "bank")]
    parts = []

    parts.append("<h2>Throughput by scenario</h2><p class='note'>Successful operations per second (16 workers unless noted).</p>")
    parts.append(hbar_chart([(r["label"], r["throughput_ops_per_sec"]) for r in kv], "ops/s"))

    local = [r for r in kv if not r["label"].startswith("delay-")]
    delayed = [r for r in kv if r["label"].startswith("delay-")]
    parts.append("<h2>Latency by scenario</h2><p class='note'>Per-operation latency including client retries (Docker bridge network, no added delay).</p>")
    parts.append(latency_chart(local))
    if delayed:
        parts.append("<h2>Latency under network profiles</h2><p class='note'>tc netem: lan = 2±1 ms, wan = 5/20/40 ms per node one-way, lossy = 10±5 ms with 2% loss. Each write needs a quorum round-trip, so latency tracks the second-fastest replica.</p>")
        parts.append(latency_chart(delayed))

    timelines = [timeline_chart(r) for r in runs if any(k in r["label"] for k in ("failover", "partition", "chaos"))]
    if timelines:
        parts.append("<h2>Availability during faults</h2><p class='note'>Shaded band = fault active. Red dots = failed operations.</p>")
        parts.append("<div class='grid2'>" + "".join(timelines) + "</div>")

    if failover:
        parts.append("<h2>Leader failover time (range 1)</h2><table><tr><th>Fault</th><th>Trials</th>"
                     "<th>Fault → new leader (ms)</th><th>Fault → first write (ms)</th></tr>")
        for f in ("crash", "isolate"):
            r = [x for x in failover if x["fault"] == f]
            if not r:
                continue
            a = [int(x["fault_to_leader_ms"]) for x in r]
            b = [int(x["fault_to_first_write_ms"]) for x in r]
            parts.append(f"<tr><td>leader {f}</td><td>{len(r)}</td>"
                         f"<td>mean {statistics.mean(a):.0f} (min {min(a)}, max {max(a)})</td>"
                         f"<td>mean {statistics.mean(b):.0f} (min {min(b)}, max {max(b)})</td></tr>")
        parts.append("</table>")

    if corr:
        parts.append("<h2>Correctness under failure</h2><table><tr><th>Scenario</th><th>Check</th><th>Result</th><th>Details</th></tr>")
        for r in corr:
            check = "linearizability (porcupine)" if r["workload"] == "linear" else "bank invariant (txn atomicity)"
            parts.append(f"<tr><td>{esc(r['label'])}</td><td>{check}</td><td>{badge(r['correctness'])}</td><td>{esc(r.get('details', ''))}</td></tr>")
        parts.append("</table>")

    parts.append("<h2>All results</h2><table><tr><th>Scenario</th><th>Workload</th><th>Workers</th><th>Ops/s</th><th>OK</th>"
                 "<th>Errors</th><th>Read p50/p99 ms</th><th>Write p50/p99 ms</th><th>Max unavailability ms</th></tr>")
    for r in runs:
        parts.append(f"<tr><td>{esc(r['label'])}</td><td>{r['workload']}</td><td>{r['concurrency']}</td>"
                     f"<td>{r['throughput_ops_per_sec']:,.0f}</td><td>{r['ok']:,}</td><td>{r['errors']:,}</td>"
                     f"<td>{r['read']['p50_ms']:.2f} / {r['read']['p99_ms']:.2f}</td>"
                     f"<td>{r['write']['p50_ms']:.2f} / {r['write']['p99_ms']:.2f}</td>"
                     f"<td>{r['max_unavailability_ms']:.0f}</td></tr>")
    parts.append("</table>")

    page = f"""<!doctype html><html><head><meta charset="utf-8">
<title>KV store evaluation — {esc(os.path.basename(dirpath.rstrip('/')))}</title>
<style>
:root{{--bg:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--grid:#e4e3df;--s1:#2a78d6;--s2:#eb6834;--crit:#d03b3b;--good:#0a7d32;--warn:#b26b00;--band:rgba(235,104,52,.10)}}
@media (prefers-color-scheme:dark){{:root{{--bg:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--grid:#34332f;--s1:#3987e5;--s2:#d95926;--crit:#e66767;--good:#3fbf6a;--warn:#e0a030;--band:rgba(217,89,38,.16)}}}}
body{{background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,sans-serif;max-width:900px;margin:32px auto;padding:0 16px}}
h1{{font-size:22px}} h2{{font-size:17px;margin-top:36px}} .note{{color:var(--ink2);margin-top:-6px}}
.chart{{width:100%;height:auto}} .lab,.val,.axis{{fill:var(--ink2);font-size:12px}} .val{{fill:var(--ink)}}
.grid{{stroke:var(--grid);stroke-width:1}} .range{{stroke:var(--grid);stroke-width:3}} .ring{{stroke:var(--bg);stroke-width:2}}
.band{{fill:var(--band)}} .hit{{fill:transparent}} .hit:hover{{fill:var(--grid);opacity:.5}} .hov:hover{{opacity:.8}}
.legend{{display:flex;gap:16px;color:var(--ink2);font-size:12px}} .legend i{{display:inline-block;width:10px;height:10px;border-radius:5px;margin-right:6px}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:16px}} figure{{margin:0}} figcaption{{font-size:12px;color:var(--ink2)}}
table{{border-collapse:collapse;width:100%;font-size:13px}} th,td{{text-align:left;padding:6px 8px;border-bottom:1px solid var(--grid)}}
.badge{{font-weight:600}} .badge.good{{color:var(--good)}} .badge.crit{{color:var(--crit)}} .badge.warn{{color:var(--warn)}} .badge.muted{{color:var(--ink2)}}
</style></head><body>
<h1>Sharded Raft KV store — evaluation report</h1>
<p class="note">Run <code>{esc(os.path.basename(dirpath.rstrip('/')))}</code>, generated by eval/report.py</p>
{''.join(parts)}
</body></html>"""
    out = os.path.join(dirpath, "report.html")
    open(out, "w").write(page)
    print(out)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else sorted(glob.glob("results/2*"))[-1])
