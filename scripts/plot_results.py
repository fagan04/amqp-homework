#!/usr/bin/env python3
"""Turn results/ into charts (docs/charts/*.png) and summary tables
(results/chaos_summary.csv, results/chaos_summary.md).

Usage: python3 scripts/plot_results.py
"""
import csv
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import NullLocator  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "docs", "charts")
os.makedirs(OUT, exist_ok=True)

# Validated categorical palette (fixed order) + neutral tokens
S1, S2, S3, S4 = "#2a78d6", "#eb6834", "#1baf7a", "#6250d6"
INK, INK2, MUTED, GRID, SURF = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1", "#fcfcfb"
plt.rcParams.update({
    "figure.facecolor": SURF, "axes.facecolor": SURF, "savefig.facecolor": SURF,
    "axes.edgecolor": MUTED, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.spines.top": False,
    "axes.spines.right": False, "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlecolor": INK, "legend.frameon": False, "lines.linewidth": 2,
})


def rcsv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def rjson(path):
    with open(path) as f:
        return json.load(f)


SCENARIOS = ["baseline", "node-crash", "leader-crash", "partition", "partition-leader", "quorum-loss", "rolling-restart"]
TITLES = {
    "baseline": "Baseline (no failures)",
    "node-crash": "Crash of consumer's node (rabbit2)",
    "leader-crash": "Crash of the quorum-queue leader",
    "partition": "Network partition of rabbit3 (follower)",
    "partition-leader": "Network partition of rabbit3 (leader)",
    "quorum-loss": "Loss of 2 of 3 nodes (quorum lost)",
    "rolling-restart": "Rolling restart of all nodes",
}


def analyse(name):
    d = os.path.join(RES, name)
    ev = rcsv(os.path.join(d, "events.csv"))
    t0 = int(next(e for e in ev if e["type"] == "load_start")["epoch_ms"])
    rate = 2000.0
    for e in ev:
        if e["type"] == "load_start":
            rate = float(e["detail"].split("rate=")[1].split()[0])
    pc = rcsv(os.path.join(d, "producer.csv"))
    cc = rcsv(os.path.join(d, "consumer.csv"))
    cl = rcsv(os.path.join(d, "cluster.csv"))
    p = rjson(os.path.join(d, "producer.json"))
    c = rjson(os.path.join(d, "consumer.json"))
    rel = lambda ms: (int(ms) - t0) / 1000.0  # noqa: E731
    out = {"scenario": name, "rate": rate, "producer": p, "consumer": c, "events": ev,
           "pt": [(rel(r["epoch_ms"]), float(r["confirmed_per_s"])) for r in pc],
           "ct": [(rel(r["epoch_ms"]), float(r["unique_per_s"])) for r in cc],
           "cl": [(rel(r["epoch_ms"]), r) for r in cl], "rel": rel}

    # --- derived recovery metrics -----------------------------------------
    fail = [e for e in ev if e["type"] in ("kill", "partition", "restart")]
    m = {}
    if fail:
        tf = rel(fail[0]["epoch_ms"])
        # leader before / after failure from clustermon samples
        before = [r["leader"] for t, r in out["cl"] if t < tf and r["leader"] not in ("", "unknown")]
        old = before[-1] if before else None
        m["leader_before"] = old
        new = next(((t, r["leader"]) for t, r in out["cl"]
                    if t >= tf and r["leader"] not in ("", "unknown", old)), None)
        if new and old and old in fail[0]["detail"]:
            m["leader_after"] = new[1]
            m["election_s"] = round(new[0] - tf, 2)  # 250 ms sampling resolution
        # seconds with publish confirms below 50% of the offered rate
        win = [v for t, v in out["pt"] if tf <= t <= tf + 45]
        m["publish_stall_s"] = sum(1 for v in win if v < 0.5 * rate)
        # first second after the failure in which publishing is back at >= 50%
        # of the offered rate following a stall (0 = never stalled)
        stalled, resume = False, 0.0
        for t, v in out["pt"]:
            if t < tf:
                continue
            if t > tf + 45:
                break
            if v < 0.5 * rate:
                stalled = True
            elif stalled:
                resume = round(t - tf, 1)
                break
        m["publish_resume_s"] = resume
        m["min_confirm_rate"] = min(win) if win else None
        winc = [v for t, v in out["ct"] if tf <= t <= tf + 45]
        m["consume_stall_s"] = sum(1 for v in winc if v < 0.5 * rate)
        # consumer reconnection time after failure (from consumer event log)
        cstart = int(rcsv(os.path.join(d, "consumer.csv"))[0]["epoch_ms"]) - \
            float(rcsv(os.path.join(d, "consumer.csv"))[0]["t_s"]) * 1000
        evs = c.get("events") or []
        fo = [e for e in evs if e["type"] == "failover"]
        if fo:
            tfo = (cstart + fo[0]["t_s"] * 1000 - t0) / 1000
            conn = next((e for e in evs if e["type"] == "connected" and e["t_s"] > fo[0]["t_s"]), None)
            if conn:
                m["consumer_failover_s"] = round((cstart + conn["t_s"] * 1000 - t0) / 1000 - tfo, 2)
                m["consumer_detect_s"] = round(tfo - tf, 2)
        # time until all 3 nodes / members back after the last start/heal
        back = [e for e in ev if e["type"] in ("start", "heal")]
        if back:
            tb = rel(back[-1]["epoch_ms"])
            full = next((t for t, r in out["cl"] if t >= tb and r["online_members"] == "3"
                         and r["node1_up"] == r["node2_up"] == r["node3_up"] == "1"), None)
            if full:
                m["rejoin_s"] = round(full - tb, 1)
    out["metrics"] = m
    return out


def shade_down(ax, a):
    """Grey bands where any node is unavailable; vertical lines at events."""
    cl = a["cl"]
    start = None
    for t, r in cl:
        down = "0" in (r["node1_up"], r["node2_up"], r["node3_up"])
        if down and start is None:
            start = t
        if not down and start is not None:
            ax.axvspan(start, t, color="#d9d8d3", alpha=0.6, lw=0)
            start = None
    if start is not None:
        ax.axvspan(start, cl[-1][0], color="#d9d8d3", alpha=0.6, lw=0)
    ymax = ax.get_ylim()[1]
    for e in a["events"]:
        if e["type"] in ("kill", "start", "partition", "heal", "restart"):
            t = a["rel"](e["epoch_ms"])
            ax.axvline(t, color=INK2, lw=1, ls="--")
            ax.text(t + 0.4, ymax * 0.97, f"{e['type']} {e['detail'].split(' ')[0]}", rotation=90,
                    va="top", ha="left", fontsize=8, color=INK2)


def timeline(a):
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot([t for t, _ in a["pt"]], [v for _, v in a["pt"]], color=S1, label="producer: confirmed msg/s")
    ax.plot([t for t, _ in a["ct"]], [v for _, v in a["ct"]], color=S2, label="consumer: unique msg/s")
    ax.set_xlim(0, max(t for t, _ in a["pt"]) + 2)
    ax.set_ylim(0, max(max(v for _, v in a["pt"]), max(v for _, v in a["ct"])) * 1.15)
    shade_down(ax, a)
    ax.set_xlabel("seconds since load start  (grey = at least one node down/isolated)")
    ax.set_ylabel("messages / s")
    c = a["consumer"]
    ax.set_title(f"{TITLES[a['scenario']]}  —  zero loss: {c.get('zero_loss')}, "
                 f"duplicates removed: {c['duplicates']}", loc="left")
    ax.legend(loc="lower left", ncol=2, fontsize=9)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f"chaos-{a['scenario']}.png"), dpi=160)
    plt.close(fig)


def bench_charts():
    b = os.path.join(RES, "bench")
    need = ["throughput.csv", "size.csv", "consumer.csv", "latency.csv"]
    if not all(os.path.exists(os.path.join(b, f)) and len(rcsv(os.path.join(b, f))) > 1 for f in need):
        print("bench results incomplete, skipping bench charts")
        return
    # throughput vs inflight, per workers
    rows = rcsv(os.path.join(b, "throughput.csv"))
    fig, ax = plt.subplots(figsize=(7, 3.8))
    for col, w in zip((S1, S2, S3), ("1", "4", "8")):
        r = [x for x in rows if x["workers"] == w]
        xs = [int(x["inflight"]) for x in r]
        ys = [float(x["throughput_msg_s"]) for x in r]
        ax.plot(xs, ys, color=col, marker="o", ms=6, label=f"{w} worker{'s' if w != '1' else ''}")
        ax.annotate(f"{ys[-1]:,.0f}", (xs[-1], ys[-1]), textcoords="offset points", xytext=(6, 0),
                    va="center", fontsize=8, color=INK2)
    ax.set_xscale("log")
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xticks([1, 16, 128, 512])
    ax.set_xticklabels(["1\n(sync)", "16", "128", "512"])
    ax.set_xlabel("publisher-confirm window (unconfirmed messages per worker)")
    ax.set_ylabel("confirmed msg/s")
    best = max(rows, key=lambda x: float(x["throughput_msg_s"]))
    sync1 = next(x for x in rows if x["workers"] == "1" and x["inflight"] == "1")
    ax.set_title(f"Pipelined confirms: {float(sync1['throughput_msg_s']):,.0f} → {float(best['throughput_msg_s']):,.0f} msg/s "
                 f"({float(best['throughput_msg_s']) / float(sync1['throughput_msg_s']):.0f}×)", loc="left")
    ax.legend(loc="upper left")
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "bench-throughput.png"), dpi=160); plt.close(fig)

    rows = rcsv(os.path.join(b, "size.csv"))
    fig, axs = plt.subplots(1, 2, figsize=(9, 3.4))
    xs = [int(x["size"]) for x in rows]
    lab = [f"{s // 1024} KiB" if s >= 1024 else f"{s} B" for s in xs]
    axs[0].bar(range(len(xs)), [float(x["throughput_msg_s"]) for x in rows], color=S1, width=0.6)
    axs[0].set_title("Messages / s", loc="left"); axs[0].set_xticks(range(len(xs))); axs[0].set_xticklabels(lab)
    axs[1].bar(range(len(xs)), [float(x["throughput_MB_s"]) for x in rows], color=S3, width=0.6)
    axs[1].set_title("Payload MB / s", loc="left"); axs[1].set_xticks(range(len(xs))); axs[1].set_xticklabels(lab)
    for ax in axs:
        ax.set_xlabel("message size")
        ax.grid(axis="x", visible=False)
    fig.suptitle("Bigger messages: fewer msg/s, more MB/s (4 workers, window 128)", x=0.01, ha="left",
                 fontweight="bold", color=INK)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "bench-size.png"), dpi=160); plt.close(fig)

    rows = rcsv(os.path.join(b, "consumer.csv"))
    fig, ax = plt.subplots(figsize=(6, 3.4))
    ax.bar(range(len(rows)), [float(x["throughput_msg_s"]) for x in rows], color=S2, width=0.6)
    for i, x in enumerate(rows):
        ax.text(i, float(x["throughput_msg_s"]), f"{float(x['throughput_msg_s']):,.0f}", ha="center",
                va="bottom", fontsize=8, color=INK2)
    ax.set_xticks(range(len(rows))); ax.set_xticklabels([x["prefetch"] for x in rows])
    ax.set_xlabel("consumer prefetch (QoS)"); ax.set_ylabel("consumed msg/s (manual ack)")
    ax.grid(axis="x", visible=False)
    ax.set_title(f"Prefetch 1 → 500 raises consume rate {float(rows[-1]['throughput_msg_s']) / float(rows[0]['throughput_msg_s']):.0f}×", loc="left")
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "bench-prefetch.png"), dpi=160); plt.close(fig)

    rows = rcsv(os.path.join(b, "latency.csv"))
    fig, ax = plt.subplots(figsize=(7.5, 3.9))
    xs = [int(x["offered_rate"]) for x in rows]
    for col, k, l in ((S1, "e2e_p50_ms", "end-to-end p50"), (S3, "e2e_p99_ms", "end-to-end p99"),
                      (S4, "confirm_p50_ms", "publisher confirm p50")):
        ys = [float(x[k]) for x in rows]
        ax.plot(xs, ys, color=col, marker="o", ms=6, label=l)
        for xv, yv in zip(xs, ys):
            if k == "e2e_p99_ms" or (k == "confirm_p50_ms" and xv in (500, 8000)):
                ax.annotate(f"{yv:,.0f} ms" if yv >= 10 else f"{yv:.1f} ms", (xv, yv), textcoords="offset points",
                            xytext=(0, 8) if k == "e2e_p99_ms" else (14, -4), ha="center" if k == "e2e_p99_ms" else "left", fontsize=8, color=INK2)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xticks(xs); ax.set_xticklabels([f"{x:,}" for x in xs])
    ax.axvspan(3000, max(xs) * 1.3, color="#d9d8d3", alpha=0.5, lw=0)
    ax.text(3150, 1.2, "consumer saturated\n(backlog grows)", fontsize=8, color=INK2, va="bottom")
    ax.set_xlim(min(xs) * 0.8, max(xs) * 1.3)
    ax.set_ylim(1, max(float(x["e2e_p99_ms"]) for x in rows) * 4)
    ax.set_xlabel("offered load (msg/s, 512 B, 4 producers + 1 consumer on a 2-vCPU host)")
    ax.set_ylabel("latency (ms, log scale)")
    r2 = next(x for x in rows if x["offered_rate"] == "2000")
    ax.set_title(f"End-to-end p99 stays ≤ {float(r2['e2e_p99_ms']):.0f} ms up to 2,000 msg/s; beyond that the consumer saturates",
                 loc="left", fontsize=11)
    ax.legend(loc="upper left")
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "bench-latency.png"), dpi=160); plt.close(fig)


def main():
    rows = []
    for s in SCENARIOS:
        if not os.path.exists(os.path.join(RES, s, "consumer.json")):
            print("skip", s); continue
        a = analyse(s)
        timeline(a)
        p, c, m = a["producer"], a["consumer"], a["metrics"]
        rows.append({
            "scenario": s, "messages": p["expected_total"], "confirmed": p["confirmed_total"],
            "consumed_unique": c["unique"], "missing": c.get("missing_total"), "zero_loss": c.get("zero_loss"),
            "duplicates_removed": c["duplicates"], "out_of_order": c["out_of_order"],
            "republished": p["republished"], "producer_reconnects": p["reconnects"], "nacks": p["nacked"],
            "consumer_failovers": c["failovers"], "switch_backs": c["switch_backs"],
            "leader_before": m.get("leader_before", ""), "leader_after": m.get("leader_after", ""),
            "election_s": m.get("election_s", ""), "publish_resume_s": m.get("publish_resume_s", ""), "consumer_detect_s": m.get("consumer_detect_s", ""), "consumer_failover_s": m.get("consumer_failover_s", ""),
            "publish_stall_s": m.get("publish_stall_s", ""), "consume_stall_s": m.get("consume_stall_s", ""),
            "rejoin_s": m.get("rejoin_s", ""),
            "e2e_p50_ms": c["e2e_latency"]["p50_ms"], "e2e_p99_ms": c["e2e_latency"]["p99_ms"],
            "e2e_max_ms": c["e2e_latency"]["max_ms"],
        })
    fr = os.path.join(RES, "full-restart", "consumer.json")
    if os.path.exists(fr):
        c = rjson(fr); p = rjson(os.path.join(RES, "full-restart", "producer.json"))
        rows.append({"scenario": "full-restart", "messages": p["expected_total"], "confirmed": p["confirmed_total"],
                     "consumed_unique": c["unique"], "missing": c["missing_total"], "zero_loss": c["zero_loss"],
                     "duplicates_removed": c["duplicates"], "out_of_order": c["out_of_order"],
                     "republished": p["republished"], "producer_reconnects": p["reconnects"], "nacks": p["nacked"]})
    if rows:
        keys = list(rows[0].keys())
        with open(os.path.join(RES, "chaos_summary.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in keys})
        with open(os.path.join(RES, "chaos_summary.md"), "w") as f:
            f.write("| " + " | ".join(keys) + " |\n|" + "---|" * len(keys) + "\n")
            for r in rows:
                f.write("| " + " | ".join(str(r.get(k, "")) for k in keys) + " |\n")
        for r in rows:
            print(r)
    bench_charts()
    print("charts ->", OUT)


if __name__ == "__main__":
    sys.exit(main())
