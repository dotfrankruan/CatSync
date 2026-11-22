#!/usr/bin/env python3
"""Print the benchmark tables used in docs/benchmark.md from results.json."""
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def med(vals):
    return statistics.median(vals) if vals else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="docs/assets/benchmarks/results.json")
    ap.add_argument("--lifecycle", default="docs/assets/benchmarks/lifecycle.json")
    args = ap.parse_args()
    doc = json.loads(Path(args.results).read_text())
    groups = defaultdict(list)
    for r in doc["runs"]:
        groups[(r["direction"], r["dataset"], r["mode"])].append(r)
    print("| Direction | Dataset | Mode | n | OK | Fail | Median Mbps | Min/Max | Median elapsed ms |")
    print("|---|---|---|---:|---:|---:|---:|---|---:|")
    for key in sorted(groups):
        rows = groups[key]
        ok = [r for r in rows if r["success"]]
        fails = len(rows) - len(ok)
        mbps = [r["effective_mbps"] for r in ok if r.get("effective_mbps") is not None]
        elapsed = [r["elapsed_ms"] for r in ok]
        m = med(mbps)
        mn = f"{min(mbps):.1f}/{max(mbps):.1f}" if mbps else "-"
        print(f"| {key[0]} | {key[1]} | {key[2]} | {len(rows)} | {len(ok)} | {fails} | {m:.2f} | {mn} | {med(elapsed):.0f} |" if m else
              f"| {key[0]} | {key[1]} | {key[2]} | {len(rows)} | {len(ok)} | {fails} | - | - | - |")
    print("\nPhases (median ms) for successful detached runs:")
    print("| Mode | Dataset | bootstrap/auth | handoff | transfer+verify |")
    print("|---|---|---:|---:|---:|")
    pg = defaultdict(list)
    for r in doc["runs"]:
        if not r.get("success") or not r.get("timing_ms"):
            continue
        ev = r["timing_ms"]
        base = ev.get("bootstrap_start", 0)
        ready = ev.get("data_control_ready", base)
        hand = ev.get("ssh_closed", ready)
        end = ev.get("client_close", hand)
        pg[(r["mode"], r["dataset"])].append((ready - base, hand - ready, end - hand))
    for key, vals in sorted(pg.items()):
        cols = [med([v[i] for v in vals]) for i in range(3)]
        print(f"| {key[0]} | {key[1]} | {cols[0]:.0f} | {cols[1]:.0f} | {cols[2]:.0f} |")
    if Path(args.lifecycle).exists():
        lc = json.loads(Path(args.lifecycle).read_text())
        print("\nLifecycle cases:")
        for c in lc.get("cases", []):
            print(f"- {c['label']}: ssh_killed={c['ssh_killed']} worker_exited={c['worker_exited']} sha_match={c['sha256_match']} ssh_end={c['ssh_end_ms']}ms data_end={c['data_end_ms']}ms")


if __name__ == "__main__":
    main()
