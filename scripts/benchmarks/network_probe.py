#!/usr/bin/env python3
"""Sample remote NIC counters around one transfer per mode.

This is a lightweight supplemental probe, not a replacement for the full
benchmark. Counters are read from /sys/class/net/*/statistics (non-loopback)
through a separate observation ssh; each read adds a few SSH bytes, which are
called out in docs/benchmark.md rather than hidden.
"""
import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

HOST = os.environ.get("CATSYNC_BENCH_HOST", "catsync-test@example.invalid")
REMOTE_ROOT = "/home/catsync-test/catsync-bench/network-probe"


def ssh(cmd):
    p = subprocess.run(["ssh", "-o", "BatchMode=yes", HOST, cmd], capture_output=True, text=True, timeout=30)
    return p.stdout.strip()


def counters():
    out = {}
    raw = ssh("for d in /sys/class/net/*; do n=$(basename $d); [ \"$n\" = lo ] && continue; echo \"$n $(cat $d/statistics/rx_bytes) $(cat $d/statistics/tx_bytes)\"; done")
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) == 3:
            out[parts[0]] = (int(parts[1]), int(parts[2]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="/tmp/catsync-detach")
    ap.add_argument("--remote-catsyncd", default="/home/catsync-test/.local/bin/catsyncd")
    ap.add_argument("--source", default="/tmp/catsync-bench-data/small/small.bin")
    ap.add_argument("--out", default="docs/assets/benchmarks/network.json")
    args = ap.parse_args()
    ssh(f"mkdir -p -- {REMOTE_ROOT}")
    specs = [
        ("legacy SSH", ["--transport", "ssh"]),
        ("daemon SSH-forward", ["--transport", "daemon", "--remote-catsyncd", args.remote_catsyncd,
                                "--daemon-bootstrap", "ssh-forward", "--daemon-transport", "tcp"]),
        ("daemon direct TCP", ["--transport", "daemon", "--remote-catsyncd", args.remote_catsyncd,
                               "--daemon-bootstrap", "direct", "--daemon-transport", "tcp"]),
        ("daemon direct QUIC", ["--transport", "daemon", "--remote-catsyncd", args.remote_catsyncd,
                                "--daemon-bootstrap", "direct", "--daemon-transport", "quic"]),
    ]
    rows = []
    for label, extra in specs:
        dest = f"{REMOTE_ROOT}/{label.replace(' ', '-')}"
        ssh(f"rm -rf -- {json.dumps(dest)}; mkdir -p -- {json.dumps(dest)}")
        before = counters()
        cmd = [args.binary, "copy"] + extra + ["--progress", "never", "--verify", "sha256", args.source, f"{HOST}:{dest}"]
        start = time.monotonic()
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        elapsed = time.monotonic() - start
        after = counters()
        deltas = {}
        for iface, (rx0, tx0) in before.items():
            rx1, tx1 = after.get(iface, (rx0, tx0))
            deltas[iface] = {"rx_bytes": rx1 - rx0, "tx_bytes": tx1 - tx0}
        rows.append({"mode": label, "payload_bytes": Path(args.source).stat().st_size,
                     "elapsed_s": round(elapsed, 3), "exit_code": p.returncode,
                     "nic_deltas": deltas, "stderr_tail": p.stderr.strip().splitlines()[-3:]})
        ssh(f"rm -rf -- {json.dumps(dest)}")
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"schema": 1, "note": "Remote NIC counters include the observation SSH connections.", "runs": rows}, indent=2) + "\n")
    print(out)


if __name__ == "__main__":
    main()
