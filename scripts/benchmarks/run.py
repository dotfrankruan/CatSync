#!/usr/bin/env python3
"""Run the CatSync transfer benchmark and write machine-readable results.

This script is deliberately dependency-free. It records client wall-clock
phases, child CPU/RSS, success/failure, and verifies each successful transfer
with the client's SHA-256 check. Network counters and remote CPU are not
fabricated; when they cannot be sampled they are omitted.
"""
import argparse
import datetime as dt
import json
import os
import resource
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

HOST = os.environ.get("CATSYNC_BENCH_HOST", "catsync-test@example.invalid")
REMOTE_ROOT = os.environ.get("CATSYNC_BENCH_REMOTE_ROOT", "/home/catsync-test/catsync-bench")
CURRENT = "workspace-detach-prototype"
OLD = "v0.2.0-pre-detach"


def ssh(remote_cmd, check=False):
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", HOST, remote_cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
    )


def remove_remote(path):
    ssh(f"rm -rf -- {json.dumps(path)}")


def parse_timing(path):
    if not path.exists():
        return {}
    events = []
    for line in path.read_text().splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not events:
        return {}
    base = events[0]["unix_nano"]
    out = {}
    for ev in events:
        out[ev["event"]] = (ev["unix_nano"] - base) / 1e6
    return out


def run_one(args, spec, dataset, repeat):
    run_id = f"{spec['id']}-{dataset['id']}-r{repeat}"
    local_dest = Path(tempfile.mkdtemp(prefix="catsync-bench-dl-"))
    remote_dest = f"{REMOTE_ROOT}/{run_id}"
    remove_remote(remote_dest)
    ssh(f"mkdir -p -- {json.dumps(remote_dest)}")
    if dataset["direction"] == "upload":
        src = dataset["local_path"]
        dst = f"{HOST}:{remote_dest}"
    else:
        src = f"{HOST}:{dataset['remote_path']}"
        dst = str(local_dest)
    timing = Path(tempfile.mktemp(prefix="catsync-timing-", suffix=".jsonl"))
    env = os.environ.copy()
    env["CATSYNC_BENCH_TIMING"] = str(timing)
    binary = args.old_binary if spec.get("old") else args.binary
    cmd = [binary, "copy", "--transport", spec["transport"]]
    if spec.get("bootstrap"):
        cmd += ["--daemon-bootstrap", spec["bootstrap"]]
    if spec.get("data_transport"):
        cmd += ["--daemon-transport", spec["data_transport"]]
    if spec.get("remote_catsyncd"):
        cmd += ["--remote-catsyncd", spec["remote_catsyncd"]]
    if spec.get("old"):
        cmd += ["--remote-catsyncd", args.old_remote_catsyncd]
    cmd += [
        "--jobs", "4", "--streams", str(spec.get("streams", 4)),
        "--large-threshold", "1MiB", "--progress", "never",
        "--verify", "sha256", src, dst,
    ]
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    try:
        proc = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, timeout=args.timeout)
        elapsed = time.monotonic() - started
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        events = parse_timing(timing)
        success = proc.returncode == 0
        failure = "" if success else (proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else f"exit {proc.returncode}")
        row = {
            "id": run_id,
            "mode": spec["label"],
            "implementation": OLD if spec.get("old") else CURRENT,
            "direction": dataset["direction"],
            "dataset": dataset["id"],
            "bytes": dataset["bytes"],
            "files": dataset["files"],
            "repeat": repeat,
            "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "elapsed_ms": round(elapsed * 1000),
            "success": success,
            "exit_code": proc.returncode,
            "failure": failure,
            "cpu_user_s": round(after.ru_utime - before.ru_utime, 3),
            "cpu_sys_s": round(after.ru_stime - before.ru_stime, 3),
            "maxrss_kb": after.ru_maxrss,
            "effective_mbps": round(dataset["bytes"] * 8 / elapsed / 1e6, 3) if elapsed > 0 else None,
            "timing_ms": {k: round(v, 3) for k, v in events.items()},
            "stdout_tail": proc.stdout.strip().splitlines()[-3:],
            "stderr_tail": proc.stderr.strip().splitlines()[-6:],
        }
        if success:
            row["verify_note"] = "client --verify sha256 passed"
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - started
        row = {
            "id": run_id, "mode": spec["label"], "implementation": OLD if spec.get("old") else CURRENT,
            "direction": dataset["direction"], "dataset": dataset["id"],
            "bytes": dataset["bytes"], "files": dataset["files"], "repeat": repeat,
            "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "elapsed_ms": round(elapsed * 1000), "success": False, "exit_code": None,
            "failure": f"timeout after {args.timeout}s", "effective_mbps": None,
            "timing_ms": {}, "stdout_tail": [], "stderr_tail": [],
        }
    finally:
        try:
            timing.unlink()
        except FileNotFoundError:
            pass
        shutil.rmtree(local_dest, ignore_errors=True)
        remove_remote(remote_dest)
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="/tmp/catsync-detach")
    ap.add_argument("--old-binary", default="/opt/homebrew/bin/catsync")
    ap.add_argument("--old-remote-catsyncd", default="/home/catsync-test/.local/bin/catsyncd.pre-detach")
    ap.add_argument("--remote-catsyncd", default="/home/catsync-test/.local/bin/catsyncd")
    ap.add_argument("--data", default="/tmp/catsync-bench-data")
    ap.add_argument("--out", default="docs/assets/benchmarks/results.json")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=180)
    args = ap.parse_args()
    root = Path(args.data)
    datasets = [
        {"id": "large-128MiB", "bytes": 128 << 20, "files": 1, "direction": "upload", "local_path": str(root / "large" / "large.bin")},
        {"id": "small-16MiB", "bytes": 16 << 20, "files": 1, "direction": "download", "remote_path": f"{REMOTE_ROOT}/data/small/small.bin"},
        {"id": "mixed-1001-files", "bytes": 8 << 20, "files": 1002, "direction": "upload", "local_path": str(root / "mixed")},
    ]
    specs = [
        {"id": "legacy", "label": "legacy SSH", "transport": "ssh"},
        {"id": "forward", "label": "daemon SSH-forward", "transport": "daemon", "bootstrap": "ssh-forward", "data_transport": "tcp", "remote_catsyncd": args.remote_catsyncd},
        {"id": "direct-tcp", "label": "daemon direct TCP", "transport": "daemon", "bootstrap": "direct", "data_transport": "tcp", "remote_catsyncd": args.remote_catsyncd},
        {"id": "direct-quic", "label": "daemon direct QUIC", "transport": "daemon", "bootstrap": "direct", "data_transport": "quic", "remote_catsyncd": args.remote_catsyncd},
        {"id": "old-direct-tcp", "label": "pre-detach direct TCP (v0.2.0)", "transport": "daemon", "bootstrap": "direct", "data_transport": "tcp", "old": True},
    ]
    # Seed the remote download dataset once (upload it first with the current client).
    ssh(f"mkdir -p -- {json.dumps(REMOTE_ROOT + '/data/small')}")
    subprocess.run([args.binary, "copy", "--transport", "ssh", "--progress", "never", str(root / "small" / "small.bin"), f"{HOST}:{REMOTE_ROOT}/data/small/"], check=True)
    rows = []
    for dataset in datasets:
        for spec in specs:
            if dataset["id"] == "mixed-1001-files" and spec["id"] in ("legacy", "forward"):
                continue
            for rep in range(1, args.reps + 1):
                print(f"running {spec['label']} {dataset['id']} rep {rep}", flush=True)
                rows.append(run_one(args, spec, dataset, rep))
    # Stream concurrency on direct TCP with the large upload.
    for streams in (1, 4, 8):
        for rep in range(1, args.reps + 1):
            spec = {"id": f"tcp-streams{streams}", "label": f"direct TCP streams={streams}",
                    "transport": "daemon", "bootstrap": "direct", "data_transport": "tcp",
                    "remote_catsyncd": args.remote_catsyncd, "streams": streams}
            print(f"running {spec['label']} large-128MiB rep {rep}", flush=True)
            rows.append(run_one(args, spec, datasets[0], rep))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "schema": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "client": {"implementation": CURRENT, "platform": "macOS arm64", "vps": "Debian 13 x86_64 (sanitized host)"},
        "datasets": datasets,
        "specs": specs,
        "runs": rows,
    }
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {out} ({len(rows)} runs)")


if __name__ == "__main__":
    main()
