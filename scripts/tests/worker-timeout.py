#!/usr/bin/env python3
"""Verify that a detached one-shot worker exits after a SIGKILLed client.

The client process is killed after the direct session is established and a
worker PID is visible. The observation ssh is not part of the transfer path.
"""
import argparse, json, os, subprocess, tempfile, time
from pathlib import Path

HOST = os.environ.get("CATSYNC_E2E_HOST", "catsync-test@example.invalid")
ROOT = "/home/catsync-test/catsync-e2e/worker-timeout"


def ssh(cmd):
    return subprocess.run(["ssh", "-o", "BatchMode=yes", HOST, cmd], capture_output=True, text=True, timeout=20).stdout.strip()


def worker():
    return ssh("pgrep -x catsyncd | head -1") or None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="/tmp/catsync-detach")
    ap.add_argument("--remote-catsyncd", default="/home/catsync-test/.local/bin/catsyncd")
    ap.add_argument("--source", default="/tmp/catsync-bench-data/large/large.bin")
    ap.add_argument("--out", default="docs/assets/benchmarks/cleanup.json")
    args = ap.parse_args()
    ssh(f"pkill -x catsyncd 2>/dev/null || true; rm -rf -- {ROOT}; mkdir -p -- {ROOT}")
    cmd = [args.binary, "copy", "--transport", "daemon", "--remote-catsyncd", args.remote_catsyncd,
           "--daemon-bootstrap", "direct", "--daemon-transport", "tcp", "--progress", "never",
           args.source, f"{HOST}:{ROOT}"]
    started = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    pid = None
    deadline = started + 30
    while time.monotonic() < deadline:
        pid = worker()
        if pid and time.monotonic() - started > 2:
            break
        time.sleep(0.2)
    killed_at = time.monotonic()
    if proc.poll() is None:
        os.kill(proc.pid, 9)
    proc.wait()
    gone_at = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if worker() is None:
            gone_at = time.monotonic()
            break
        time.sleep(0.5)
    result = {
        "test": "client SIGKILL after handoff",
        "binary": "workspace-detach-prototype",
        "worker_pid": pid,
        "client_exit_ms": round((killed_at - started) * 1000),
        "worker_exit_after_client_kill_ms": round((gone_at - killed_at) * 1000) if gone_at else None,
        "worker_exited": gone_at is not None,
        "observation": "separate ssh pgrep only; not part of the transfer",
    }
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n")
    print(out)
    if not result["worker_exited"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
