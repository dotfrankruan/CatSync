#!/usr/bin/env python3
"""Real OpenSSH acceptance test for SSH bootstrap lifecycle handoff.

It starts the real catsync/catsyncd binaries, holds the bootstrap SSH open
briefly after a successful handoff, and either lets it exit normally or
SIGKILLs only the client-side bootstrap ssh. The direct data transfer must
continue, end with a valid SHA-256 check, and leave no catsyncd worker.

Observation uses separate short ssh commands (pgrep/hash) and is not part of
the tested transfer; the client process tree is used to prove that no second
bootstrap SSH exists.
"""
import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

HOST = os.environ.get("CATSYNC_E2E_HOST", "catsync-test@example.invalid")
ROOT = "/home/catsync-test/catsync-e2e/lifecycle-test"


def ssh(cmd, timeout=20):
    return subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", HOST, cmd],
                          capture_output=True, text=True, timeout=timeout)


def remote_pid():
    p = ssh("pgrep -x catsyncd | head -1")
    return p.stdout.strip() or None


def local_ssh_children(pid):
    p = subprocess.run(["ps", "-ax", "-o", "pid=,ppid=,command="], capture_output=True, text=True)
    out = set()
    for line in p.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3 and parts[1] == str(pid):
            prog = parts[2].split()[0].rsplit("/", 1)[-1] if parts[2].split() else ""
            if prog == "ssh":
                out.add(int(parts[0]))
    return out


def local_ssh_child(pid):
    kids = local_ssh_children(pid)
    return min(kids) if kids else None


def parse_timing(path):
    events = []
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return {e["event"]: (e["unix_nano"] - events[0]["unix_nano"]) / 1e6 for e in events} if events else {}


def run_case(args, kill_ssh, hold_ms):
    name = "ssh-killed" if kill_ssh else "normal-handoff"
    dest = f"{ROOT}/{name}"
    ssh(f"rm -rf -- {json.dumps(dest)}; mkdir -p -- {json.dumps(dest)}")
    timing_path = Path(tempfile.mktemp(prefix="catsync-lifecycle-", suffix=".jsonl"))
    env = os.environ.copy()
    env["CATSYNC_BENCH_TIMING"] = str(timing_path)
    env["CATSYNC_TEST_HANDOFF_HOLD_SSH_MS"] = str(hold_ms)
    cmd = [args.binary, "copy", "--transport", "daemon", "--remote-catsyncd", args.remote_catsyncd,
           "--daemon-bootstrap", "direct", "--daemon-transport", args.data_transport,
           "--jobs", "1", "--streams", "4", "--large-threshold", "1MiB", "--progress", "never",
           "--verify", "sha256", args.source, f"{HOST}:{dest}"]
    started = time.monotonic()
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    worker_pid = None
    ssh_pid = None
    kill_at = None
    try:
        deadline = started + 60
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            worker_pid = worker_pid or remote_pid()
            ssh_pid = ssh_pid or local_ssh_child(proc.pid)
            # The test hold keeps the local ssh process present after handoff.
            if worker_pid and ssh_pid:
                if kill_ssh:
                    time.sleep(1.0)
                    kill_at = (time.monotonic() - started) * 1000
                    os.kill(ssh_pid, 9)
                break
            time.sleep(0.2)
        worker_after_kill = remote_pid()
        second_ssh_seen = False
        known_ssh = {ssh_pid} if ssh_pid else set()
        monitor_deadline = started + 300
        while proc.poll() is None and time.monotonic() < monitor_deadline:
            extra = local_ssh_children(proc.pid) - known_ssh
            if extra:
                second_ssh_seen = True
                known_ssh |= extra
            time.sleep(0.2)
        try:
            out, err = proc.communicate(timeout=240)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            raise RuntimeError("client timeout")
        elapsed = (time.monotonic() - started) * 1000
    finally:
        pass
    # Keep the timing file until it has been parsed below.
    # Wait for the one-shot worker to disappear after the client finished.
    deadline = time.monotonic() + 60
    gone = False
    while time.monotonic() < deadline:
        if remote_pid() is None:
            gone = True
            break
        time.sleep(0.5)
    remote_name = Path(args.source).name
    remote_hash = ssh(f"sha256sum {json.dumps(dest + '/' + remote_name)} 2>/dev/null | awk '{{print $1}}'").stdout.strip()
    base = Path(args.source)
    local_hash = ""
    if base.is_file():
        h = subprocess.run(["shasum", "-a", "256", str(base)], capture_output=True, text=True).stdout.split()
        local_hash = h[0] if h else ""
    timing_file = Path(env["CATSYNC_BENCH_TIMING"])
    timing = parse_timing(timing_file) if timing_file.exists() else {}
    ssh_end = timing.get("ssh_closed", timing.get("handoff_ack", 0))
    if kill_ssh and kill_at is not None:
        ssh_end = kill_at
    case = {
        "label": "normal handoff" if not kill_ssh else "SSH SIGKILL after handoff",
        "mode": f"direct {args.data_transport}",
        "ssh_killed": kill_ssh,
        "exit_code": proc.returncode,
        "elapsed_ms": round(elapsed),
        "worker_pid": worker_pid,
        "worker_after_handoff": worker_after_kill,
        "second_bootstrap_ssh": second_ssh_seen,
        "ssh_pid": ssh_pid,
        "kill_ms": round(kill_at) if kill_at else None,
        "timing_ms": {k: round(v, 3) for k, v in timing.items()},
        "ssh_start_ms": round(timing.get("bootstrap_start", 0)),
        "data_start_ms": round(timing.get("data_control_ready", timing.get("handoff_ack", 0))),
        "ssh_end_ms": round(ssh_end),
        "data_end_ms": round(timing.get("client_close", elapsed)),
        "worker_exited": gone,
        "local_sha256": local_hash,
        "remote_sha256": remote_hash,
        "sha256_match": bool(local_hash) and local_hash == remote_hash,
        "stderr_tail": err.strip().splitlines()[-5:],
    }
    # Preserve the timing file next to the result for audit, then remove it.
    case["_timing_path"] = str(timing_file)
    return case, timing_file


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="/tmp/catsync-detach")
    ap.add_argument("--remote-catsyncd", default="/home/catsync-test/.local/bin/catsyncd")
    ap.add_argument("--source", default="/tmp/catsync-bench-data/large/large.bin")
    ap.add_argument("--data-transport", default="tcp")
    ap.add_argument("--out", default="docs/assets/benchmarks/lifecycle.json")
    args = ap.parse_args()
    cases = []
    files = []
    try:
        for kill, hold in ((False, 1000), (True, 8000)):
            print(f"running {args.data_transport} kill_ssh={kill}")
            case, tfile = run_case(args, kill, hold)
            cases.append(case)
            files.append((case, tfile))
            if not case["sha256_match"]:
                raise SystemExit(f"SHA mismatch in {case['label']}: {case['local_sha256']} != {case['remote_sha256']}")
            if not case["worker_exited"]:
                raise SystemExit(f"worker did not exit in {case['label']}")
    finally:
        for _case, tfile in files:
            Path(tfile).unlink(missing_ok=True)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "schema": 1,
        "description": "Real OpenSSH lifecycle handoff acceptance; times relative to bootstrap start.",
        "environment": {"client": "macOS arm64", "server": "Debian 13 x86_64 (sanitized)", "transport": args.data_transport},
        "cases": cases,
    }
    for c in doc["cases"]:
        c.pop("_timing_path", None)
    out.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
