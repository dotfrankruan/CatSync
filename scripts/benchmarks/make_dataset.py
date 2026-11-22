#!/usr/bin/env python3
"""Create the deterministic benchmark datasets.

The random payload is generated with a fixed seed so every mode transfers
identical bytes. The mixed tree exercises small-file batching and path
metadata without relying on compression or sparse-file behaviour.
"""
import argparse
import os
import random

SEED = 20261122


def make_file(path, size):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rng = random.Random(SEED ^ size ^ len(path))
    with open(path, "wb") as f:
        remaining = size
        while remaining:
            chunk = rng.randbytes(min(remaining, 1 << 20))
            f.write(chunk)
            remaining -= len(chunk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/tmp/catsync-bench-data")
    args = ap.parse_args()
    root = os.path.abspath(args.root)
    make_file(os.path.join(root, "large", "large.bin"), 128 << 20)
    make_file(os.path.join(root, "small", "small.bin"), 16 << 20)
    mixed = os.path.join(root, "mixed")
    os.makedirs(mixed, exist_ok=True)
    make_file(os.path.join(mixed, "big.bin"), 8 << 20)
    for i in range(1000):
        name = f"file-{i:04d}.txt"
        if i % 97 == 0:
            name = f"unicode-{i:04d}-目录-😀.txt"
        elif i % 89 == 0:
            name = f"space-{i:04d} name.txt"
        elif i % 83 == 0:
            name = f"hidden-{i:04d}.txt" if i % 2 else f".hidden-{i:04d}.txt"
        with open(os.path.join(mixed, name), "wb") as f:
            f.write(f"catsync benchmark file {i}\n".encode() * (1 + i % 7))
    with open(os.path.join(mixed, "empty.txt"), "wb"):
        pass
    print(root)


if __name__ == "__main__":
    main()
