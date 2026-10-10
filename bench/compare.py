#!/usr/bin/env python3
"""Compare two result files from bench/run.sh.

Usage: compare.py <base.json> <new.json>
A change is flagged only when it exceeds 2x the larger of the two runs' stddev.
"""
import json
import sys

# Metrics where a smaller value is an improvement.
LOWER_IS_BETTER_UNITS = {"ms", "us", "chains", "blocks", "faults", "bytes"}


def main():
    base, new = (json.load(open(p)) for p in sys.argv[1:3])
    for side, r in (("base", base), ("new", new)):
        m = r["meta"]
        print(f"{side}: {m.get('git_sha')} dirty={m.get('git_dirty')} reps={m.get('reps')} "
              f"n={m.get('n')} {m.get('label', '')}")
    if base["meta"].get("cpu") != new["meta"].get("cpu"):
        print("WARNING: different CPUs, numbers are not comparable")
    print()

    print(f"{'metric':40} {'base':>14} {'new':>14} {'delta':>8}")
    for key, b in base["metrics"].items():
        n = new["metrics"].get(key)
        if n is None or b["mean"] == 0:
            continue
        delta = 100 * (n["mean"] - b["mean"]) / b["mean"]
        noise = 2 * max(b["stddev"], n["stddev"])
        mark = ""
        if abs(n["mean"] - b["mean"]) > noise:
            better = (delta < 0) if b["unit"] in LOWER_IS_BETTER_UNITS else (delta > 0)
            mark = "  better" if better else "  WORSE"
        print(f"{key:40} {b['mean']:14.2f} {n['mean']:14.2f} {delta:+7.1f}%{mark}")


if __name__ == "__main__":
    main()
