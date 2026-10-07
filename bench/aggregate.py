#!/usr/bin/env python3
"""Aggregate per-rep CSVs into one JSON with mean/stddev per metric.

Usage: aggregate.py <csv_dir> <out.json> [key=value ...]
CSV files are named <suite>_<rep>.csv; metrics are keyed "<suite>.<name>".
"""
import csv
import json
import statistics
import sys
from pathlib import Path


def main():
    csv_dir, out_path, *meta_args = sys.argv[1:]
    meta = dict(a.split("=", 1) for a in meta_args)

    values, units = {}, {}
    for f in sorted(Path(csv_dir).glob("*_*.csv")):
        suite = f.stem.rsplit("_", 1)[0]
        with open(f) as fh:
            for row in csv.DictReader(fh):
                key = f"{suite}.{row['name']}"
                values.setdefault(key, []).append(float(row["value"]))
                units[key] = row["unit"]

    metrics = {}
    for key, vals in values.items():
        mean = statistics.mean(vals)
        sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        metrics[key] = {"unit": units[key], "mean": mean, "stddev": sd,
                        "cv_pct": 100 * sd / mean if mean else 0.0, "values": vals}

    Path(out_path).write_text(json.dumps({"meta": meta, "metrics": metrics}, indent=2) + "\n")

    print(f"{'metric':40} {'mean':>14} {'stddev':>12} {'cv%':>6}  unit")
    for key, s in metrics.items():
        print(f"{key:40} {s['mean']:14.2f} {s['stddev']:12.2f} {s['cv_pct']:6.1f}  {s['unit']}")
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
