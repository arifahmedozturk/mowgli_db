#!/usr/bin/env bash
# Builds Release, runs bench, bench_wal, bench_btree (if built) and ycsb REPS times, writes bench/results/<date>_<sha>[_label].json.
# Usage: bench/run.sh [--reps 5] [--n 100000] [--ops 100000] [--wal-n 2000] [--keys "random_u64 seq_u64 prefix_str varlen_str"]
#        [--label name] [--no-ycsb]
set -euo pipefail

REPS=5
N=100000
OPS=100000
WAL_N=2000   # WAL-on mutations fsync twice each (~ms), so keep this small
LABEL=""
KEYS="random_u64"   # bench key distributions; non-default ones report as bench_<dist>.*
YCSB=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --reps)    REPS="$2"; shift 2 ;;
        --n)       N="$2"; shift 2 ;;
        --ops)     OPS="$2"; shift 2 ;;
        --wal-n)   WAL_N="$2"; shift 2 ;;
        --keys)    KEYS="$2"; shift 2 ;;
        --label)   LABEL="$2"; shift 2 ;;
        --no-ycsb) YCSB=0; shift ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build_bench"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cmake -S "$ROOT" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$BUILD" -j"$(nproc)" --target bench bench_wal ycsb >/dev/null
# Optional: only exists when liblmdb-dev / libsqlite3-dev are installed.
BTREE=0
rm -f "$BUILD/bench_btree"
cmake --build "$BUILD" -j"$(nproc)" --target bench_btree >/dev/null 2>&1 && BTREE=1

for ((i = 1; i <= REPS; i++)); do
    echo "rep $i/$REPS"
    for k in $KEYS; do
        suite="bench"; [[ "$k" != random_u64 ]] && suite="bench_$k"
        "$BUILD/bench" "$N" "$WORK/data" --keys "$k" --csv "$WORK/${suite}_$i.csv" >/dev/null
        if [[ $BTREE -eq 1 ]]; then
            "$BUILD/bench_btree" "$N" "$WORK/data" --keys "$k" --csv "$WORK/btree${suite#bench}_$i.csv" >/dev/null
        fi
    done
    "$BUILD/bench_wal" "$WAL_N" "$WORK/data" --csv "$WORK/wal_$i.csv" >/dev/null
    if [[ $YCSB -eq 1 ]]; then
        "$BUILD/ycsb" "$N" --ops "$OPS" --data "$WORK/data" --csv "$WORK/ycsb_$i.csv" >/dev/null
    fi
done

SHA="$(git -C "$ROOT" rev-parse --short HEAD)"
DIRTY="$(git -C "$ROOT" status --porcelain --untracked-files=no | grep -q . && echo true || echo false)"
NAME="$(date +%Y%m%d-%H%M%S)_${SHA}${LABEL:+_$LABEL}"
mkdir -p "$ROOT/bench/results"

python3 "$ROOT/bench/aggregate.py" "$WORK" "$ROOT/bench/results/$NAME.json" \
    git_sha="$SHA" git_dirty="$DIRTY" label="$LABEL" reps="$REPS" n="$N" ops="$OPS" wal_n="$WAL_N" keys="$KEYS" \
    cpu="$(lscpu | sed -n 's/^Model name:[[:space:]]*//p' | head -1)" \
    cores="$(nproc)" \
    mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)" \
    kernel="$(uname -r)" \
    compiler="$(c++ --version | head -1)"
