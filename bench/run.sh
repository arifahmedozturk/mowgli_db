#!/usr/bin/env bash
# Builds Release, runs bench and ycsb REPS times, writes bench/results/<date>_<sha>[_label].json.
# Usage: bench/run.sh [--reps 5] [--n 100000] [--ops 100000] [--label name] [--no-ycsb]
set -euo pipefail

REPS=5
N=100000
OPS=100000
LABEL=""
YCSB=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --reps)    REPS="$2"; shift 2 ;;
        --n)       N="$2"; shift 2 ;;
        --ops)     OPS="$2"; shift 2 ;;
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
cmake --build "$BUILD" -j"$(nproc)" --target bench ycsb >/dev/null

for ((i = 1; i <= REPS; i++)); do
    echo "rep $i/$REPS"
    "$BUILD/bench" "$N" "$WORK/data" --csv "$WORK/bench_$i.csv" >/dev/null
    if [[ $YCSB -eq 1 ]]; then
        "$BUILD/ycsb" "$N" --ops "$OPS" --data "$WORK/data" --csv "$WORK/ycsb_$i.csv" >/dev/null
    fi
done

SHA="$(git -C "$ROOT" rev-parse --short HEAD)"
DIRTY="$(git -C "$ROOT" status --porcelain --untracked-files=no | grep -q . && echo true || echo false)"
NAME="$(date +%Y%m%d-%H%M%S)_${SHA}${LABEL:+_$LABEL}"
mkdir -p "$ROOT/bench/results"

python3 "$ROOT/bench/aggregate.py" "$WORK" "$ROOT/bench/results/$NAME.json" \
    git_sha="$SHA" git_dirty="$DIRTY" label="$LABEL" reps="$REPS" n="$N" ops="$OPS" \
    cpu="$(lscpu | sed -n 's/^Model name:[[:space:]]*//p' | head -1)" \
    cores="$(nproc)" \
    mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)" \
    kernel="$(uname -r)" \
    compiler="$(c++ --version | head -1)"
