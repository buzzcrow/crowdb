#!/usr/bin/env bash
# Long steady-state foreground EC versus mirror-only small-write sentinel.
set -euo pipefail
cd "$(dirname "$0")/.."

export CHUNKIO_SMALL_BENCH_MODE=ab
export CHUNKIO_SMALL_BENCH_DURATION="${CHUNKIO_SMALL_BENCH_DURATION:-60}"
export CHUNKIO_SMALL_BENCH_CASES="${CHUNKIO_SMALL_BENCH_CASES:-small_1k_32t small_1k_128t}"
export CHUNKIO_SMALL_BENCH_MIN_EC_MIRROR_RATIO_PCT="${CHUNKIO_SMALL_BENCH_MIN_EC_MIRROR_RATIO_PCT:-80}"

exec bash tools/bench-chunkio-small-write-regression.sh
