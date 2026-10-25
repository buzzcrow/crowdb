#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Real echo transport regression; the test fixture owns each server lifetime.
set -euo pipefail
cd "$(dirname "$0")/../.."
unset CROWDB_ASAN
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${RPC_BENCH_LOG_ROOT:-${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/rpc-regression-$RUN_STAMP}"
RESULTS_FILE="${RPC_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
DURATION="${RPC_BENCH_DURATION:-20}"
VALUE_SIZE="${RPC_BENCH_VALUE_SIZE:-128}"
CASES="${RPC_BENCH_CASES:-}"
FAILURES=0
mkdir -p "$LOG_ROOT"
pixi run cargo build --release -p crowdb-rpc-test
pixi run cmake --build lib/crowdb-rpc/build -j 4 --target crowdb-rpc-fb-server
printf 'label\twkr\tops_s\tavg_us\tp50_us\tp99_us\tnagle\terrors\n' > "$RESULTS_FILE"

run_bench() {
    local loaders="$1" connections="$2" label="$3" engines="${4:-1}" workers="${5:-1}" mode="${6:-coroutine}" nagle="${7:-0}"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then return; fi
    local options=() code=0 output
    if [ "$nagle" = 1 ]; then options+=(--enable-nagle); fi
    echo ">>> $label"
    output=$(pixi run python tools/rpc-test/run.py --log-dir "$LOG_ROOT/$label" \
        --duration-secs "$DURATION" --loader-num "$loaders" --connections "$connections" \
        --value-size "$VALUE_SIZE" --io-engines "$engines" --io-workers "$workers" \
        --mode "$mode" "${options[@]}") || code=$?
    printf '%s\n' "$output" > "$LOG_ROOT/$label.json"
    if [ "$code" -ne 0 ] || ! jq -e '.total_ops > 0 and .total_errors == 0' <<< "$output" >/dev/null; then
        printf '%s\n' "$output" >&2
        echo "ERROR: $label failed (exit=$code)" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    jq -r --arg label "$label" --arg workers "$workers" --arg nagle "$nagle" \
        '[$label, $workers, (.total_ops * 1000 / .duration_ms), .by_op.write.latency_us.avg_us, .by_op.write.latency_us.p50_us, .by_op.write.latency_us.p99_us, $nagle, .total_errors] | @tsv' \
        <<< "$output" >> "$RESULTS_FILE"
}

# Standard regression sweep — single-engine, scale workers with load.
# Coroutine: all 4 configs. Tokio: only 1/64/1000 loaders (skip 512).
run_bench 1   1  "rpc_1e1w_1l_1c"        1 1
run_bench 1   1  "rpc_1e1w_1l_1c_tokio"  1 1 tokio
run_bench 64  4  "rpc_1e4w_64l_4c"       1 4
run_bench 64  4  "rpc_1e4w_64l_4c_tokio" 1 4 tokio
run_bench 512 8  "rpc_1e8w_512l_8c"      1 8

# High-concurrency: 1000T, single-engine 16 workers.
run_bench 1000 32 "rpc_1e16w_1000l_32c"        1 16
run_bench 1000 32 "rpc_1e16w_1000l_32c_tokio"  1 16 tokio

# Nagle-enabled (coroutine) — TCP coalescing batches multiple small
# frames per writev/read syscall. Run at 64/512/1000T to measure the
# nagle benefit across load levels.
run_bench 64   4  "rpc_1e4w_64l_4c_nagle"     1 4  coroutine 1
run_bench 512  8  "rpc_1e8w_512l_8c_nagle"    1 8  coroutine 1
run_bench 1000 32 "rpc_1e16w_1000l_32c_nagle" 1 16 coroutine 1

printf 'Results: %s\n' "$RESULTS_FILE"
cat "$RESULTS_FILE"
[ "$FAILURES" -eq 0 ]
