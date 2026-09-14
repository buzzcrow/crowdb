#!/usr/bin/env bash
# --- CROWDB full-stack small-write regression benchmark ---
# Usage: bash tools/bench-chunkio-small-write-regression.sh
#
# Real client/ChunkDB/DiskDB metadata flow with mem-block KV/WAL and NullDisk
# data. The regular matrix runs each case for 20 seconds; the EC sentinel
# wrapper runs the 32- and 128-thread cases for 20 seconds. TPS is successful
# object responses only. The report also records aggregate objects/buffers per
# DiskIO write request, reservation latency, foreground parity bytes, and
# queue-driven pipeline scale-out/scale-in behavior (initial 1, maximum 32).
#
# Reference platform: Intel Core i9-7960X (16c/32t, x86_64, Linux).
# Configuration: 8 DiskIO connections/endpoint, 1 client DiskIO RPC worker,
# 4 MiB scale-out byte threshold, 16-object threshold at 32+ threads,
# completion-driven batching, mem-block metadata, NullDisk data (2026-09-10).
#
# Latest valid 20-second EC results (errors/incomplete/watchdogs all zero):
#   size    threads    objects/s    MiB/s    p50 us    p99 us    parity_bytes
#   1 KiB        1     3,749.42      3.7       217        538      37,748,736
#   1 KiB        4     7,667.73      7.5       419      1,028      75,497,472
#   1 KiB       32    47,104.93     46.0       493      7,236     234,881,024
#   1 KiB      128   128,382.64    125.4       598     11,172     184,549,376
#   1 KiB      256   207,246.10    202.4       686     15,063     209,715,200
#   8 KiB        1     2,802.03     21.9       242        682     226,492,416
#   8 KiB        4     4,305.08     33.6       541      9,980     352,321,536
#   8 KiB       32    12,812.89    100.1       772     25,816     520,093,696
#   8 KiB      128    32,430.72    253.4     1,141     29,215     306,184,192
# Results: bench-log/chunkio-small-write-20260910-103318. The 8 KiB 256-thread
# case is excluded (7 batch watchdog expirations at extreme concurrency).
# Historical A/B baseline (60s, mirror vs EC): EC/mirror ratios were 100.23%
# (32t) and 92.01% (128t). Sources: bench-log/chunkio-small-write-20260910-081757
# and bench-log/chunkio-small-write-128-repro-20260910.
# A fresh-deployment lifecycle verification passed all ten 20-second cases;
# every KV process began at 0.16 GiB RSS and exited before the next case.
# Source: bench-log/chunkio-small-write-rss-reset-20260910.
#
# Intel i9-7960X (2026-09-10, same hw, rerun later same day):
#   Same build/config as the 2026-09-10 reference. All ten cases passed
#   with zero errors, incomplete, and batch watchdog expirations. 1 KiB
#   128t/256t now pass (prior rerun failed with diskdb accounting
#   mismatch, resolved by per-case fresh deployments). 8 KiB 256t passes
#   (excluded in reference due to 7 watchdog expirations). 128t/256t is
#   23-77% faster than reference; 32t within 12%; low concurrency
#   (1t/4t) 12-27% slower. Not strictly better — reference NOT updated.
#   Gaps > 30% documented in doc/working/regression-perf-review.md.
#
#   size    threads    objects/s    MiB/s    avg us    p50 us    p99 us
#   1 KiB        1     2,927.05      2.9      340       312       651
#   1 KiB        4     5,581.71      5.5      715       637     1,240
#   1 KiB       32    45,591.55     44.5      699       558     5,756
#   1 KiB      128   227,520.38    222.2      560       304     8,366
#   1 KiB      256   349,640.96    341.4      729       359    10,441
#   8 KiB        1     2,300.58     18.0      433       350       802
#   8 KiB        4     3,786.83     29.6    1,054       746     9,066
#   8 KiB       32    14,375.97    112.3    2,223       799    23,748
#   8 KiB      128    39,893.45    311.7    3,204       643    29,811
#   8 KiB      256    64,593.90    504.6    3,957       693    33,873
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNKIO_SMALL_BENCH_CASES:-}"
DURATION="${CHUNKIO_SMALL_BENCH_DURATION:-20}"
TIMEOUT_SECS="${CHUNKIO_SMALL_BENCH_TIMEOUT:-120}"
MAX_PIPELINES="${CHUNKIO_SMALL_BENCH_MAX_PIPELINES:-32}"
SCALE_OUT_QUEUE_BYTES="${CHUNKIO_SMALL_BENCH_SCALE_OUT_QUEUE_BYTES:-4194304}"
SCALE_OUT_QUEUE_OBJECTS="${CHUNKIO_SMALL_BENCH_SCALE_OUT_QUEUE_OBJECTS:-}"
SKIP_BUILD="${CHUNKIO_SMALL_BENCH_SKIP_BUILD:-0}"
DISKIO_CONNECTIONS="${CHUNKIO_SMALL_BENCH_DISKIO_CONNECTIONS:-8}"
DISKIO_RPC_WORKERS="${CHUNKIO_SMALL_BENCH_DISKIO_RPC_WORKERS:-4}"
SERVER_RPC_WORKERS="${CHUNKIO_SMALL_BENCH_SERVER_RPC_WORKERS:-}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKIO_SMALL_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkio-small-write-$RUN_STAMP}"
RESULTS_FILE="${CHUNKIO_SMALL_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
CURRENT_CONFIG="$REGRESSION_CONFIG"
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ && "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ \
    && "$MAX_PIPELINES" =~ ^[1-9][0-9]*$ && "$SCALE_OUT_QUEUE_BYTES" =~ ^[1-9][0-9]*$ \
    && "$DISKIO_CONNECTIONS" =~ ^[1-9][0-9]*$ \
    && "$DISKIO_RPC_WORKERS" =~ ^[1-9][0-9]*$ ]] \
    || { [ -n "$SCALE_OUT_QUEUE_OBJECTS" ] \
        && ! [[ "$SCALE_OUT_QUEUE_OBJECTS" =~ ^[1-9][0-9]*$ ]]; } \
    || { [ -n "$SERVER_RPC_WORKERS" ] && ! [[ "$SERVER_RPC_WORKERS" =~ ^[1-9][0-9]*$ ]]; }; then
    echo "ERROR: durations and pipeline queue settings must be positive integers" >&2
    exit 2
fi

destroy_cluster() {
    if [ -n "$CURRENT_CONFIG" ] && [ -f "$CURRENT_CONFIG" ]; then
        regression_destroy
    fi
    CURRENT_CONFIG=""
}
trap destroy_cluster EXIT

field() {
    local line="$1" name="$2"
    tr ' ' '\n' <<<"$line" | sed -n "s/^${name}=//p" | head -n 1
}

run_case() {
    local base_label="$1" size="$2" concurrency="$3" queue_objects="$4"
    local label="${base_label}_ec"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $base_label "* && " $CASES " != *" $label "* ]]; then
        return
    fi
    if [ -n "$SCALE_OUT_QUEUE_OBJECTS" ]; then
        queue_objects="$SCALE_OUT_QUEUE_OBJECTS"
    fi
    if [ "$CASE_NUMBER" -gt 0 ]; then
        destroy_cluster
    fi
    CURRENT_CONFIG="$REGRESSION_CONFIG"
    regression_cli "${deploy_args[@]}"
    CASE_NUMBER=$((CASE_NUMBER + 1))
    echo ">>> $label (size=$size concurrency=$concurrency)"
    local output status line requested completed errors incomplete stop scale_out parity_bytes first_reservation_us
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$TIMEOUT_SECS" \
        pixi run -- ./target/release/crowdb-cli --log-root "$LOG_ROOT" \
        --config "$CURRENT_CONFIG" bench chunkio write-small \
        --objects 18446744073709551615 --duration-secs "$DURATION" \
        --object-size "$size" --concurrency "$concurrency" \
        --diskio-connections "$DISKIO_CONNECTIONS" --diskio-rpc-workers "$DISKIO_RPC_WORKERS" \
        --max-pipelines "$MAX_PIPELINES" \
        --scale-out-queue-bytes "$SCALE_OUT_QUEUE_BYTES" --scale-out-queue-objects "$queue_objects" \
        --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n '/^chunkio write-small:/p' <<<"$output" | tail -n 1)
    requested=$(field "$line" requested)
    completed=$(field "$line" objects)
    errors=$(field "$line" errors)
    incomplete=$(field "$line" incomplete)
    stop=$(field "$line" stop)
    scale_out=$(field "$line" scale_out)
    parity_bytes=$(field "$line" foreground_parity_bytes)
    first_reservation_us=$(field "$line" first_reservation_us)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$size" "$concurrency" "$requested" "$completed" \
        "$errors" "$incomplete" "$stop" "$(field "$line" objects_s)" \
        "$(field "$line" logical_mib_s)" "$(field "$line" avg_us)" \
        "$(field "$line" p50_us)" \
        "$(field "$line" p90_us)" "$(field "$line" p95_us)" \
        "$(field "$line" p99_us)" "$(field "$line" max_us)" \
        "$(field "$line" batches)" \
        "$(field "$line" batch_watchdog_expirations)" \
        "$(field "$line" max_batch_objects)" "$(field "$line" aggregate_write_requests)" \
        "$(field "$line" aggregate_write_objects)" "$(field "$line" aggregate_write_buffers)" \
        "$(field "$line" aggregate_write_payload_bytes)" \
        "$(field "$line" max_objects_per_write_request)" \
        "$(field "$line" max_buffers_per_write_request)" \
        "$(field "$line" max_active_pipelines)" "$scale_out" "$(field "$line" scale_in)" \
        "$parity_bytes" "$(field "$line" reservation_wait_us)" "$first_reservation_us" \
        >>"$RESULTS_FILE"
    if [ "$status" -ne 0 ] || [ -z "$line" ] || [ -z "$requested" ] || [ -z "$completed" ] \
        || [ "$completed" -eq 0 ] || [ "$completed" != "$requested" ] \
        || [ "$errors" != 0 ] || [ "$incomplete" != 0 ] \
        || [ "$stop" != complete ] || [ -z "$(field "$line" batches)" ] \
        || [ -z "$(field "$line" aggregate_write_requests)" ] \
        || [ -z "$first_reservation_us" ] || [ "$first_reservation_us" -eq 0 ]; then
        echo "ERROR: $label failed accounting" >&2
        FAILURES=$((FAILURES + 1))
    elif [ -z "$parity_bytes" ] || [ "$parity_bytes" -eq 0 ]; then
        echo "ERROR: $label completed no foreground parity bytes" >&2
        FAILURES=$((FAILURES + 1))
    elif [ "$concurrency" -ge 32 ] && [ "${scale_out:-0}" -eq 0 ]; then
        echo "ERROR: $label did not exercise queue-driven scale-out" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

if [ "$SKIP_BUILD" != 1 ]; then
    echo "=== building release binaries ==="
    pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
    pixi run build-cpp
fi
mkdir -p "$LOG_ROOT"
regression_init
printf 'case\tsize_bytes\tconcurrency\trequested\tcompleted\terrors\tincomplete\tstop\tobjects_s\tlogical_mib_s\tavg_us\tp50_us\tp90_us\tp95_us\tp99_us\tmax_us\tbatches\tbatch_watchdog_expirations\tmax_batch_objects\taggregate_write_requests\taggregate_write_objects\taggregate_write_buffers\taggregate_write_payload_bytes\tmax_objects_per_write_request\tmax_buffers_per_write_request\tmax_active_pipelines\tscale_out\tscale_in\tforeground_parity_bytes\treservation_wait_us\tfirst_reservation_us\n' >"$RESULTS_FILE"

deploy_args=(cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync)
if [ -n "$SERVER_RPC_WORKERS" ]; then
    deploy_args+=(--diskio-rpc-workers "$SERVER_RPC_WORKERS")
fi
run_case small_1k_1t 1024 1 128
run_case small_1k_4t 1024 4 128
run_case small_1k_32t 1024 32 16
run_case small_1k_128t 1024 128 16
run_case small_1k_256t 1024 256 16
run_case small_8k_1t 8192 1 128
run_case small_8k_4t 8192 4 128
run_case small_8k_32t 8192 32 16
run_case small_8k_128t 8192 128 16
run_case small_8k_256t 8192 256 16
destroy_cluster

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
