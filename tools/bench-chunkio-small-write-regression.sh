#!/usr/bin/env bash
# --- CROWDB full-stack small-write regression benchmark ---
# Usage: bash tools/bench-chunkio-small-write-regression.sh
#
# Real client/ChunkDB/DiskDB metadata flow with mem-block KV/WAL and NullDisk
# data. Each case runs for 20 seconds; TPS is successful object responses only.
# The report also records aggregate objects/buffers per DiskIO write request and
# queue-driven pipeline scale-out/scale-in behavior (initial 1, maximum 32).
#
# Reference platform: Intel Core i9-7960X (16c/32t, x86_64, Linux).
# Configuration: 8 DiskIO connections/endpoint, 1 client DiskIO RPC worker,
# 4 MiB scale-out queue threshold, completion-driven batching, mem-block
# metadata, NullDisk data (2026-09-09).
#
# Reference results (all errors=0, incomplete=0, stop=complete):
#   size    threads    success TPS    MiB/s    p50 us    p99 us
#   1 KiB         1         424.42       0.4     2,309      3,532
#   1 KiB         4         856.48       0.8     4,627      6,095
#   1 KiB        32       6,275.96       6.1     5,032      9,448
#   1 KiB       128      19,508.32      19.1     6,166     13,627
#   1 KiB       256      31,313.08      30.6     7,549     19,388
#   8 KiB         1         390.94       3.1     2,492      3,909
#   8 KiB         4         675.29       5.3     5,323     10,882
#   8 KiB        32       4,464.84      34.9     6,701     18,595
#   8 KiB       128      13,461.05     105.2     8,791     23,066
#   8 KiB       256      23,446.56     183.2     9,811     27,503
# The 1 KiB/1-thread distribution rerun additionally measured p90=2,665 us,
# p95=2,918 us, and max=26,575 us across 8,489 successful responses.
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
DISKIO_RPC_WORKERS="${CHUNKIO_SMALL_BENCH_DISKIO_RPC_WORKERS:-1}"
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
    local label="$1" size="$2" concurrency="$3" queue_objects="$4"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    if [ -n "$SCALE_OUT_QUEUE_OBJECTS" ]; then
        queue_objects="$SCALE_OUT_QUEUE_OBJECTS"
    fi
    if [ "$CASE_NUMBER" -gt 0 ]; then
        regression_reset_stack 1
    fi
    CASE_NUMBER=$((CASE_NUMBER + 1))
    echo ">>> $label (size=$size concurrency=$concurrency)"
    local output status line requested completed errors incomplete stop scale_out
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
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$size" "$concurrency" "$requested" "$completed" \
        "$errors" "$incomplete" "$stop" "$(field "$line" objects_s)" \
        "$(field "$line" logical_mib_s)" "$(field "$line" p50_us)" \
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
        >>"$RESULTS_FILE"
    if [ "$status" -ne 0 ] || [ -z "$line" ] || [ -z "$requested" ] || [ -z "$completed" ] \
        || [ "$completed" -eq 0 ] || [ "$completed" != "$requested" ] \
        || [ "$errors" != 0 ] || [ "$incomplete" != 0 ] \
        || [ "$stop" != complete ] || [ -z "$(field "$line" batches)" ] \
        || [ "$(field "$line" batch_watchdog_expirations)" != 0 ] \
        || [ -z "$(field "$line" aggregate_write_requests)" ]; then
        echo "ERROR: $label failed accounting" >&2
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
printf 'case\tsize_bytes\tconcurrency\trequested\tcompleted\terrors\tincomplete\tstop\tobjects_s\tlogical_mib_s\tp50_us\tp90_us\tp95_us\tp99_us\tmax_us\tbatches\tbatch_watchdog_expirations\tmax_batch_objects\taggregate_write_requests\taggregate_write_objects\taggregate_write_buffers\taggregate_write_payload_bytes\tmax_objects_per_write_request\tmax_buffers_per_write_request\tmax_active_pipelines\tscale_out\tscale_in\n' >"$RESULTS_FILE"

deploy_args=(cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync)
if [ -n "$SERVER_RPC_WORKERS" ]; then
    deploy_args+=(--diskio-rpc-workers "$SERVER_RPC_WORKERS")
fi
regression_cli "${deploy_args[@]}"
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
