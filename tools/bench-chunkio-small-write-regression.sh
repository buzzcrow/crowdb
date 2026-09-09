#!/usr/bin/env bash
# CROWDB full-stack small-write benchmark using mem-block metadata and NullDisk data.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNKIO_SMALL_BENCH_CASES:-}"
DURATION="${CHUNKIO_SMALL_BENCH_DURATION:-10}"
TIMEOUT_SECS="${CHUNKIO_SMALL_BENCH_TIMEOUT:-120}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKIO_SMALL_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkio-small-write-$RUN_STAMP}"
RESULTS_FILE="${CHUNKIO_SMALL_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
CURRENT_CONFIG="$REGRESSION_CONFIG"
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ && "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: durations must be positive integers" >&2
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
        --object-size "$size" --concurrency "$concurrency" --diskio-connections 8 \
        --max-pipelines 32 \
        --scale-out-queue-bytes 4194304 --scale-out-queue-objects "$queue_objects" \
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
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$size" "$concurrency" "$requested" "$completed" \
        "$errors" "$incomplete" "$stop" "$(field "$line" objects_s)" "$scale_out" \
        >>"$RESULTS_FILE"
    if [ "$status" -ne 0 ] || [ -z "$line" ] || [ -z "$requested" ] || [ -z "$completed" ] \
        || [ "$completed" -eq 0 ] || [ "$completed" != "$requested" ] \
        || [ "$errors" != 0 ] || [ "$incomplete" != 0 ] \
        || [ "$stop" != complete ] || [ -z "$(field "$line" batches)" ]; then
        echo "ERROR: $label failed accounting" >&2
        FAILURES=$((FAILURES + 1))
    elif [ "$queue_objects" -eq 1 ] && [ "${scale_out:-0}" -eq 0 ]; then
        echo "ERROR: $label did not exercise queue-driven scale-out" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
pixi run build-cpp
mkdir -p "$LOG_ROOT"
regression_init
printf 'case\tsize_bytes\tconcurrency\trequested\tcompleted\terrors\tincomplete\tstop\tobjects_s\tscale_out\n' >"$RESULTS_FILE"

regression_cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync
run_case small_1k_1t 1024 1 128
run_case small_1k_32t 1024 32 1
run_case small_8k_32t 8192 32 1
destroy_cluster

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
