#!/usr/bin/env bash
# CROWDB full-stack read benchmark using real mem-block metadata and NullDisk data.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNKIO_READ_BENCH_CASES:-}"
DURATION="${CHUNKIO_READ_BENCH_DURATION:-10}"
TIMEOUT_SECS="${CHUNKIO_READ_BENCH_TIMEOUT:-180}"
DATASET_OBJECTS="${CHUNKIO_READ_DATASET_OBJECTS:-16}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKIO_READ_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkio-read-$RUN_STAMP}"
RESULTS_FILE="${CHUNKIO_READ_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
CURRENT_CONFIG="$REGRESSION_CONFIG"
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ && "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ \
    && "$DATASET_OBJECTS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: duration, timeout, and dataset size must be positive integers" >&2
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
    local label="$1" verb="$2" concurrency="$3"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    if [ "$CASE_NUMBER" -gt 0 ]; then
        regression_reset_stack 1
    fi
    CASE_NUMBER=$((CASE_NUMBER + 1))
    echo ">>> $label ($verb concurrency=$concurrency)"
    local output status line requested reads errors incomplete stop small_reads large_reads
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$TIMEOUT_SECS" \
        pixi run -- ./target/release/crowdb-cli --log-root "$LOG_ROOT" \
        --config "$CURRENT_CONFIG" bench chunkio "$verb" \
        --requests 18446744073709551615 --duration-secs "$DURATION" \
        --dataset-objects "$DATASET_OBJECTS" --concurrency "$concurrency" \
        --diskio-connections 8 \
        --small-object-size 8192 --large-object-size 16777216 \
        --mixed-large-percent 50 --data-num 8 --code-num 4 \
        --block-size 1048576 --chunk-size 1073741824 --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n "/^chunkio ${verb}:/p" <<<"$output" | tail -n 1)
    requested=$(field "$line" requested)
    reads=$(field "$line" reads)
    small_reads=$(field "$line" small_reads)
    large_reads=$(field "$line" large_reads)
    errors=$(field "$line" errors)
    incomplete=$(field "$line" incomplete)
    stop=$(field "$line" stop)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$verb" "$concurrency" "$requested" "$reads" \
        "$small_reads" "$large_reads" "$errors" "$incomplete" "$stop" \
        "$(field "$line" reads_s)" >>"$RESULTS_FILE"
    local mix_valid=1
    if [ "$verb" = read-mix ] \
        && { [ "${small_reads:-0}" -eq 0 ] || [ "${large_reads:-0}" -eq 0 ]; }; then
        mix_valid=0
    fi
    if [ "$status" -ne 0 ] || [ -z "$line" ] || [ -z "$requested" ] || [ -z "$reads" ] \
        || [ "$reads" -eq 0 ] || [ "$reads" != "$requested" ] \
        || [ "$errors" != 0 ] || [ "$incomplete" != 0 ] || [ "$stop" != complete ] \
        || [ "$mix_valid" -ne 1 ] || [ -z "$(field "$line" logical_bytes)" ]; then
        echo "ERROR: $label failed accounting" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
pixi run build-cpp
mkdir -p "$LOG_ROOT"
regression_init
printf 'case\tverb\tconcurrency\trequested\treads\tsmall_reads\tlarge_reads\terrors\tincomplete\tstop\treads_s\n' >"$RESULTS_FILE"

regression_cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync
run_case read_small_1t read-small 1
run_case read_small_32t read-small 32
run_case read_large_32t read-large 32
run_case read_mix_32t read-mix 32
destroy_cluster

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
