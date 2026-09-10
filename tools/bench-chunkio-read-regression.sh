#!/usr/bin/env bash
# --- CROWDB full-stack chunk read regression benchmark ---
# Usage: bash tools/bench-chunkio-read-regression.sh
#
# The preparation phase writes real ChunkDB/DiskDB metadata through the client
# library into mem-block KV/WAL backends. The timed phase reads through the
# complete client -> ChunkDB -> KV and client -> DiskIO paths; DiskIO uses
# NullDisk so device capacity and media latency do not dominate the result.
# Each case runs for 20 seconds and counts only successful responses as TPS.
# cluster clean restarts the auxiliary stack between cases.
#
# Reference platform: Intel Core i9-7960X (16c/32t, x86_64, Linux).
# Configuration: 8 DiskIO connections/endpoint, 1 client DiskIO RPC worker,
# 8 KiB small objects, 16 MiB large objects, 16 prepared objects/class,
# EC 8+4, 1 MiB blocks, mem-block metadata, NullDisk data (2026-09-09).
#
# Reference results (all errors=0, incomplete=0, stop=complete):
#   workload    threads    success TPS    MiB/s     p50 us    p99 us
#   small             1         783.11       6.1      1,268      2,270
#   small             4       3,418.85      26.7      1,164      1,725
#   small             8       8,691.62      67.9        874      1,661
#   small            16      21,254.08     166.0        730      1,183
#   small            32      36,815.96     287.6        843      1,558
#   small           128      57,098.87     446.1      2,139      3,799
#   small           256      56,879.50     444.4      4,359      6,947
#   large             1          13.79     220.7     71,241     88,609
#   large             4          70.20   1,123.2     55,410     86,171
#   large            32         173.76   2,780.2    181,237    275,468
#   50/50 mix          1          24.30     194.9     40,443     94,923
#   50/50 mix          4         135.18   1,082.4     28,849     82,793
#   50/50 mix         32         352.08   2,818.1     36,473    236,431
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNKIO_READ_BENCH_CASES:-}"
DURATION="${CHUNKIO_READ_BENCH_DURATION:-20}"
TIMEOUT_SECS="${CHUNKIO_READ_BENCH_TIMEOUT:-180}"
DATASET_OBJECTS="${CHUNKIO_READ_DATASET_OBJECTS:-16}"
DISKIO_CONNECTIONS="${CHUNKIO_READ_DISKIO_CONNECTIONS:-8}"
DISKIO_RPC_WORKERS="${CHUNKIO_READ_DISKIO_RPC_WORKERS:-1}"
SERVER_RPC_WORKERS="${CHUNKIO_READ_SERVER_RPC_WORKERS:-}"
SKIP_BUILD="${CHUNKIO_READ_BENCH_SKIP_BUILD:-0}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKIO_READ_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkio-read-$RUN_STAMP}"
RESULTS_FILE="${CHUNKIO_READ_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
CURRENT_CONFIG="$REGRESSION_CONFIG"
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ && "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ \
    && "$DATASET_OBJECTS" =~ ^[1-9][0-9]*$ \
    && "$DISKIO_CONNECTIONS" =~ ^[1-9][0-9]*$ \
    && "$DISKIO_RPC_WORKERS" =~ ^[1-9][0-9]*$ ]] \
    || { [ -n "$SERVER_RPC_WORKERS" ] && ! [[ "$SERVER_RPC_WORKERS" =~ ^[1-9][0-9]*$ ]]; }; then
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
        --diskio-connections "$DISKIO_CONNECTIONS" --diskio-rpc-workers "$DISKIO_RPC_WORKERS" \
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
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$verb" "$concurrency" "$requested" "$reads" \
        "$small_reads" "$large_reads" "$errors" "$incomplete" "$stop" \
        "$(field "$line" reads_s)" "$(field "$line" logical_mib_s)" \
        "$(field "$line" avg_us)" "$(field "$line" p50_us)" "$(field "$line" p99_us)" \
        "$(field "$line" prepare_s)" >>"$RESULTS_FILE"
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

if [ "$SKIP_BUILD" != 1 ]; then
    echo "=== building release binaries ==="
    pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
    pixi run build-cpp
fi
mkdir -p "$LOG_ROOT"
regression_init
printf 'case\tverb\tconcurrency\trequested\treads\tsmall_reads\tlarge_reads\terrors\tincomplete\tstop\treads_s\tlogical_mib_s\tavg_us\tp50_us\tp99_us\tprepare_s\n' >"$RESULTS_FILE"

deploy_args=(cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync)
if [ -n "$SERVER_RPC_WORKERS" ]; then
    deploy_args+=(--diskio-rpc-workers "$SERVER_RPC_WORKERS")
fi
regression_cli "${deploy_args[@]}"
run_case read_small_1t read-small 1
run_case read_small_4t read-small 4
run_case read_small_8t read-small 8
run_case read_small_16t read-small 16
run_case read_small_32t read-small 32
run_case read_small_128t read-small 128
run_case read_small_256t read-small 256
run_case read_large_1t read-large 1
run_case read_large_4t read-large 4
run_case read_large_32t read-large 32
run_case read_mix_1t read-mix 1
run_case read_mix_4t read-mix 4
run_case read_mix_32t read-mix 32
destroy_cluster

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
