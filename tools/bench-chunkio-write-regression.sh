#!/usr/bin/env bash
# CROWDB end-to-end large chunk-write regression using three NullDisk nodes.
#
# Optional environment variables:
#   CHUNKIO_BENCH_CASES       space-separated case labels
#   CHUNKIO_BENCH_LOG_ROOT    retained run root
#   CHUNKIO_BENCH_RESULTS     result TSV path
#   CHUNKIO_BENCH_TIMEOUT     seconds allowed per case (default: 120)
#   CHUNKIO_PREFETCH_CHUNKS   chunks warmed before timed load (default: 10)
#
# Reference run (2026-09-08): Intel Core i9-7960X, 4 memory channels,
# Linux 6.11, three-node loopback deployment, three NullDisk instances,
# EC 8+4, 1 MiB blocks, 1 GiB chunks, and 16 MiB objects.
# (AMD Ryzen 9 5950X runs use 2 memory channels; results differ.)
#
# Case        Obj    C  logical  physical  p50 us  p99 us  dram_read  dram_write  dram_total  errors
# stream_1t   204    1   162.5    243.8     99191  111225    2496.6     1393.0      3889.6       0
# direct_1t   288    1   229.1    343.7     69191   85641    1996.1     1228.7      3224.8       0
# stream_4t  2467    4  1965.6   2948.5     31917   51806   12699.7    10943.0     23642.7       0
# direct_4t  3352    4  2672.1   4008.1     22811   40000   11799.6    12051.5     23851.1       0
# stream_32t 4089   32  3249.3   4873.9    151716  267992   19562.5    15603.7     35166.2       0
# direct_32t 4495   32  3547.9   5321.9    138654  261388   14816.5    12766.6     27583.1       0
#
# Host memory-counter samples are retained as diagnostic data, not hard
# thresholds. The sentinel gates accounting, errors, stop reason, and metrics.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNKIO_BENCH_CASES:-}"
TIMEOUT_SECS="${CHUNKIO_BENCH_TIMEOUT:-120}"
PREFETCH_CHUNKS="${CHUNKIO_PREFETCH_CHUNKS:-10}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKIO_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkio-write-regression-$RUN_STAMP}"
RESULTS_FILE="${CHUNKIO_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
CURRENT_CONFIG="$REGRESSION_CONFIG"
CURRENT_LOG_ROOT="$LOG_ROOT"
BENCH_LOG_DIR=""
FAILURES=0
CASE_NUMBER=0

if ! [[ "$TIMEOUT_SECS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: CHUNKIO_BENCH_TIMEOUT must be a positive integer" >&2
    exit 2
fi

cli() {
    regression_cli "$@"
}

destroy_cluster() {
    if [ -n "$CURRENT_CONFIG" ] && [ -f "$CURRENT_CONFIG" ]; then
        regression_destroy
    fi
    CURRENT_CONFIG=""
}
trap destroy_cluster EXIT

field() {
    local line="$1" name="$2"
    sed -n "s/.*${name}=\([^ ]*\).*/\1/p" <<<"$line"
}

memory_bandwidth() {
    local line="$1" field="$2"
    local value
    value=$(sed -n "s/.*${field}=\([^ ]*\).*/\1/p" <<<"$line")
    if [[ -z "$value" || "$value" == "unsupported" ]]; then
        echo "unsupported"
    else
        printf '%s' "$value"
    fi
}

verify_logs() {
    local kv diskdb chunkdb diskio cli_metrics cli_metrics_file
    kv=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-kv-server-metrics-*.log' | wc -l)
    diskdb=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-diskdb-metrics-*.log' | wc -l)
    chunkdb=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-chunkdb-metrics-*.log' | wc -l)
    diskio=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-diskio-metrics-*.log' | wc -l)
    cli_metrics=$(find "$CURRENT_LOG_ROOT" -type f -name 'crowdb-cli-metrics-*.log' | wc -l)
    cli_metrics_file=$(find "$BENCH_LOG_DIR" -type f -name 'crowdb-cli-metrics-*.log' | head -n 1)
    [ "$kv" -eq 3 ] && [ "$diskdb" -ge $((CASE_NUMBER * 3)) ] \
        && [ "$chunkdb" -ge $((CASE_NUMBER * 3)) ] \
        && [ "$diskio" -ge $((CASE_NUMBER * 3)) ] && [ "$cli_metrics" -eq "$CASE_NUMBER" ] \
        && grep -Eq -- 'chunkio\.object\.write\.e2e\.lh' "$cli_metrics_file" \
        && grep -Eq -- 'chunkio\.chunk\.allocate\.e2e\.lh' "$cli_metrics_file" \
        && grep -Eq -- 'chunkio\.diskio\.write\.e2e\.lh' "$cli_metrics_file" \
        && regression_require_metric_files 'crowdb-kv-server-metrics-*.log' rust cpp-rpc cpp-tree \
        && regression_require_metric_files 'crowdb-diskdb-metrics-*.log' rust cpp-rpc \
        && regression_require_metric_files 'crowdb-chunkdb-metrics-*.log' rust cpp-rpc \
        && regression_require_metric_files 'crowdb-diskio-metrics-*.log' cpp-rpc \
        && ! grep -ERq -- 'fd .*not registered|DiskNotExist' "$CURRENT_LOG_ROOT"/cli-cluster-local-deploy-*/deploy/*/*/*/log
}

run_case() {
    local label="$1" object_size="$2" concurrency="$3" input_mode="$4"
    local objects=1000000 duration_secs=20
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    CURRENT_CONFIG="$REGRESSION_CONFIG"
    echo ">>> $label (duration=${duration_secs}s size=$object_size concurrency=$concurrency EC=8+4)"
    if [ "$CASE_NUMBER" -gt 0 ]; then
        regression_reset_stack 1
    fi
    CASE_NUMBER=$((CASE_NUMBER + 1))

    local output status line read_avg read_max write_avg write_max total_avg total_max
    local input_args=()
    if [ "$input_mode" = direct ]; then
        input_args+=(--direct-buffers)
    fi
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$TIMEOUT_SECS" \
        pixi run -- ./target/release/crowdb-cli --log-root "$CURRENT_LOG_ROOT" --config "$CURRENT_CONFIG" \
        bench chunkio write --objects "$objects" --duration-secs "$duration_secs" \
        --object-size "$object_size" \
        --concurrency "$concurrency" --diskio-connections 8 --data-num 8 --code-num 4 \
        --block-size 1048576 --chunk-size 1073741824 --seed 1 \
        --prefetch-chunks "$PREFETCH_CHUNKS" --prefetch-strips-per-chunk 2 \
        "${input_args[@]}" \
        --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    BENCH_LOG_DIR=$(sed -n 's/^log dir: //p' <<<"$output" | tail -n 1)
    line=$(sed -n '/^chunkio write:/p' <<<"$output" | tail -n 1)
    if [ -n "$line" ]; then
        read_avg=$(memory_bandwidth "$line" dram_read_mib_s)
        read_max="$read_avg"
        write_avg=$(memory_bandwidth "$line" dram_write_mib_s)
        write_max="$write_avg"
        total_avg=$(memory_bandwidth "$line" dram_total_mib_s)
        total_max="$total_avg"
    else
        read_avg=unsupported
        read_max=unsupported
        write_avg=unsupported
        write_max=unsupported
        total_avg=unsupported
        total_max=unsupported
    fi
    if [ -z "$line" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t0\t1\t%s\tfailed\t0\t0\t0\t0\t0\t0\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" 0 "$object_size" "$((object_size / 1048576))" \
            "$concurrency" "$objects" "$read_avg" "$read_max" "$write_avg" "$write_max" \
            "$total_avg" "$total_max" >>"$RESULTS_FILE"
    else
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$(field "$line" requested)" "$object_size" "$((object_size / 1048576))" "$concurrency" \
            "$(field "$line" objects)" "$(field "$line" errors)" \
            "$(field "$line" incomplete)" "$(field "$line" stop)" \
            "$(field "$line" objects_s)" "$(field "$line" logical_mib_s)" \
            "$(field "$line" physical_mib_s)" \
            "$(field "$line" avg_us)" "$(field "$line" p50_us)" "$(field "$line" p99_us)" \
            "$read_avg" "$read_max" "$write_avg" "$write_max" \
            "$total_avg" "$total_max" >>"$RESULTS_FILE"
    fi
    local requested completed errors incomplete stop objects_s logical physical p50 p99 valid=1
    requested=$(field "$line" requested)
    completed=$(field "$line" objects)
    errors=$(field "$line" errors)
    incomplete=$(field "$line" incomplete)
    stop=$(field "$line" stop)
    objects_s=$(field "$line" objects_s)
    logical=$(field "$line" logical_mib_s)
    physical=$(field "$line" physical_mib_s)
    p50=$(field "$line" p50_us)
    p99=$(field "$line" p99_us)
    if [ -z "$line" ] || [ "$completed" != "$requested" ] || [ "$errors" != 0 ] \
        || [ "$incomplete" != 0 ] || [ "$stop" != complete ] \
        || [ -z "$objects_s" ] || [ -z "$logical" ] || [ -z "$physical" ] \
        || [ -z "$p50" ] || [ -z "$p99" ]; then
        valid=0
    fi
    if [ "$status" -ne 0 ] || [ "$valid" -ne 1 ] || ! verify_logs; then
        echo "ERROR: $label failed or did not retain all service metrics" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
pixi run build-cpp
mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
regression_init
printf 'case\trequested\tsize_bytes\tsize_mib\tconcurrency\tcompleted\terrors\tincomplete\tstop\tobjects_s\tlogical_mib_s\tphysical_mib_s\tavg_us\tp50_us\tp99_us\tmem_read_avg_mib\tmem_read_max_mib\tmem_write_avg_mib\tmem_write_max_mib\tmem_total_avg_mib\tmem_total_max_mib\n' >"$RESULTS_FILE"

cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync

run_case stream_1t 16777216 1 stream
run_case direct_1t 16777216 1 direct
run_case stream_4t 16777216 4 stream
run_case direct_4t 16777216 4 direct
run_case stream_32t 16777216 32 stream
run_case direct_32t 16777216 32 direct
destroy_cluster

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
