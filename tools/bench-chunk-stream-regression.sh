#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
#
# Production chunk-stream regression over the three-node local NullDisk stack.
#
# Optional environment variables:
#   CHUNK_STREAM_BENCH_CASES       space-separated case labels
#   CHUNK_STREAM_BENCH_DURATION    timed workload seconds (default: 10)
#   CHUNK_STREAM_BENCH_TIMEOUT     seconds allowed per case (default: 180)
#   CHUNK_STREAM_BENCH_LOG_ROOT    retained run root
#   CHUNK_STREAM_BENCH_RESULTS     result TSV path
#   CHUNK_STREAM_BENCH_SKIP_BUILD  set to 1 to reuse release artifacts
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
CASES="${CHUNK_STREAM_BENCH_CASES:-}"
DURATION_SECS="${CHUNK_STREAM_BENCH_DURATION:-10}"
TIMEOUT_SECS="${CHUNK_STREAM_BENCH_TIMEOUT:-180}"
SKIP_BUILD="${CHUNK_STREAM_BENCH_SKIP_BUILD:-0}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNK_STREAM_BENCH_LOG_ROOT:-${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/chunk-stream-regression-$RUN_STAMP}"
RESULTS_FILE="${CHUNK_STREAM_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
FAILURES=0
CASE_NUMBER=0

for value in "$DURATION_SECS" "$TIMEOUT_SECS"; do
    if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: benchmark duration and timeout must be positive integers" >&2
        exit 2
    fi
done
if [[ "$SKIP_BUILD" != 0 && "$SKIP_BUILD" != 1 ]]; then
    echo "ERROR: CHUNK_STREAM_BENCH_SKIP_BUILD must be 0 or 1" >&2
    exit 2
fi

destroy_cluster() {
    if [ -f "$REGRESSION_CONFIG" ]; then
        regression_destroy
    fi
}
trap destroy_cluster EXIT

field() {
    local line="$1" name="$2"
    awk -v name="$name" '{
        for (i = 1; i <= NF; i++) {
            split($i, pair, "=")
            if (pair[1] == name) {
                print pair[2]
                exit
            }
        }
    }' <<<"$line"
}

selected() {
    local label="$1"
    [ -z "$CASES" ] || [[ " $CASES " == *" $label "* ]]
}

record_result() {
    local label="$1" line="$2"
    printf '%s' "$label" >>"$RESULTS_FILE"
    local name
    for name in workload operations bytes errors seconds ops_s mib_s avg_us p50_us p99_us \
        rss_start_kib rss_end_kib rss_peak_kib max_queue_requests max_queue_bytes \
        batches batch_requests rollovers metadata_publications cache_hits cache_misses \
        physical_reads reclaimed_bytes watchdogs; do
        printf '\t%s' "$(field "$line" "$name")" >>"$RESULTS_FILE"
    done
    printf '\n' >>"$RESULTS_FILE"
}

validate_common() {
    local label="$1" status="$2" line="$3"
    local operations errors queue_requests queue_bytes watchdogs rss_start rss_peak
    operations=$(field "$line" operations)
    errors=$(field "$line" errors)
    queue_requests=$(field "$line" max_queue_requests)
    queue_bytes=$(field "$line" max_queue_bytes)
    watchdogs=$(field "$line" watchdogs)
    rss_start=$(field "$line" rss_start_kib)
    rss_peak=$(field "$line" rss_peak_kib)
    if [ "$status" -ne 0 ] || [ -z "$line" ] || [ -z "$operations" ] \
        || [ "$operations" -eq 0 ] || [ "$errors" != 0 ] || [ "$watchdogs" != 0 ] \
        || [ "$queue_requests" -gt 1024 ] || [ "$queue_bytes" -gt 67108864 ] \
        || [ $((rss_peak - rss_start)) -gt 262144 ]; then
        echo "ERROR: $label violated completion, queue, or watchdog bounds" >&2
        return 1
    fi
}

at_least() {
    local actual="$1" minimum="$2"
    awk -v actual="$actual" -v minimum="$minimum" 'BEGIN { exit !(actual >= minimum) }'
}

at_most() {
    local actual="$1" maximum="$2"
    awk -v actual="$actual" -v maximum="$maximum" 'BEGIN { exit !(actual <= maximum) }'
}

validate_case() {
    local label="$1" line="$2"
    local workload rollovers publications misses hits reads reclaimed
    workload=$(field "$line" workload)
    rollovers=$(field "$line" rollovers)
    publications=$(field "$line" metadata_publications)
    misses=$(field "$line" cache_misses)
    hits=$(field "$line" cache_hits)
    reads=$(field "$line" physical_reads)
    reclaimed=$(field "$line" reclaimed_bytes)
    case "$workload" in
        append)
            if [ "$publications" -gt $((rollovers + 1)) ]; then
                echo "ERROR: $label published metadata on the append hot path" >&2
                return 1
            fi
            if [[ "$label" == rollover_* && "$rollovers" -eq 0 ]]; then
                echo "ERROR: $label did not cross a chunk boundary" >&2
                return 1
            fi
            ;;
        random-read)
            if [ "$reads" -eq 0 ] || [ "$misses" -gt $((32 * (rollovers + 1))) ] || [ "$hits" -eq 0 ]; then
                echo "ERROR: $label exceeded extent lookup bounds" >&2
                return 1
            fi
            ;;
        replay)
            if [ "$reads" -eq 0 ] || [ "$misses" -gt $((rollovers + 1)) ]; then
                echo "ERROR: $label exceeded replay lookup bounds" >&2
                return 1
            fi
            ;;
        gc)
            if [ "$reclaimed" -eq 0 ]; then
                echo "ERROR: $label reclaimed no complete stream chunk" >&2
                return 1
            fi
            ;;
        *)
            echo "ERROR: $label returned unknown workload $workload" >&2
            return 1
            ;;
    esac
    local ops_s mib_s p99
    ops_s=$(field "$line" ops_s)
    mib_s=$(field "$line" mib_s)
    p99=$(field "$line" p99_us)
    case "$label" in
        append_4k_1t) at_least "$ops_s" 250 && at_most "$p99" 10000 ;;
        append_4k_32t) at_least "$ops_s" 3000 && at_most "$p99" 20000 ;;
        append_1m_8t) at_least "$mib_s" 125 && at_most "$p99" 150000 ;;
        rollover_4m) at_least "$mib_s" 200 && at_most "$p99" 500000 ;;
        random_4k_32t) at_least "$ops_s" 30000 && at_most "$p99" 5000 ;;
        replay_320m_1t) at_least "$mib_s" 300 && at_most "$p99" 2000000 ;;
        gc_576m) at_least "$mib_s" 1000 && at_most "$p99" 100000 ;;
    esac || {
        echo "ERROR: $label violated its production throughput or p99 bound" >&2
        return 1
    }
}

run_case() {
    local label="$1"
    shift
    if ! selected "$label"; then
        return
    fi
    if [ "$CASE_NUMBER" -gt 0 ]; then
        regression_reset_stack 1
    fi
    CASE_NUMBER=$((CASE_NUMBER + 1))
    echo ">>> $label"
    local output status line case_log="$LOG_ROOT/$label.log"
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$TIMEOUT_SECS" \
        pixi run -- cargo bench -p crowdb-chunk-stream --bench production_bounds -- \
        --duration-secs "$DURATION_SECS" "$@" 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output" | tee "$case_log"
    line=$(sed -n '/^chunk-stream:/p' <<<"$output" | tail -n 1)
    if [ -n "$line" ]; then
        record_result "$label" "$line"
    fi
    if ! validate_common "$label" "$status" "$line" || ! validate_case "$label" "$line"; then
        FAILURES=$((FAILURES + 1))
    fi
}

if [ "$SKIP_BUILD" -eq 0 ]; then
    echo "=== building production benchmark and local stack ==="
    pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
    pixi run build-cpp
    pixi run -- cargo bench -p crowdb-chunk-stream --bench production_bounds --no-run
fi

mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
regression_init
printf 'case\tworkload\toperations\tbytes\terrors\tseconds\tops_s\tmib_s\tavg_us\tp50_us\tp99_us\trss_start_kib\trss_end_kib\trss_peak_kib\tmax_queue_requests\tmax_queue_bytes\tbatches\tbatch_requests\trollovers\tmetadata_publications\tcache_hits\tcache_misses\tphysical_reads\treclaimed_bytes\twatchdogs\n' >"$RESULTS_FILE"

regression_cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
    --kv-backend mem-block --wal-backend mem-block --no-fsync

run_case append_4k_1t --workload append --object-size 4096 --concurrency 1
run_case append_4k_32t --workload append --object-size 4096 --concurrency 32
run_case append_1m_8t --workload append --object-size 1048576 --concurrency 8
run_case rollover_4m --workload append --object-size 4194304 --concurrency 8 --operations 65
run_case random_4k_32t --workload random-read --object-size 4096 --concurrency 32 \
    --dataset-bytes 335544320
run_case replay_320m_1t --workload replay --object-size 4096 --concurrency 1 \
    --dataset-bytes 335544320
run_case gc_576m --workload gc --object-size 4096 --concurrency 1 --dataset-bytes 603979776

destroy_cluster
trap - EXIT
echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
