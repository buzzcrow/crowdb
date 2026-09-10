#!/usr/bin/env bash
# CROWDB ChunkDB EC allocation regression.
#
# Three co-located logical nodes, each running KV, DiskDB, and ChunkDB.
# The fixture has one rack, three KV data groups, and four 4-TiB logical
# disks per DiskDB. Each operation allocates one EC 8+4 strip. DiskDB requests
# are batched per data group within that strip; strips are not batched together.
#
# Optional environment variables:
#   CHUNKDB_BENCH_DURATION       seconds per case (default: 20)
#   CHUNKDB_BENCH_CASES          space-separated case labels
#   CHUNKDB_BENCH_CONNECTIONS    override all client/service connection counts
#   CHUNKDB_BENCH_RPC_WORKERS    override all RPC worker counts
#   CHUNKDB_BENCH_KV_INFLIGHT    KV proposal window (default: 32)
#   CHUNKDB_BENCH_KV_COALESCE    KV coalescing width (default: 32)
#   CHUNKDB_BENCH_DISK_CAPACITY  bytes per disk (default: 4 TiB)
#   CHUNKDB_BENCH_ZONE_SIZE      bytes per zone (default: 256 GiB)
#   CHUNKDB_BENCH_LOG_ROOT       retained run root
#   CHUNKDB_BENCH_RESULTS        result TSV path
#
# Wl       Grp Thr Strip EC  Cli Cdb Ddb Kv Wkr Win Coal chunk/s block/s p50   p99    Dur Err Stop     Spc
# allocate  3   1   1     8+4 2   2   2   2  2   32  32   799     9588    1275  1561  20s 0   deadline exact
# allocate  3   16  1     8+4 2   2   2   2  2   32  32   8800    105600  1719  3481  20s 0   deadline exact
# allocate  3   128 1     8+4 4   4   4   4  4   32  32   11914   142968  9945  22922 20s 0   deadline exact
# allocate  3   256 1     8+4 4   4   4   4  4   32  32   12685   152220  18427 49050 20s 0   deadline exact
# allocate  3   512 1     8+4 4   4   4   4  4   32  32   12734   152808  37131 91641 20s 0   deadline exact
# Clean artifacts: chunkdb-r98-final2-20260905-223329 (all rows).
#
# Intel i9-7960X (2026-09-10, 16c/32t, Linux 6.11, x86_64):
#   Same build/config as AMD 2026-09-05. EC 8+4, 1 strip/request, 20s.
#   1T 65% slower (per-op overhead higher on Intel). 16T 12% slower,
#   256T equal. 128T and 512T consistently fail with diskdb accounting
#   mismatch (expected ~2/3 of expected busy bytes — one node's disk
#   usage not fully reported). Throughput is comparable (11900-13186
#   chunk/s) but space accounting breaks at high concurrency on Intel.
#   This is a pre-existing correctness issue, not a perf regression —
#   documented in doc/working/regression-perf-review.md.
#
# Wl       Grp Thr Strip EC  Cli Cdb Ddb Kv Wkr Win Coal chunk/s block/s avg    p50    p99    Dur Err Stop      Spc
# allocate  3   1   1     8+4 2   2   2   2  2   32  32   283     3396    3528   3562   4648   20s 0   deadline exact
# allocate  3   16  1     8+4 2   2   2   2  2   32  32   7706    92472   2074   2003   3574   20s 0   deadline exact
# allocate  3   128 1     8+4 4   4   4   4  4   32  32   11900   142800  10750  10168  21095  20s 1   deadline mismatch
# allocate  3   256 1     8+4 4   4   4   4  4   32  32   12657   151884  20211  19456  37471  20s 0   deadline exact
# allocate  3   512 1     8+4 4   4   4   4  4   32  32   13149   157788  38893  36401  84413  20s 1   deadline mismatch
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
DURATION="${CHUNKDB_BENCH_DURATION:-20}"
CASES="${CHUNKDB_BENCH_CASES:-}"
CONNECTIONS_OVERRIDE="${CHUNKDB_BENCH_CONNECTIONS:-}"
RPC_WORKERS_OVERRIDE="${CHUNKDB_BENCH_RPC_WORKERS:-}"
KV_INFLIGHT="${CHUNKDB_BENCH_KV_INFLIGHT:-32}"
KV_COALESCE="${CHUNKDB_BENCH_KV_COALESCE:-32}"
DISK_CAPACITY="${CHUNKDB_BENCH_DISK_CAPACITY:-4398046511104}"
ZONE_SIZE="${CHUNKDB_BENCH_ZONE_SIZE:-274877906944}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNKDB_BENCH_LOG_ROOT:-$(pwd)/bench-log/chunkdb-regression-$RUN_STAMP}"
RESULTS_FILE="${CHUNKDB_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh
CURRENT_CONFIG="$REGRESSION_CONFIG"
FAILURES=0
CASE_NUMBER=0
DEPLOY_NUMBER=0
CASE_IN_GROUP=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]] || ! [[ "$DISK_CAPACITY" =~ ^[1-9][0-9]*$ ]] \
    || ! [[ "$ZONE_SIZE" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: duration, disk capacity, and zone size must be positive integers" >&2
    exit 2
fi

cli() {
    regression_cli "$@"
}

destroy_cluster() {
    if [ -n "$CURRENT_CONFIG" ] && [ -f "$CURRENT_CONFIG" ]; then
        cli cluster destroy || true
    fi
    CURRENT_CONFIG=""
}
trap destroy_cluster EXIT

field() {
    local line="$1" name="$2"
    sed -n "s/.*\(^\| \)${name}=\([^ ]*\).*/\2/p" <<<"$line"
}

any_case_selected() {
    [ -z "$CASES" ] && return 0
    local label
    for label in "$@"; do
        [[ " $CASES " == *" $label "* ]] && return 0
    done
    return 1
}

verify_logs() {
    local label="$1" kv_metrics diskdb_metrics chunkdb_metrics cli_metrics
    local kv_rpc diskdb_rpc chunkdb_rpc cli_rpc expected_servers expected_clients
    kv_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/kv-server-*/log/crowdb-kv-server-metrics-*.log' -type f | wc -l)
    diskdb_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/diskdb-*/log/crowdb-diskdb-metrics-*.log' -type f | wc -l)
    chunkdb_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/chunkdb-*/log/crowdb-chunkdb-metrics-*.log' -type f | wc -l)
    cli_metrics=$(find "$LOG_ROOT" -path '*/cli-bench-chunkdb-*/crowdb-cli-metrics-*.log' -type f | wc -l)
    kv_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/kv-server-*/log/crowdb-kv-server-rpc-*.log' -type f | wc -l)
    diskdb_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/diskdb-*/log/crowdb-diskdb-rpc-*.log' -type f | wc -l)
    chunkdb_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/chunkdb-*/log/crowdb-chunkdb-rpc-*.log' -type f | wc -l)
    cli_rpc=$(find "$LOG_ROOT" -path '*/cli-bench-chunkdb-*/crowdb-cli-rpc-*.log' -type f | wc -l)
    expected_servers=$((CASE_NUMBER * 3))
    expected_clients="$CASE_NUMBER"
    if [ "$kv_metrics" -ne $((DEPLOY_NUMBER * 3)) ] || [ "$diskdb_metrics" -lt "$expected_servers" ] \
        || [ "$chunkdb_metrics" -lt "$expected_servers" ] || [ "$cli_metrics" -ne "$expected_clients" ] \
        || [ "$kv_rpc" -ne $((DEPLOY_NUMBER * 3)) ] || [ "$diskdb_rpc" -lt "$expected_servers" ] \
        || [ "$chunkdb_rpc" -lt "$expected_servers" ] || [ "$cli_rpc" -ne "$expected_clients" ]; then
        echo "ERROR: incomplete logs for $label (kv=$kv_metrics/$kv_rpc diskdb=$diskdb_metrics/$diskdb_rpc chunkdb=$chunkdb_metrics/$chunkdb_rpc cli=$cli_metrics/$cli_rpc)" >&2
        return 1
    fi
    regression_require_metric_files 'crowdb-kv-server-metrics-*.log' rust cpp-rpc cpp-tree || return 1
    regression_require_metric_files 'crowdb-diskdb-metrics-*.log' rust cpp-rpc || return 1
    regression_require_metric_files 'crowdb-chunkdb-metrics-*.log' rust cpp-rpc || return 1
    echo "    logs: kv=$kv_metrics/$kv_rpc diskdb=$diskdb_metrics/$diskdb_rpc chunkdb=$chunkdb_metrics/$chunkdb_rpc cli=$cli_metrics/$cli_rpc"
}

deploy_group() {
    local connections="$1" workers="$2"
    CURRENT_CONFIG="$REGRESSION_CONFIG"
    CASE_IN_GROUP=0
    DEPLOY_NUMBER=$((DEPLOY_NUMBER + 1))
    cli cluster local-deploy -t combined \
        --kv-backend mem-block --wal-backend mem-block --metrics-interval 1 \
        --event-write --peer-pool-size "$connections" --rpc-workers "$workers" \
        --max-inflight "$KV_INFLIGHT" --coalesce-max-keys "$KV_COALESCE" \
        --data-groups 1,2,3 --disk-groups-per-node 1 --disks-per-group 4 \
        --disk-capacity-bytes "$DISK_CAPACITY" --disk-zone-size-bytes "$ZONE_SIZE" \
        --disk-unit-size-bytes 1048576 --kv-connections "$connections" \
        --kv-client-rpc-workers "$workers" --diskdb-connections "$connections" \
        --diskdb-client-rpc-workers "$workers" --chunkdb-instances 3
}

run_case() {
    local concurrency="$1" label="$2" profile_connections="$3" profile_workers="$4"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    CURRENT_CONFIG="$REGRESSION_CONFIG"
    local connections="${CONNECTIONS_OVERRIDE:-$profile_connections}"
    local workers="${RPC_WORKERS_OVERRIDE:-$profile_workers}"
    CASE_NUMBER=$((CASE_NUMBER + 1))
    echo ">>> $label (EC 8+4, concurrency=$concurrency)"
    if [ "$CASE_IN_GROUP" -gt 0 ]; then
        regression_reset_stack 1 2 3
    fi
    CASE_IN_GROUP=$((CASE_IN_GROUP + 1))

    local output status line space busy expected
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$((DURATION + 40))" \
        pixi run -- ./target/release/crowdb-cli --log-root "$LOG_ROOT" --config "$CURRENT_CONFIG" \
        bench chunkdb allocate --duration-secs "$DURATION" --concurrency "$concurrency" \
        --chunkdb-connections "$connections" --chunkdb-client-rpc-workers "$workers" \
        --strip-count 1 --strip-type ec --data-num 8 --code-num 4 \
        --write-granularity-kb 1024 --seed 1 --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n '/^chunkdb bench /p' <<<"$output" | tail -n 1)
    if [ -z "$line" ]; then
        printf 'allocate\t3\t%s\t1\t8+4\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t0\t0\t0\t0\t%ss\t1\tunknown\tunknown\n' \
            "$concurrency" "$connections" "$connections" "$connections" "$connections" \
            "$workers" "$KV_INFLIGHT" "$KV_COALESCE" "$DURATION" >>"$RESULTS_FILE"
    else
        busy=$(field "$line" busy_delta)
        expected=$(field "$line" expected_busy_delta)
        space=mismatch
        if [ -n "$busy" ] && [ "$busy" = "$expected" ]; then
            space=exact
        fi
        printf 'allocate\t3\t%s\t1\t8+4\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%ss\t%s\t%s\t%s\n' \
            "$concurrency" "$connections" "$connections" "$connections" "$connections" \
            "$workers" "$KV_INFLIGHT" "$KV_COALESCE" "$(field "$line" ops_per_sec)" \
            "$(field "$line" block_allocs_per_sec)" "$(field "$line" avg_us)" \
            "$(field "$line" p50_us)" "$(field "$line" p99_us)" "$DURATION" \
            "$(field "$line" errors)" "$(field "$line" stop)" "$space" >>"$RESULTS_FILE"
    fi
    if ! verify_logs "$label"; then
        FAILURES=$((FAILURES + 1))
    fi
    if [ "$status" -ne 0 ]; then
        echo "ERROR: benchmark failed for $label (exit=$status)" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
regression_init
printf 'Wl\tGrp\tThr\tStrip\tEC\tCli\tCdb\tDdb\tKv\tWkr\tWin\tCoal\tchunk/s\tblock/s\tavg\tp50\tp99\tDur\tErr\tStop\tSpc\n' >"$RESULTS_FILE"

if any_case_selected allocate_ec8_4_1t allocate_ec8_4_16t; then
    deploy_group "${CONNECTIONS_OVERRIDE:-2}" "${RPC_WORKERS_OVERRIDE:-2}"
    run_case 1 allocate_ec8_4_1t 2 2
    run_case 16 allocate_ec8_4_16t 2 2
    destroy_cluster
fi
if any_case_selected allocate_ec8_4_128t allocate_ec8_4_256t allocate_ec8_4_512t; then
    deploy_group "${CONNECTIONS_OVERRIDE:-4}" "${RPC_WORKERS_OVERRIDE:-4}"
    run_case 128 allocate_ec8_4_128t 4 4
    run_case 256 allocate_ec8_4_256t 4 4
    run_case 512 allocate_ec8_4_512t 4 4
    destroy_cluster
fi

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
