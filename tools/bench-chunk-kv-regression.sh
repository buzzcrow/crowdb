#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
#
# Real-process routed chunk-KV regression over the local three-node stack.
set -euo pipefail
cd "$(dirname "$0")/.."

OPERATIONS="${CHUNK_KV_BENCH_OPERATIONS:-30000}"
CONCURRENCY="${CHUNK_KV_BENCH_CONCURRENCY:-32}"
VALUE_BYTES="${CHUNK_KV_BENCH_VALUE_BYTES:-512}"
READ_PERCENT="${CHUNK_KV_BENCH_READ_PERCENT:-25}"
HOT_KEY_PREFIX="${CHUNK_KV_BENCH_HOT_KEY_PREFIX:-object/hot}"
TARGET_PARTITION_BYTES="${CHUNK_KV_BENCH_TARGET_PARTITION_BYTES:-5242880}"
TIMEOUT_SECS="${CHUNK_KV_BENCH_TIMEOUT:-60}"
LOAD_TIMEOUT_SECS="${CHUNK_KV_BENCH_LOAD_TIMEOUT:-$TIMEOUT_SECS}"
READY_TIMEOUT_SECS="${CHUNK_KV_BENCH_READY_TIMEOUT:-$TIMEOUT_SECS}"
SKIP_BUILD="${CHUNK_KV_BENCH_SKIP_BUILD:-0}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${CHUNK_KV_BENCH_LOG_ROOT:-${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/chunk-kv-regression-$RUN_STAMP}"
RESULTS_FILE="${CHUNK_KV_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/bench-regression-common.sh

CHUNK_KV_PIDS=()

stop_auxiliary_processes() {
    local pattern="$1" pid
    local pids=()
    mapfile -t pids < <(pgrep -f "$pattern" || true)
    for pid in "${pids[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 0.1
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
}

cleanup() {
    local pid
    for pid in "${CHUNK_KV_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    regression_destroy || true
    stop_auxiliary_processes "^$LOG_ROOT/cli-cluster-local-deploy-.*/deploy/.*bin/crowdb-(chunkdb|diskdb|diskio)"
}
trap cleanup EXIT

for value in "$OPERATIONS" "$CONCURRENCY" "$VALUE_BYTES" "$TIMEOUT_SECS" "$LOAD_TIMEOUT_SECS" \
    "$READY_TIMEOUT_SECS" \
    "$TARGET_PARTITION_BYTES"; do
    if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: benchmark bounds must be positive integers" >&2
        exit 2
    fi
done
if ! [[ "$READ_PERCENT" =~ ^[0-9]+$ ]] || [ "$READ_PERCENT" -gt 100 ]; then
    echo "ERROR: read percent must be an integer from 0 through 100" >&2
    exit 2
fi
if [ "$SKIP_BUILD" -eq 0 ]; then
    pixi run build-cpp
    pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server \
        -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server \
        -p crowdb-chunk-kv-client
fi

mkdir -p "$LOG_ROOT"
regression_init
stop_auxiliary_processes "^${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/chunk-kv-regression-.*/cli-cluster-local-deploy-.*/deploy/.*bin/crowdb-(chunkdb|diskdb|diskio)"
DEPLOYED=0
for attempt in 1 2 3; do
    REGRESSION_CONFIG="$LOG_ROOT/console-attempt-$attempt.toml"
    if regression_cli cluster local-deploy -t combined --metrics-interval 1 --allow-unsafe-ec \
        --kv-backend mem-block --wal-backend mem-block --no-fsync \
        --diskio-dummy-disk-type mem; then
        DEPLOYED=1
        break
    fi
    regression_destroy || true
done
if [[ "$DEPLOYED" -ne 1 ]]; then
    echo "chunk KV local deployment failed after 3 attempts" >&2
    exit 1
fi

MGMT_SEED=$(sed -n 's/^[[:space:]]*url = "\([^"]*\)"/\1/p' "$REGRESSION_CONFIG" | head -n 1)
if [ -z "$MGMT_SEED" ]; then
    echo "ERROR: local deployment did not publish a KV management endpoint" >&2
    exit 1
fi

write_config() {
    local instance="$1" rpc_port="$2" http_port="$3" config="$4"
    {
        echo "instance_id = $instance"
        echo "rpc_listen_addr = \"127.0.0.1:$rpc_port\""
        echo "rpc_advertise_addr = \"127.0.0.1:$rpc_port\""
        echo "http_listen_addr = \"127.0.0.1:$http_port\""
        echo "group0_mgmt_seeds = [\"$MGMT_SEED\"]"
        echo "catalog_refresh_interval_ms = 500"
        echo
        echo "[storage]"
        echo "metadata_store_id = 0"
        echo "stream_writer_lease_ms = 5000"
        echo "diskio_connections_per_endpoint = 1"
        echo "diskio_rpc_workers = 2"
        echo
        echo "[balance]"
        echo "enabled = true"
        echo "target_partitions_per_owner = 4"
        echo "target_partition_bytes = $TARGET_PARTITION_BYTES"
        echo "minimum_weighted_improvement_percent = 25"
        echo "cooldown_ms = 1000"
        echo "max_owner_request_rate = 0"
        if [ "$instance" -eq 1 ]; then
            echo
            echo "[bootstrap_partition]"
            echo "partition_id = { high = 1, low = 1 }"
            echo "tree_id = 1"
            echo "stream_name = { high = 1, low = 1 }"
            echo "owner_epoch = 1"
            echo "metadata_group_id = 1"
        fi
    } >"$config"
}

start_server() {
    local instance="$1" config="$LOG_ROOT/chunk-kv-$instance.toml"
    write_config "$instance" "$((15200 + instance))" "$((15100 + instance))" "$config"
    pixi run -- ./target/release/crowdb-chunk-kv-server --config "$config" \
        --log-dir "$LOG_ROOT/chunk-kv-$instance-log" \
        >"$LOG_ROOT/chunk-kv-$instance.console.log" 2>&1 &
    SERVER_PID=$!
    CHUNK_KV_PIDS+=("$SERVER_PID")
}

wait_ready() {
    local instance="$1" pid="$2" deadline=$((SECONDS + READY_TIMEOUT_SECS))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if curl --silent --fail "http://127.0.0.1:$((15100 + instance))/ready" \
            >"$LOG_ROOT/chunk-kv-$instance.ready.json"; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null || [ "$(awk '{ print $3 }' "/proc/$pid/stat" 2>/dev/null || true)" = Z ]; then
            wait "$pid" 2>/dev/null || true
            echo "chunk-KV server $instance exited before becoming ready" >&2
            return 1
        fi
        sleep 1
    done
    echo "ERROR: chunk-KV server $instance did not become ready" >&2
    return 1
}

for instance in 1 2 3; do
    start_server "$instance"
done
for instance in 1 2 3; do
    wait_ready "$instance" "${CHUNK_KV_PIDS[$((instance - 1))]}"
done

rss_kib() {
    awk '/VmRSS:/ { print $2; exit }' "/proc/$1/status"
}

metric_value() {
    local metrics="$1" field="$2"
    sed -n "s/.*\"$field\":\([0-9][0-9]*\).*/\1/p" <<<"$metrics"
}

collect_metrics() {
    local instance metrics value
    ADMISSION_BACKPRESSURE=0
    RECOVERIES=0
    SPLIT_FINALIZATIONS=0
    SPLIT_CATCHUP_LAG_RECORDS=0
    SPLIT_FINALIZATION_DURATION_US=0
    for instance in 1 2 3; do
        if ! metrics=$(curl --silent --fail "http://127.0.0.1:$((15100 + instance))/metrics"); then
            continue
        fi
        value=$(metric_value "$metrics" admission_backpressure)
        ADMISSION_BACKPRESSURE=$((ADMISSION_BACKPRESSURE + ${value:-0}))
        value=$(metric_value "$metrics" recoveries)
        RECOVERIES=$((RECOVERIES + ${value:-0}))
        value=$(metric_value "$metrics" split_finalizations)
        SPLIT_FINALIZATIONS=$((SPLIT_FINALIZATIONS + ${value:-0}))
        value=$(metric_value "$metrics" split_catchup_lag_records)
        if [ "${value:-0}" -gt "$SPLIT_CATCHUP_LAG_RECORDS" ]; then
            SPLIT_CATCHUP_LAG_RECORDS=$value
        fi
        value=$(metric_value "$metrics" split_finalization_duration_us)
        if [ "${value:-0}" -gt "$SPLIT_FINALIZATION_DURATION_US" ]; then
            SPLIT_FINALIZATION_DURATION_US=$value
        fi
    done
}

capture_failure_metrics() {
    local instance metrics
    collect_metrics
    {
        printf '{"admission_backpressure":%s,"recoveries":%s,"split_finalizations":%s,"split_catchup_lag_records":%s,"split_finalization_duration_us":%s}\n' \
            "$ADMISSION_BACKPRESSURE" "$RECOVERIES" "$SPLIT_FINALIZATIONS" \
            "$SPLIT_CATCHUP_LAG_RECORDS" "$SPLIT_FINALIZATION_DURATION_US"
        for instance in 1 2 3; do
            if metrics=$(curl --silent --fail "http://127.0.0.1:$((15100 + instance))/metrics"); then
                printf 'server_%s %s\n' "$instance" "$metrics"
            fi
        done
    } >"$LOG_ROOT/load-failure-metrics.log"
}

RSS_START=0
for pid in "${CHUNK_KV_PIDS[@]}"; do
    RSS_START=$((RSS_START + $(rss_kib "$pid")))
done

if ! LOAD_OUTPUT=$(timeout "$LOAD_TIMEOUT_SECS" pixi run -- ./target/release/crowdb-chunk-kv-cli \
    --mgmt-seed "$MGMT_SEED" load --operations "$OPERATIONS" \
    --concurrency "$CONCURRENCY" --value-bytes "$VALUE_BYTES" --keyspace "$OPERATIONS" \
    --key-prefix "$HOT_KEY_PREFIX" --key-offset 0 --read-percent "$READ_PERCENT"); then
    printf '%s\n' "$LOAD_OUTPUT" >"$LOG_ROOT/load-continuous.log"
    capture_failure_metrics
    echo "ERROR: continuous routed load failed; retained metrics: $LOG_ROOT/load-failure-metrics.log" >&2
    exit 1
fi
printf '%s\n' "$LOAD_OUTPUT" | tee "$LOG_ROOT/load-continuous.log"
LOAD_LINE=$(sed -n '/^chunk-kv:/p' <<<"$LOAD_OUTPUT" | tail -n 1)
if [ -z "$LOAD_LINE" ] || ! grep -q 'errors=0' <<<"$LOAD_LINE"; then
    echo "ERROR: continuous routed load did not complete without errors" >&2
    exit 1
fi
P99=$(sed -n 's/.* p99_us=\([0-9][0-9]*\).*/\1/p' <<<"$LOAD_LINE")

PARTITIONS=0
CATALOG_GENERATION=0
OWNER_MIN_PARTITIONS=0
OWNER_MAX_PARTITIONS=0
OWNERS_WITH_PARTITIONS=0
SPLIT_STARTED_MS=$(date +%s%3N)
deadline=$((SECONDS + TIMEOUT_SECS))
while [ "$SECONDS" -lt "$deadline" ]; do
    if PROBE=$(pixi run -- ./target/release/crowdb-chunk-kv-cli --mgmt-seed "$MGMT_SEED" \
        load --operations 1 --concurrency 1 --value-bytes 1 --keyspace 1 2>&1); then
        # The point probe deliberately retains its original g1 routing to
        # verify old-client compatibility.  Count the current local catalog
        # from the owner instead of interpreting that compatible client view
        # as the authoritative partition count.
        :
    else
        printf '%s\n' "$PROBE" >"$LOG_ROOT/last-split-probe-error.log"
    fi
    # Refresh independently of the compatibility probe: a g1 client is
    # intentionally permitted to keep routing through its old dispatcher
    # while the owner has already installed g2.
    PARTITIONS=0
    OWNER_MIN_PARTITIONS=2147483647
    OWNER_MAX_PARTITIONS=0
    OWNERS_WITH_PARTITIONS=0
    for instance in 1 2 3; do
        if ! HEALTH=$(curl --silent --fail "http://127.0.0.1:$((15100 + instance))/health"); then
            continue
        fi
        OWNER_PARTITIONS=$(sed -n 's/.*"serving_partitions":\([0-9][0-9]*\).*/\1/p' <<<"$HEALTH")
        OWNER_GENERATION=$(sed -n 's/.*"catalog_generation":\([0-9][0-9]*\).*/\1/p' <<<"$HEALTH")
        PARTITIONS=$((PARTITIONS + ${OWNER_PARTITIONS:-0}))
        if [ "${OWNER_PARTITIONS:-0}" -lt "$OWNER_MIN_PARTITIONS" ]; then
            OWNER_MIN_PARTITIONS=${OWNER_PARTITIONS:-0}
        fi
        if [ "${OWNER_PARTITIONS:-0}" -gt "$OWNER_MAX_PARTITIONS" ]; then
            OWNER_MAX_PARTITIONS=${OWNER_PARTITIONS:-0}
        fi
        if [ "${OWNER_PARTITIONS:-0}" -gt 0 ]; then
            OWNERS_WITH_PARTITIONS=$((OWNERS_WITH_PARTITIONS + 1))
        fi
        if [ "${OWNER_GENERATION:-0}" -gt "$CATALOG_GENERATION" ]; then
            CATALOG_GENERATION=${OWNER_GENERATION:-0}
        fi
    done
    if [ "$OWNERS_WITH_PARTITIONS" -ge 2 ] && [ "$CATALOG_GENERATION" -ge 2 ]; then
        break
    fi
    sleep 1
done
if [ "${CATALOG_GENERATION:-0}" -lt 2 ]; then
    capture_failure_metrics
    echo "ERROR: automatic split did not publish a child within the bounded observation window" >&2
    exit 1
fi
SPLIT_PREPARE_MS=$(($(date +%s%3N) - SPLIT_STARTED_MS))
collect_metrics
if [ "$SPLIT_FINALIZATIONS" -lt 1 ]; then
    capture_failure_metrics
    echo "ERROR: automatic split did not retain local finalization metrics" >&2
    exit 1
fi
if [ "$OWNERS_WITH_PARTITIONS" -lt 2 ]; then
    capture_failure_metrics
    echo "ERROR: automatic child-owner balance did not place a partition on a remote owner" >&2
    exit 1
fi

OLD_PID=${CHUNK_KV_PIDS[0]}
kill -TERM "$OLD_PID"
# Keep the intentional restart inside the registry dead-owner window. The
# service stops admission promptly, but storage-client teardown can outlive
# that window and must not turn this split/replay check into a failover test.
restart_stop_deadline=$((SECONDS + 3))
while kill -0 "$OLD_PID" 2>/dev/null && [ "$SECONDS" -lt "$restart_stop_deadline" ]; do
    sleep 0.1
done
if kill -0 "$OLD_PID" 2>/dev/null; then
    kill -KILL "$OLD_PID" 2>/dev/null || true
fi
wait "$OLD_PID" 2>/dev/null || true
REPLAY_STARTED_MS=$(date +%s%3N)
RESTARTED_PID=0
for attempt in 1 2 3; do
    start_server 1
    RESTARTED_PID=$SERVER_PID
    if wait_ready 1 "$RESTARTED_PID"; then
        break
    fi
    RESTARTED_PID=0
    sleep 6
done
if [ "$RESTARTED_PID" -eq 0 ]; then
    echo "ERROR: restarted chunk-KV server did not become ready after 3 attempts" >&2
    exit 1
fi
REPLAY_MS=$(($(date +%s%3N) - REPLAY_STARTED_MS))
REPLAY_PARTITIONS=$(sed -n 's/.*"hosted_partitions":\([0-9][0-9]*\).*/\1/p' \
    "$LOG_ROOT/chunk-kv-1.ready.json")
REPLAY_PARTITIONS_PER_S=$((1000 * ${REPLAY_PARTITIONS:-0} / (REPLAY_MS > 0 ? REPLAY_MS : 1)))
if [ "${REPLAY_PARTITIONS:-0}" -eq 0 ]; then
    echo "ERROR: restarted chunk-KV server did not recover an assigned partition" >&2
    exit 1
fi
REPLAY_OUTPUT=$(pixi run -- ./target/release/crowdb-chunk-kv-cli --mgmt-seed "$MGMT_SEED" \
    get "$HOT_KEY_PREFIX/00000000000000000000")
printf '%s\n' "$REPLAY_OUTPUT" >"$LOG_ROOT/replay-get.log"
if ! grep -q 'result: Ok(Value(Some' <<<"$REPLAY_OUTPUT"; then
    echo "ERROR: restarted chunk-KV server did not replay the written value" >&2
    exit 1
fi

RSS_END=0
for pid in "${CHUNK_KV_PIDS[@]:1}"; do
    if kill -0 "$pid" 2>/dev/null; then
        RSS_END=$((RSS_END + $(rss_kib "$pid")))
    fi
done
RSS_DELTA=$((RSS_END - RSS_START))
printf 'operations\tconcurrency\tvalue_bytes\tread_percent\tp99_us\tpartitions\towners_with_partitions\towner_min_partitions\towner_max_partitions\tadmission_backpressure\tsplit_prepare_ms\tsplit_finalizations\tsplit_catchup_lag_records\tsplit_finalization_duration_us\trecoveries\treplay_ms\treplay_partitions\treplay_partitions_s\trss_start_kib\trss_end_kib\trss_delta_kib\treplay_ready\n' \
    >"$RESULTS_FILE"
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t1\n' \
    "$OPERATIONS" "$CONCURRENCY" "$VALUE_BYTES" "$READ_PERCENT" "$P99" "$PARTITIONS" \
    "$OWNERS_WITH_PARTITIONS" "$OWNER_MIN_PARTITIONS" "$OWNER_MAX_PARTITIONS" "$ADMISSION_BACKPRESSURE" \
    "$SPLIT_PREPARE_MS" "$SPLIT_FINALIZATIONS" \
    "$SPLIT_CATCHUP_LAG_RECORDS" "$SPLIT_FINALIZATION_DURATION_US" \
    "$RECOVERIES" "$REPLAY_MS" "$REPLAY_PARTITIONS" "$REPLAY_PARTITIONS_PER_S" \
    "$RSS_START" "$RSS_END" "$RSS_DELTA" >>"$RESULTS_FILE"
if [ "${P99:-1000001}" -gt 1000000 ] || [ "$RSS_DELTA" -gt 524288 ] \
    || [ "$SPLIT_PREPARE_MS" -gt 300000 ] \
    || [ "$SPLIT_FINALIZATION_DURATION_US" -gt 30000000 ] \
    || [ "$SPLIT_CATCHUP_LAG_RECORDS" -gt 1024 ] \
    || [ "$REPLAY_MS" -gt 30000 ]; then
    echo "ERROR: latency, memory, split, or replay bound exceeded; measured values: $RESULTS_FILE" >&2
    exit 1
fi
echo "chunk-KV regression passed; retained results: $RESULTS_FILE"
