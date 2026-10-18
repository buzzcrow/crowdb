#!/usr/bin/env bash
# RDMA RPC echo benchmark. Mirrors bench-rpc-regression.sh with an HCA-backed
# connection so transport throughput and latency can be compared directly.
set -euo pipefail
cd "$(dirname "$0")/../.."

RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${RDMA_BENCH_LOG_ROOT:-${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/rdma-rpc-$RUN_STAMP}"
RESULTS_FILE="${RDMA_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/benchmark/bench-regression-common.sh
export CROWDB_LOG_ROOT="$LOG_ROOT"
regression_init

DURATION="${RDMA_BENCH_DURATION:-10}"
VALUE_SIZE="${RDMA_BENCH_VALUE_SIZE:-128}"
CASES="${RDMA_BENCH_CASES:-}"
ADDR="${CROWDB_RDMA_SOURCE_ADDR:-10.200.1.1}"
DEVICE="${CROWDB_RDMA_DEVICE:-rocep35s0f0}"
HCA_PORT="${CROWDB_RDMA_PORT:-1}"
PORT_OFFSET="${CROWDB_PORT_OFFSET:-0}"
PORT=$((19101 + PORT_OFFSET))
SERVER_PID=""
SERVER_BIN="${RDMA_BENCH_SERVER_BIN:-lib/crowdb-rpc/build/crowdb-rpc-fb-server}"
CLI_BIN="${RDMA_BENCH_CLI_BIN:-target/release/crowdb-cli}"

start_server() {
    local workers="$1"
    echo "    [server] RDMA host=$ADDR port=$PORT device=$DEVICE workers=$workers" >&2
    CROWDB_RDMA_ENABLED=1 CROWDB_RDMA_E2E_PORT="$PORT" \
    CROWDB_RDMA_DEVICE="$DEVICE" CROWDB_RDMA_PORT="$HCA_PORT" \
    CROWDB_RDMA_SOURCE_ADDR="$ADDR" \
        "$SERVER_BIN" --host "$ADDR" --port "$PORT" \
        --io-engines 1 --io-workers "$workers" --logdir "$LOG_ROOT/server" \
        >"$LOG_ROOT/server.stdout" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 100); do
        if rg -q '^listening port=' "$LOG_ROOT/server.stdout"; then return 0; fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            cat "$LOG_ROOT/server.stdout" >&2
            return 1
        fi
        sleep 0.1
    done
    echo "    [server] timeout waiting for readiness" >&2
    return 1
}

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

run_bench() {
    local loaders="$1" connections="$2" label="$3" workers="$4"
    if [[ -n "$CASES" && " $CASES " != *" $label "* ]]; then return; fi
    echo ">>> $label (RDMA, workers=$workers) ..."
    stop_server
    mkdir -p "$LOG_ROOT/server"
    start_server "$workers"
    local output json
    output=$(CROWDB_RDMA_ENABLED=1 CROWDB_RDMA_LISTEN=0 CROWDB_RDMA_E2E_PORT="$PORT" \
        CROWDB_RDMA_DEVICE="$DEVICE" CROWDB_RDMA_PORT="$HCA_PORT" \
        CROWDB_RDMA_SOURCE_ADDR="$ADDR" CROWDB_PORT_OFFSET="$PORT_OFFSET" \
        pixi run -- "$CLI_BIN" bench rpc \
        --duration-secs "$DURATION" --loader-num "$loaders" --connections "$connections" \
        --value-size "$VALUE_SIZE" --io-engines 1 --io-workers "$workers" \
        --server-host "$ADDR" --server-port "$PORT" 2>&1) || true
    json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [[ -z "$json" ]]; then
        echo "$output" | tail -20 >&2
        printf '%s\t%s\t0\t0\t0\t0\t0\t1\n' "$label" "$workers" >>"$RESULTS_FILE"
    else
        local ops avg p50 p99 p999 errors
        ops=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
        avg=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
        p50=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
        p99=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
        p999=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
        errors=$(echo "$json" | jq -r '.total_errors')
        echo "    ops/s=$ops avg=${avg}us p50=${p50}us p99=${p99}us p999=${p999}us err=$errors"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$workers" "$ops" "$avg" "$p50" "$p99" "$p999" "$errors" >>"$RESULTS_FILE"
    fi
    stop_server
}

trap stop_server EXIT
mkdir -p "$LOG_ROOT"
printf 'label\twkr\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors\n' >"$RESULTS_FILE"
echo "=== RDMA RPC echo ($VALUE_SIZE B, ${DURATION}s, HCA $DEVICE:$HCA_PORT) ==="
run_bench 1 1 rdma_1w_1l_1c 1
run_bench 64 4 rdma_4w_64l_4c 4
run_bench 512 8 rdma_8w_512l_8c 8
run_bench 1000 32 rdma_16w_1000l_32c 16
echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
