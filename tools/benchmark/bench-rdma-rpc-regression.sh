#!/usr/bin/env bash
# RDMA RPC echo benchmark. Mirrors bench-rpc-regression.sh with an HCA-backed
# connection so transport throughput and latency can be compared directly.
#
# Historical run (2026-10-18, 3s per case, reported 0 errors):
# Audit on 2026-10-22 found TCP socket traffic matching the 128B counts.
# These values are not a verified RDMA baseline; retain them for provenance.
#   payload  workers/connections  ops/s       avg-us  p50-us  p99-us
#   128 B    1/1                  43,161      22.1    65      65
#   128 B    4/4                 441,779     143.6   124     215
#   128 B    8/8                 732,084     695.0   675     917
#   128 B   16/32              1,186,876     832.9   419   5,439
#   1 KiB    1/1                  43,003      22.2    65      65
#   1 KiB    4/4                 414,466     153.2   136     221
#   1 KiB    8/8                 707,931     718.3   704     954
#   1 KiB   16/32              1,120,659     882.5   448   6,193
#   4 KiB    1/1                  39,625      24.0    65      65
#   4 KiB    4/4                 376,107     168.8   149     266
#   4 KiB    8/8                 582,940     872.2   811   1,810
#   4 KiB   16/32                834,076   1,184.5   606   6,881
# New RDMA results require strict path selection and a valid structured report.
# Record the exact HCA, binary, duration, payload, and concurrency configuration.
set -euo pipefail
cd "$(dirname "$0")/../.."

RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${RDMA_BENCH_LOG_ROOT:-${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/rdma-rpc-$RUN_STAMP}"
RESULTS_FILE="${RDMA_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/benchmark/bench-regression-common.sh
export CROWDB_LOG_ROOT="$LOG_ROOT"
export CROWDB_RDMA_ALLOW_TCP_FALLBACK=0
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
    local output json client_exit=0
    output=$(CROWDB_RDMA_ENABLED=1 CROWDB_RDMA_LISTEN=0 CROWDB_RDMA_E2E_PORT="$PORT" \
        CROWDB_RDMA_DEVICE="$DEVICE" CROWDB_RDMA_PORT="$HCA_PORT" \
        CROWDB_RDMA_SOURCE_ADDR="$ADDR" CROWDB_PORT_OFFSET="$PORT_OFFSET" \
        pixi run -- "$CLI_BIN" bench rpc \
        --duration-secs "$DURATION" --loader-num "$loaders" --connections "$connections" \
        --mode coroutine \
        --value-size "$VALUE_SIZE" --io-engines 1 --io-workers "$workers" \
        --server-host "$ADDR" --server-port "$PORT" 2>&1) || client_exit=$?
    printf '%s\n' "$output" >"$LOG_ROOT/$label.client.log"
    json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [[ -z "$json" ]] || ! jq -e '(.total_ops | type) == "number" and
        (.total_errors | type) == "number" and (.duration_ms | type) == "number" and
        (.by_op.write.latency_us.avg_us | type) == "number" and
        (.by_op.write.latency_us.p50_us | type) == "number" and
        (.by_op.write.latency_us.p99_us | type) == "number" and
        .total_ops >= 0 and .total_errors >= 0 and .duration_ms > 0' <<<"$json" >/dev/null; then
        printf '%s\n' "$output" >&2
        printf '%s\t%s\tNA\tNA\tNA\tNA\tNA\tNA\t%s\t1\t0\n' \
            "$label" "$workers" "$client_exit" >>"$RESULTS_FILE"
        stop_server
        echo "    ERROR: missing or invalid structured benchmark report; client_exit=$client_exit" >&2
        return 1
    else
        local ops avg p50 p99 p999 errors valid=0
        ops=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
        avg=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
        p50=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
        p99=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
        p999=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
        errors=$(echo "$json" | jq -r '.total_errors')
        if [[ "$client_exit" == 0 && "$errors" == 0 ]] && jq -e '.total_ops > 0' <<<"$json" >/dev/null; then
            valid=1
        fi
        echo "    ops/s=$ops avg=${avg}us p50=${p50}us p99=${p99}us p999=${p999}us err=$errors"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t%s\n' \
            "$label" "$workers" "$ops" "$avg" "$p50" "$p99" "$p999" "$errors" "$client_exit" "$valid" >>"$RESULTS_FILE"
        stop_server
        if [[ "$valid" != 1 ]]; then
            echo "    ERROR: failed benchmark profile; client_exit=$client_exit errors=$errors" >&2
            return 1
        fi
    fi
}

trap stop_server EXIT
mkdir -p "$LOG_ROOT"
printf 'label\twkr\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors\tclient_exit\treport_errors\tvalid\n' >"$RESULTS_FILE"
echo "=== RDMA RPC echo ($VALUE_SIZE B, ${DURATION}s, HCA $DEVICE:$HCA_PORT) ==="
run_bench 1 1 rdma_1w_1l_1c 1
run_bench 64 4 rdma_4w_64l_4c 4
run_bench 512 8 rdma_8w_512l_8c 8
run_bench 1000 32 rdma_16w_1000l_32c 16
echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
