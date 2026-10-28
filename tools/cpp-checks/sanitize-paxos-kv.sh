#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# ASan/LSan correctness gate: three owned containers, external host load client.
# Requires a local CROWDB_SANITIZER_BASE_IMAGE. No CPU or memory quota.
# --built reuses binaries built by sanitize-regression.sh; symbol checks still run.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${CROWDB_SANITIZER_BASE_IMAGE:?set a local CrowDB OCI base image}"
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}"
export CARGO_PROFILE_DEV_DEBUG=line-tables-only
export KV_BENCH_LOG_ROOT="${KV_BENCH_LOG_ROOT:-$PWD/.crowdb-runtime/artifacts/sanitize-paxos-kv-$(date +%Y%m%d-%H%M%S)-$$}"
mkdir -p "$KV_BENCH_LOG_ROOT"
if [[ "${1:-}" != --built ]]; then
    export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target/sanitize-paxos-kv}"
    CROWDB_ASAN=1 pixi run cargo build --locked -p crowdb-cli -p crowdb-paxos-kv-server
fi
BUILD_DIR="${CARGO_TARGET_DIR:-$PWD/target}/debug"
export KV_BENCH_CLI="$BUILD_DIR/crowdb-cli"
pixi run python - "$KV_BENCH_CLI" <<'PY'
import subprocess
import sys
symbols = subprocess.check_output(['readelf', '--dyn-syms', '--wide', sys.argv[1]], text=True)
if '__asan_init' not in symbols:
    raise SystemExit('client does not link ASan; build with CROWDB_ASAN=1')
PY
export KV_BENCH_DEPLOYMENT="$KV_BENCH_LOG_ROOT/deployment.json"
export KV_BENCH_KEYSPACE="${KV_BENCH_KEYSPACE:-1000}"
# Diagnostic smoke duration; the same load model, not a release TPS baseline.
export KV_BENCH_DURATION="${KV_BENCH_DURATION:-5}"
export KV_BENCH_WARMUP="${KV_BENCH_WARMUP:-1}"
export KV_BENCH_TAIL="${KV_BENCH_TAIL:-1}"
cleanup() {
    local code=$?
    trap - EXIT
    if [[ -f "$KV_BENCH_DEPLOYMENT" ]]; then
        pixi run python container/oci-image/paxos-kv-cluster.py down --state "$KV_BENCH_DEPLOYMENT" \
            --graceful --artifacts "$KV_BENCH_LOG_ROOT/servers" || code=1
    fi
    exit "$code"
}
trap cleanup EXIT
pixi run python tools/cpp-checks/sanitize-paxos-image.py --binary "$BUILD_DIR/crowdb-paxos-kv-server" \
    --base-image "$CROWDB_SANITIZER_BASE_IMAGE" --output "$KV_BENCH_LOG_ROOT/image"
pixi run python container/oci-image/paxos-kv-cluster.py up --state "$KV_BENCH_DEPLOYMENT" \
    --image "$(cat "$KV_BENCH_LOG_ROOT/image/image-id")" --sanitizer
source tools/benchmark/bench-kv-common.sh
ASAN_LIBRARY=$(pixi run python -c 'import os; print(os.path.join(os.environ["CONDA_PREFIX"], "lib/libasan.so"))')
BENCH_PREFIX=(env "LD_PRELOAD=$ASAN_LIBRARY" "ASAN_OPTIONS=detect_leaks=1:abort_on_error=0:log_path=$LOG_ROOT/client-asan")
run_case() {
    local label="$1" operation="$2" threads="$3" connections="$4"; shift 4
    invoke "$label" "$operation" --key-space "$KEYSPACE" --value-size 128 \
        --loader-num "$threads" --connections "$connections" "$@"
    local reports=("$LOG_ROOT"/client-asan.*)
    if [[ -f "${reports[0]}" ]]; then cat "${reports[@]}" >&2; return 1; fi
}
run_case prepare prepare 4 4
run_case write_1t write 1 1
run_case write_16t write 16 2
run_case write_64t write 64 4
run_case read_1t read 1 1 --read-mode linearizable
run_case read_16t read 16 2 --read-mode linearizable
run_case read_32t read 32 32 --read-mode linearizable
run_case read_minslot read 1 1 --read-mode minslot
run_case scan_1t scan 1 1
run_case scan_16t scan 16 2
run_case scan_32t scan 32 32
# Server exit/leak checks run in cleanup, and can fail this gate.
echo "Client load cases passed; checking container shutdown: $LOG_ROOT"

# Diagnostic history (2026-10-28 Asia/Shanghai; raw CLI timestamps are UTC 2026-10-27)
# AMD Ryzen 9 5950X, Linux x86_64, 32 logical CPUs; 3-node host network.
# ASan/LSan diagnostic; 1/5/1; 1k keys x 128B; no CPU/memory quota; all 3 servers exit 0; no sanitizer reports
# Artifacts: .crowdb-runtime/artifacts/sanitize-paxos-kv-20261028-run3
# 2026-10-27 read_16t host-container ops/s=30377.0 latency_ns={"avg":525800.1293676137,"count":151885,"max":25600828,"min":234074,"p50":479231,"p99":1032191,"p999":2179071} passed=true
# 2026-10-27 read_1t host-container ops/s=2029.2 latency_ns={"avg":491675.2288586635,"count":10146,"max":948873,"min":271194,"p50":518143,"p99":667647,"p999":741375} passed=true
# 2026-10-27 read_32t host-container ops/s=29203.2 latency_ns={"avg":1094542.8122808458,"count":146016,"max":19420729,"min":259994,"p50":1056767,"p99":1884159,"p999":2457599} passed=true
# 2026-10-27 read_minslot host-container ops/s=2221.4 latency_ns={"avg":449080.81120014406,"count":11107,"max":936864,"min":250033,"p50":471039,"p99":655359,"p999":745471} passed=true
# 2026-10-27 scan_16t host-container ops/s=7433.6 latency_ns={"avg":2150358.186262376,"count":37168,"max":21788473,"min":1365190,"p50":2162687,"p99":5144575,"p999":7766015} passed=true
# 2026-10-27 scan_1t host-container ops/s=329.0 latency_ns={"avg":3035564.5708206687,"count":1645,"max":47784866,"min":1685694,"p50":2981887,"p99":3735551,"p999":23855103} passed=true
# 2026-10-27 scan_32t host-container ops/s=9528.6 latency_ns={"avg":3354508.0670402786,"count":47643,"max":17309319,"min":1527312,"p50":3276799,"p99":6619135,"p999":11141119} passed=true
# 2026-10-27 write_16t host-container ops/s=9336.6 latency_ns={"avg":1712016.3134331557,"count":46683,"max":10740314,"min":1070495,"p50":1622015,"p99":2850815,"p999":9109503} passed=true
# 2026-10-27 write_1t host-container ops/s=615.2 latency_ns={"avg":1623120.0812743823,"count":3076,"max":2441336,"min":833972,"p50":1646591,"p99":1867775,"p999":1998847} passed=true
# 2026-10-27 write_64t host-container ops/s=28933.8 latency_ns={"avg":2209824.375146023,"count":144669,"max":23462877,"min":1225008,"p50":2113535,"p99":3342335,"p999":15728639} passed=true
