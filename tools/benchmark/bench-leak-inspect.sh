#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Inspect cumulative memory against an externally owned container deployment.
# KV_BENCH_DEPLOYMENT is required. Allocator settings belong to deployment time.
# No reset between cases: growth is intentionally retained for inspection.
# Exit leaves the containers running; their owner handles shutdown.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/benchmark/bench-kv-common.sh
[[ $(jq -r '.kind // "container"' "$KV_BENCH_DEPLOYMENT") == container ]] || {
    echo 'Leak inspection requires a container deployment' >&2; exit 2;
}
sample_memory() {
    local label="$1"
    docker inspect "${containers[@]}" > "$LOG_ROOT/$label-inspect.json"
    docker stats --no-stream --format '{{json .}}' "${containers[@]}" | tee "$LOG_ROOT/$label-memory.jsonl"
}
run_case() {
    local threads="$1" connections="$2" workers="$3" label="write_${1}t"
    selected "$label" || return 0
    sample_memory "$label-before"
    invoke "$label" write --key-space "$KEYSPACE" --value-size "${KV_BENCH_VALUE_SIZE:-512}" \
        --loader-num "$threads" --connections "$connections" --rpc-workers "$workers" --event-write
    sample_memory "$label-after"
}
run_case 1 1 2
run_case 16 2 2
run_case 64 4 2
run_case 128 4 4
finish_cases
echo "Containers remain running for inspection; their owner must stop them."

# Diagnostic history (2026-10-28 Asia/Shanghai; raw CLI timestamps are UTC 2026-10-27)
# AMD Ryzen 9 5950X, Linux x86_64, 32 logical CPUs; 3-node host network.
# Container cumulative-memory smoke; 1/2/1; 1k keys x 512B; no CPU/memory quota; not a leak-freedom proof
# Artifacts: .crowdb-runtime/artifacts/tools-migration-20261028/leak
# 2026-10-27 write_128t host-container ops/s=148813.0 latency_ns={"avg":859382.5470557008,"count":297626,"max":2702898,"min":344565,"p50":835583,"p99":1449983,"p999":1744895} passed=true
# 2026-10-27 write_16t host-container ops/s=44567.5 latency_ns={"avg":358787.00964828633,"count":89135,"max":3372416,"min":199503,"p50":342015,"p99":618495,"p999":1458175} passed=true
# 2026-10-27 write_1t host-container ops/s=4365.5 latency_ns={"avg":228884.86095521704,"count":8731,"max":800821,"min":161343,"p50":227327,"p99":288767,"p999":356351} passed=true
# 2026-10-27 write_64t host-container ops/s=108350.0 latency_ns={"avg":590196.4161744347,"count":216700,"max":2162980,"min":270383,"p50":573439,"p99":974847,"p999":1662975} passed=true
