#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Profile servers in three prestarted host-network containers (or the host client).
# Usage: KV_BENCH_DEPLOYMENT=state.json pixi run bash tools/profiling/profile-write.sh [perf|samply] [20]
# KV_PROFILE_TARGET=client selects host-client sampling; the default is servers.
# Server sampling requires host permission to attach to each server UID. The
# script archives container binaries/DSOs for symbolization; it changes no permissions.
# For client sampling, supply KV_BENCH_CLI with symbols. Build separately with
# CARGO_PROFILE_RELEASE_DEBUG=line-tables-only pixi run cargo build --release -p crowdb-cli.
# The sample includes preparation/verification; report.json measures only the middle window.
# This script never builds, starts, stops, resets or changes the server deployment.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
SAMPLER="${1:-perf}"
PROFILE_TARGET="${KV_PROFILE_TARGET:-servers}"
PROFILE_FREQUENCY="${KV_PROFILE_FREQUENCY:-99}"
export KV_BENCH_DURATION="${2:-${KV_BENCH_DURATION:-20}}"
export KV_BENCH_LOG_ROOT="${KV_BENCH_LOG_ROOT:-$PWD/.crowdb-runtime/artifacts/profile/write-$(date +%Y%m%d-%H%M%S)-$$}"
source tools/benchmark/bench-kv-common.sh
PERF=(perf --buildid-dir "$LOG_ROOT/buildid-cache")
[[ $(jq -r '.kind // "container"' "$KV_BENCH_DEPLOYMENT") == container ]] || {
    echo 'Profiling requires a container deployment' >&2; exit 2;
}
case "$SAMPLER" in
    perf)
        command -v perf >/dev/null
        if [[ "$PROFILE_TARGET" == client ]]; then
            BENCH_PREFIX=("${PERF[@]}" record -F "$PROFILE_FREQUENCY" --call-graph dwarf -o "$LOG_ROOT/perf.data" --)
        elif [[ "$PROFILE_TARGET" != servers ]]; then
            echo 'KV_PROFILE_TARGET must be servers or client' >&2; exit 2
        fi
        ;;
    samply)
        [[ "$PROFILE_TARGET" == client ]] || {
            echo 'Container server sampling uses perf; select KV_PROFILE_TARGET=client for samply' >&2; exit 2;
        }
        command -v samply >/dev/null
        BENCH_PREFIX=(samply record --save-only -o "$LOG_ROOT/profile.json.gz" --)
        ;;
    *) echo 'Expected perf or samply' >&2; exit 2 ;;
esac
sampler_pid=""
stop_sampler() {
    [[ -n "$sampler_pid" ]] || return 0
    local status=0 interrupted=0
    if kill -INT "$sampler_pid" 2>/dev/null; then interrupted=1; fi
    wait "$sampler_pid" || status=$?
    sampler_pid=""
    cat "$LOG_ROOT/perf-record.stderr" >&2
    # perf may return 128+SIGINT after successfully finalizing the recording.
    # Accept this only when we requested its normal stop, not for other errors.
    if [[ "$interrupted" == 1 && "$status" == 130 ]]; then status=0; fi
    return "$status"
}
cleanup() {
    local status=$?
    trap - EXIT
    stop_sampler || status=1
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ "$PROFILE_TARGET" == servers ]]; then
    # Docker's init PID may be a launcher. Resolve the actual server executable
    # in each container, and retain the process/image identity with the profile.
    server_pids=$(python tools/profiling/container-server-pids.py \
        --state "$KV_BENCH_DEPLOYMENT" --output "$LOG_ROOT/server-processes.json")
    "${PERF[@]}" record -F "$PROFILE_FREQUENCY" --call-graph dwarf --pid "$server_pids" -o "$LOG_ROOT/perf.data" \
        > "$LOG_ROOT/perf-record.stdout" 2> "$LOG_ROOT/perf-record.stderr" &
    sampler_pid=$!
    sleep .2
    if ! kill -0 "$sampler_pid" 2>/dev/null; then
        stop_sampler || true
        echo 'Server profiler exited before load; inspect perf-record.stderr' >&2; exit 1
    fi
fi
invoke profile-write write --key-space "$KEYSPACE" --value-size "${KV_BENCH_VALUE_SIZE:-512}" \
    --loader-num "${KV_BENCH_LOADERS:-24}" --connections "${KV_BENCH_CONNECTIONS:-24}" --event-write
stop_sampler
if [[ "$SAMPLER" == perf ]]; then
    symbol_options=()
    if [[ "$PROFILE_TARGET" == servers ]]; then symbol_options=(--symfs "$LOG_ROOT/symbols/rootfs"); fi
    "${PERF[@]}" script "${symbol_options[@]}" -i "$LOG_ROOT/perf.data" > "$LOG_ROOT/perf-script.txt"
    [[ -s "$LOG_ROOT/perf-script.txt" ]] || { echo 'Profile contains no samples' >&2; exit 1; }
    if command -v inferno-collapse-perf >/dev/null && command -v inferno-flamegraph >/dev/null; then
        inferno-collapse-perf --all < "$LOG_ROOT/perf-script.txt" | \
            inferno-flamegraph --title "Paxos-KV $PROFILE_TARGET (whole invocation)" > "$LOG_ROOT/flamegraph.svg"
    fi
fi
echo "$PROFILE_TARGET profile and workload report: $LOG_ROOT"

# Diagnostic history (2026-10-28 Asia/Shanghai; raw CLI timestamps are UTC 2026-10-27)
# AMD Ryzen 9 5950X, Linux x86_64, 32 logical CPUs; 3-node host network.
# perf host-client smoke; 1/2/1; 1k keys x 512B; 4T/2conn; not a formal perf baseline
# Artifacts: .crowdb-runtime/artifacts/tools-migration-20261028/profile
# 2026-10-27 profile-write host-container ops/s=12240.0 latency_ns={"avg":326643.42691993463,"count":24480,"max":1008324,"min":170362,"p50":342015,"p99":438271,"p999":507903} passed=true
# Server smoke (2026-10-28 Asia/Shanghai): 3 host-network containers, no resource quotas.
# Same release binary; diagnostic image changes crowdb UID/GID to host 1000 for perf attach.
# 1/2/5 seconds, 1k keys x 512B, 4T/2conn, 99Hz; whole invocation, not a formal baseline.
# 5829 samples / 48.459 MB; server functions resolved with archived container symfs.
# Kernel symbols remain restricted. All three servers stopped gracefully with exit 0.
# Artifacts: .crowdb-runtime/artifacts/server-profile-final-20261028/profile-final
# profile-write host-container ops/s=11412.5 passed=true; p99=589823ns, p999=839679ns.
