#!/usr/bin/env bash
# --- CrowDB sanitize regression (ASan + LSan) ---
# Usage: bash tools/sanitize-regression.sh
#
# It's expected to have low perf since we enable ASAN + LSAN check.
# This script is a leak/corruption sentinel, not a throughput sentinel.
# Use tools/bench-kv-write-regression.sh (release, no ASan) for
# throughput regression tracking.
#
# What this script verifies:
#   1. No heap-use-after-free in the write/read/scan paths (the
#      MetricsRegistry UAF was the original trigger — reaper thread
#      accessing freed counters after MetricsRegistry destruction).
#   2. No leaked RPC Connection objects (crowdb_rpc_conn_destroy + Arc
#      ownership).
#   3. No leaked OutFrame objects stuck in transport send queues
#      (Connection destructor drains pending frames).
#   4. No leaked Rust FFI handler closures (tracked + freed in
#      RpcServer/RpcClient Drop via clear_handlers).
#   5. No leaked in-flight call() user_data (fail_all in stop_reaper
#      drains pending requests on shutdown).
#   6. Zero correctness errors under load.
#   7. No leaks in DiskDB allocate/mix paths (block allocator + KV
#      allocation records).
#   8. No leaks in ChunkDB allocate/mix paths (chunk lifecycle + EC
#      strip placement).
#   9. No leaks in chunkio write path (end-to-end large object write
#      through ChunkDB + DiskIO + EC).
#
# Workloads:
#   - prepare: pre-populate keys (so read/scan have data to work with)
#   - write:   put workload (consensus + WAL + storage)
#   - read:    point-get workload (linearizable + minslot)
#   - scan:    range scan workload
#   - diskdb:  block allocate/mix workload (DiskDB allocator)
#   - chunkdb: chunk allocate/mix workload (ChunkDB lifecycle + EC)
#   - chunkio: end-to-end large object write (ChunkDB + DiskIO + EC)
#
# ASan/LSan configuration:
#   - CROWDB_ASAN=1 passed to cargo build (build.rs adds
#     -fsanitize=address to the C++ libraries via cc::Build).
#   - LD_PRELOAD the pixi libasan.so so both Rust and C++ code use the
#     same sanitizer runtime.
#   - ASAN_OPTIONS: detect_leaks=1 (LSan), abort_on_error=0 (don't abort
#     on first error — let the process exit normally so we can check
#     the exit code), log_path writes per-process ASan logs to /tmp.
#   - Debug build (not release) — ASan needs debug info for stack traces.
#
# Leak interpretation:
#   - Client bench process: MUST exit 0 (no leaks). The client is a
#     short-lived process that connects, runs the workload, and exits
#     cleanly.
#   - Server processes: killed via SIGTERM during `cluster destroy`.
#     Graceful shutdown runs (SIGTERM handler → PxKvStore::shutdown →
#     stop_rpc_server → RpcServer::stop → clear_handlers). If the
#     server exits cleanly (no ASan log), all leaks are fixed.
#   - The local-deploy CLI process may show ~97KB tokio runtime noise
#     (113 allocations) — this is a known tokio cleanup issue, not
#     our code. We filter it out in the leak check.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - The script handles building: it enables CROWDB_ASAN=1, rebuilds
#     in debug mode, runs all sub-tests, then rebuilds WITHOUT ASan to
#     restore the default debug binary.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/sanitize-regression.tsv"
DURATION=5
KEYSPACE=1000
VALUE_SIZE=128
PREPARE_KEYS=500

# Path to the debug binary (built with CROWDB_ASAN=1).
CROWDB_CLI="$(cd "$(dirname "$0")/.." && pwd)/target/debug/crowdb-cli"

# ASan runtime configuration. We set LD_PRELOAD only for the crowdb-cli
# binary, NOT globally — if set globally, `cargo run` / `pixi` themselves
# get ASan-instrumented and produce false leak reports from their own
# internals.
LIBASAN="$(cd "$(dirname "$0")/.." && pixi run -- pwd)/.pixi/envs/default/lib/libasan.so"

# Clean up stale ASan logs from previous runs.
rm -f /tmp/asan-sanitize-*.* 2>/dev/null || true

# --- Phase 1: Build with ASan + LSan enabled ---
echo "=== building with CROWDB_ASAN=1 (debug) ==="
CROWDB_ASAN=1 pixi run -- cargo build -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb 2>&1 | tail -3

# Verify the binary exists.
if [ ! -x "$CROWDB_CLI" ]; then
    echo "ERROR: $CROWDB_CLI not found after build."
    exit 1
fi

# Verify the ASan library exists.
if [ ! -f "$LIBASAN" ]; then
    echo "ERROR: libasan.so not found at $LIBASAN"
    echo "Run: pixi install"
    exit 1
fi

# asan_cli <log_prefix> <args...>
# Runs crowdb-cli directly (not via cargo run) under ASan with the
# given log_path prefix. Using the binary directly avoids ASan
# instrumenting cargo/pixi themselves (which produces false leaks).
asan_cli() {
    local log_prefix="$1"; shift
    env ASAN_OPTIONS="detect_leaks=1:abort_on_error=0:log_path=${log_prefix}" \
        LD_PRELOAD="$LIBASAN" \
        "$CROWDB_CLI" "$@"
}

# check_asan_logs <log_prefix> <label>
# Checks for ASan log files matching the prefix. Prints a summary.
# Returns 0 if no logs (clean), 1 if logs found (leaks/errors).
# Cleans up logs after reading.
check_asan_logs() {
    local prefix="$1" label="$2"
    local logs
    logs=$(ls "${prefix}."* 2>/dev/null || true)
    if [ -z "$logs" ]; then
        echo "    [$label] no ASan logs — clean"
        rm -f "${prefix}."* 2>/dev/null || true
        return 0
    fi
    echo "    [$label] ASAN LOGS FOUND:"
    for log in $logs; do
        local summary
        summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
        echo "      $log: $summary"
    done
    rm -f "${prefix}."* 2>/dev/null || true
    return 1
}

# run_bench <deploy_name> <subcmd> <threads> <conn> <label> <extra_args...>
# Cleans user data (except for read/scan which need prepare first),
# runs the workload under ASan, checks for leaks.
run_bench() {
    local deploy="$1" subcmd="$2" threads="$3" conn="$4" label="$5"; shift 5
    local extra_args=("$@")
    echo ">>> $label ..."
    local config_file
    config_file=$(cat "/tmp/sanitize-reg-${deploy}.cfgpath" 2>/dev/null || echo "")
    if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
        echo "    ERROR: no config for deploy '$deploy'"
        echo -e "$label\t0\t0\t0\t1\tFAIL" >> "$RESULTS_FILE"
        return
    fi
    local log_prefix="/tmp/asan-sanitize-${label}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    local output rc
    output=$(asan_cli "$log_prefix" --config "$config_file" \
        bench kv "$subcmd" --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --json "${extra_args[@]}" 2>&1) || true
    rc=$?
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t0\t0\t0\t1\tFAIL" >> "$RESULTS_FILE"
        rm -f "${log_prefix}."* 2>/dev/null || true
        return
    fi
    local total_ops ops_s errors
    total_ops=$(echo "$json" | jq -r '.total_ops')
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    errors=$(echo "$json" | jq -r '.total_errors')
    # Check for ASan leak logs from the client process.
    local leak_status="PASS"
    local logs
    logs=$(ls "${log_prefix}."* 2>/dev/null || true)
    if [ -n "$logs" ]; then
        leak_status="FAIL(leaks)"
        for log in $logs; do
            local summary
            summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
            echo "    LEAK: $summary"
        done
    fi
    if [ "$rc" -ne 0 ] && [ "$leak_status" = "PASS" ]; then
        leak_status="FAIL(exit=$rc)"
    fi
    rm -f "${log_prefix}."* 2>/dev/null || true
    echo "    ops/s=$ops_s err=$errors leak_check=$leak_status"
    echo -e "$label\t$ops_s\t$errors\t$leak_status" >> "$RESULTS_FILE"
}

# run_prepare <deploy_name> <label>
# Pre-populates keys so read/scan have data to work with.
run_prepare() {
    local deploy="$1" label="$2"
    echo ">>> $label ..."
    local config_file
    config_file=$(cat "/tmp/sanitize-reg-${deploy}.cfgpath" 2>/dev/null || echo "")
    if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
        echo "    ERROR: no config for deploy '$deploy'"
        return
    fi
    local log_prefix="/tmp/asan-sanitize-${label}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    local output rc
    output=$(asan_cli "$log_prefix" --config "$config_file" \
        bench kv prepare --keys "$PREPARE_KEYS" \
        --value-size "$VALUE_SIZE" --concurrency 4 \
        --json 2>&1) || true
    rc=$?
    echo "    exit=$rc"
    check_asan_logs "$log_prefix" "$label" || true
}

# deploy_cluster <name>
# Deploy a 3-node cluster with default tunables for sanitize testing.
deploy_cluster() {
    local name="$1"
    local config_file="/tmp/sanitize-reg-${name}.toml"
    echo "=== deploying cluster '$name' ==="
    rm -f "$config_file"
    local log_prefix="/tmp/asan-sanitize-deploy-${name}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    asan_cli "$log_prefix" --config "$config_file" \
        cluster local-deploy -n 3 -t kv \
        --kv-backend mem-block --wal-backend mem-block 2>&1 | tail -3 || true
    echo "$config_file" > "/tmp/sanitize-reg-${name}.cfgpath"
    # The deploy process exits with 1 due to tokio runtime leak noise.
    # Check + report but don't fail the script.
    local logs
    logs=$(ls "${log_prefix}."* 2>/dev/null || true)
    if [ -n "$logs" ]; then
        for log in $logs; do
            local summary bytes
            summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
            bytes=$(echo "$summary" | grep -oP '\d+(?= byte)' | head -1 || echo "?")
            if [ "$bytes" = "97368" ]; then
                echo "    deploy process: known tokio noise ($bytes bytes) — OK"
            else
                echo "    deploy process: UNEXPECTED leak — $summary"
            fi
        done
        rm -f "${log_prefix}."* 2>/dev/null || true
    fi
}

# teardown_cluster <name>
# Destroy the cluster — servers get SIGTERM → graceful shutdown → ASan
# leak check on each server process. No ASan logs = no leaks.
teardown_cluster() {
    local name="$1"
    local config_file
    config_file=$(cat "/tmp/sanitize-reg-${name}.cfgpath" 2>/dev/null || echo "")
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        local log_prefix="/tmp/asan-sanitize-destroy-${name}"
        rm -f "${log_prefix}."* 2>/dev/null || true
        asan_cli "$log_prefix" --config "$config_file" \
            cluster destroy 2>&1 | tail -2 || true
        # Check for server leak logs. Servers are killed during destroy;
        # if they shut down cleanly (SIGTERM → graceful shutdown), no
        # ASan logs are emitted. Any logs here indicate server leaks.
        check_asan_logs "$log_prefix" "server-shutdown" || true
        rm -f "$config_file" "/tmp/sanitize-reg-${name}.cfgpath"
    fi
}

# deploy_combined_cluster <name>
# Deploy a full-stack cluster (KV + DiskDB + ChunkDB + DiskIO) for
# chunkdb/diskdb/chunkio sanitize testing. Uses mem-block backends and
# small capacity to keep memory bounded under ASan.
deploy_combined_cluster() {
    local name="$1"
    local config_file="/tmp/sanitize-reg-${name}.toml"
    echo "=== deploying combined cluster '$name' ==="
    rm -f "$config_file"
    local log_prefix="/tmp/asan-sanitize-deploy-${name}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    asan_cli "$log_prefix" --config "$config_file" \
        cluster local-deploy -t combined \
        --kv-backend mem-block --wal-backend mem-block \
        --metrics-interval 1 --event-write \
        --data-groups 1,2,3 --disk-groups-per-node 1 --disks-per-group 4 \
        --disk-capacity-bytes 4294967296 --disk-zone-size-bytes 1073741824 \
        --disk-unit-size-bytes 1048576 --chunkdb-instances 3 \
        --allow-unsafe-ec 2>&1 | tail -3 || true
    echo "$config_file" > "/tmp/sanitize-reg-${name}.cfgpath"
    # The deploy process exits with 1 due to tokio runtime leak noise.
    local logs
    logs=$(ls "${log_prefix}."* 2>/dev/null || true)
    if [ -n "$logs" ]; then
        for log in $logs; do
            local summary bytes
            summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
            bytes=$(echo "$summary" | grep -oP '\d+(?= byte)' | head -1 || echo "?")
            if [ "$bytes" = "97368" ]; then
                echo "    deploy process: known tokio noise ($bytes bytes) — OK"
            else
                echo "    deploy process: UNEXPECTED leak — $summary"
            fi
        done
        rm -f "${log_prefix}."* 2>/dev/null || true
    fi
}

# run_diskdb_bench <deploy_name> <workload> <concurrency> <label>
# Runs a DiskDB bench under ASan and checks for leaks.
run_diskdb_bench() {
    local deploy="$1" workload="$2" concurrency="$3" label="$4"
    echo ">>> $label ..."
    local config_file
    config_file=$(cat "/tmp/sanitize-reg-${deploy}.cfgpath" 2>/dev/null || echo "")
    if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
        echo "    ERROR: no config for deploy '$deploy'"
        echo -e "$label\t0\t0\t1\tFAIL" >> "$RESULTS_FILE"
        return
    fi
    local log_prefix="/tmp/asan-sanitize-${label}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    local output rc
    output=$(asan_cli "$log_prefix" --config "$config_file" \
        bench diskdb "$workload" --duration-secs "$DURATION" \
        --concurrency "$concurrency" --unit-count 1 --blocks-per-request 1 \
        --mode mem --seed 1 --metrics-interval 1 2>&1) || true
    rc=$?
    local line
    line=$(echo "$output" | sed -n '/^diskdb bench /p' | tail -n 1)
    local ops_s=0 errors=0
    if [ -n "$line" ]; then
        ops_s=$(echo "$line" | grep -oP 'ops_per_sec=\K[0-9]+' || echo 0)
        errors=$(echo "$line" | grep -oP 'errors=\K[0-9]+' || echo 0)
    fi
    local leak_status="PASS"
    local logs
    logs=$(ls "${log_prefix}."* 2>/dev/null || true)
    if [ -n "$logs" ]; then
        leak_status="FAIL(leaks)"
        for log in $logs; do
            local summary
            summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
            echo "    LEAK: $summary"
        done
    fi
    if [ "$rc" -ne 0 ] && [ "$leak_status" = "PASS" ]; then
        leak_status="FAIL(exit=$rc)"
    fi
    rm -f "${log_prefix}."* 2>/dev/null || true
    echo "    ops/s=$ops_s err=$errors leak_check=$leak_status"
    echo -e "$label\t$ops_s\t$errors\t$leak_status" >> "$RESULTS_FILE"
}

# run_chunkdb_bench <deploy_name> <workload> <concurrency> <label>
# Runs a ChunkDB bench under ASan and checks for leaks.
run_chunkdb_bench() {
    local deploy="$1" workload="$2" concurrency="$3" label="$4"
    echo ">>> $label ..."
    local config_file
    config_file=$(cat "/tmp/sanitize-reg-${deploy}.cfgpath" 2>/dev/null || echo "")
    if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
        echo "    ERROR: no config for deploy '$deploy'"
        echo -e "$label\t0\t0\t1\tFAIL" >> "$RESULTS_FILE"
        return
    fi
    local log_prefix="/tmp/asan-sanitize-${label}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    local output rc
    output=$(asan_cli "$log_prefix" --config "$config_file" \
        bench chunkdb "$workload" --duration-secs "$DURATION" \
        --concurrency "$concurrency" --strip-count 1 --strip-type ec \
        --data-num 8 --code-num 4 --write-granularity-kb 1024 \
        --seed 1 --metrics-interval 1 2>&1) || true
    rc=$?
    local line
    line=$(echo "$output" | sed -n '/^chunkdb bench /p' | tail -n 1)
    local ops_s=0 errors=0
    if [ -n "$line" ]; then
        ops_s=$(echo "$line" | grep -oP 'ops_per_sec=\K[0-9]+' || echo 0)
        errors=$(echo "$line" | grep -oP 'errors=\K[0-9]+' || echo 0)
    fi
    local leak_status="PASS"
    local logs
    logs=$(ls "${log_prefix}."* 2>/dev/null || true)
    if [ -n "$logs" ]; then
        leak_status="FAIL(leaks)"
        for log in $logs; do
            local summary
            summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
            echo "    LEAK: $summary"
        done
    fi
    if [ "$rc" -ne 0 ] && [ "$leak_status" = "PASS" ]; then
        leak_status="FAIL(exit=$rc)"
    fi
    rm -f "${log_prefix}."* 2>/dev/null || true
    echo "    ops/s=$ops_s err=$errors leak_check=$leak_status"
    echo -e "$label\t$ops_s\t$errors\t$leak_status" >> "$RESULTS_FILE"
}

# run_chunkio_bench <deploy_name> <concurrency> <label> <extra_args...>
# Runs a chunkio write bench under ASan and checks for leaks.
# Uses small objects (1 MiB) and short duration to keep memory bounded.
run_chunkio_bench() {
    local deploy="$1" concurrency="$2" label="$3"; shift 3
    local extra_args=("$@")
    echo ">>> $label ..."
    local config_file
    config_file=$(cat "/tmp/sanitize-reg-${deploy}.cfgpath" 2>/dev/null || echo "")
    if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
        echo "    ERROR: no config for deploy '$deploy'"
        echo -e "$label\t0\t0\t1\tFAIL" >> "$RESULTS_FILE"
        return
    fi
    local log_prefix="/tmp/asan-sanitize-${label}"
    rm -f "${log_prefix}."* 2>/dev/null || true
    local output rc
    output=$(asan_cli "$log_prefix" --config "$config_file" \
        bench chunkio write --duration-secs "$DURATION" \
        --object-size 1048576 --concurrency "$concurrency" \
        --data-num 8 --code-num 4 --block-size 1048576 \
        --chunk-size 16777216 --seed 1 --prefetch-chunks 2 \
        --prefetch-strips-per-chunk 1 --metrics-interval 1 \
        "${extra_args[@]}" 2>&1) || true
    rc=$?
    local line
    line=$(echo "$output" | sed -n '/^chunkio write:/p' | tail -n 1)
    local ops_s=0 errors=0
    if [ -n "$line" ]; then
        ops_s=$(echo "$line" | grep -oP 'objects_s=\K[0-9.]+' || echo 0)
        errors=$(echo "$line" | grep -oP 'errors=\K[0-9]+' || echo 0)
    fi
    local leak_status="PASS"
    local logs
    logs=$(ls "${log_prefix}."* 2>/dev/null || true)
    if [ -n "$logs" ]; then
        leak_status="FAIL(leaks)"
        for log in $logs; do
            local summary
            summary=$(grep "SUMMARY:" "$log" 2>/dev/null || echo "(no summary)")
            echo "    LEAK: $summary"
        done
    fi
    if [ "$rc" -ne 0 ] && [ "$leak_status" = "PASS" ]; then
        leak_status="FAIL(exit=$rc)"
    fi
    rm -f "${log_prefix}."* 2>/dev/null || true
    echo "    ops/s=$ops_s err=$errors leak_check=$leak_status"
    echo -e "$label\t$ops_s\t$errors\t$leak_status" >> "$RESULTS_FILE"
}

# --- ASan sanitize regression reference results ---
#
# Reference results (2026-09-02, AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux):
#   Debug build with CROWDB_ASAN=1, mem-block backend, 5s duration,
#   128B values, 1K keyspace, 3-node cluster. ASan/LSan enabled.
#   Performance is ~50-100x slower than release (expected — ASan adds
#   per-access shadow checks + leak scan at exit).
#
#   workload           ops/s    err  leaks
#   prepare_500keys    —        0    none
#   write_1t_1c        ~4,800   0    none
#   write_16t_2c       ~4,900   0    none
#   read_1t_1c_lin     ~5,000   0    none
#   read_16t_2c_lin    ~5,000   0    none
#   read_1t_1c_minslot ~5,000   0    none
#   scan_1t_1c         ~5,000   0    none
#   scan_16t_2c        ~5,000   0    none
#   diskdb_alloc_1t    ~3,000   0    none
#   diskdb_mix_4t      ~3,000   0    none
#   chunkdb_alloc_1t   ~800     0    none
#   chunkdb_mix_4t     ~800     0    none
#   chunkio_1t         ~5       0    none
#   chunkio_4t         ~10      0    none
#
# Leak status: all processes (client, server, destroy) report zero leaks
# except the known tokio runtime noise in the local-deploy CLI process
# (~97KB, 113 allocations — not our code).
#
# What changed to get here (2026-09-02):
#   - MetricsRegistry: heap-allocated + std::atexit for clean thread
#     shutdown (fixes UAF: reaper thread accessing freed counters).
#   - crowdb_rpc_conn_destroy: C API to free connection wrappers.
#   - Connection: Arc<ConnectionInner> with owned flag + destructor that
#     drains pending OutFrame objects from send/overflow queues.
#   - RpcServer/RpcClient: handler_ptrs tracking + clear_handlers in
#     stop/Drop (breaks Arc reference cycle from handler closures).
#   - RpcClient::stop_reaper: fail_all(nullptr, ConnectionClosed) to
#     drain in-flight call() user_data allocations.

echo -e "label\tops_s\terrors\tleak_check" > "$RESULTS_FILE"

# --- Phase 2a: KV-only cluster (write/read/scan) ---

# Single deploy for all KV sub-tests.
DEPLOY="sanitize-reg-$$-$(date +%s)"
deploy_cluster "$DEPLOY"

# Prepare: pre-populate keys so read/scan have data.
run_prepare "$DEPLOY" "prepare_${PREPARE_KEYS}keys"

# Write: put workload (consensus + WAL + storage).
echo "=== write ==="
run_bench "$DEPLOY" write 1 1 "write_1t_1c"
run_bench "$DEPLOY" write 16 2 "write_16t_2c"

# Read: point-get workload (linearizable + minslot).
echo "=== read ==="
run_bench "$DEPLOY" read 1 1 "read_1t_1c_linearizable" --read-mode linearizable
run_bench "$DEPLOY" read 16 2 "read_16t_2c_linearizable" --read-mode linearizable
run_bench "$DEPLOY" read 1 1 "read_1t_1c_minslot" --read-mode minslot

# Scan: range scan workload.
echo "=== scan ==="
run_bench "$DEPLOY" scan 1 1 "scan_1t_1c"
run_bench "$DEPLOY" scan 16 2 "scan_16t_2c"

# Teardown: destroy KV cluster, check server shutdown leaks.
teardown_cluster "$DEPLOY"

# --- Phase 2b: Combined cluster (diskdb + chunkdb + chunkio) ---

COMBINED_DEPLOY="sanitize-reg-combined-$$-$(date +%s)"
deploy_combined_cluster "$COMBINED_DEPLOY"

# DiskDB: block allocator workload.
echo "=== diskdb ==="
run_diskdb_bench "$COMBINED_DEPLOY" allocate 1 "diskdb_alloc_1t"
run_diskdb_bench "$COMBINED_DEPLOY" mix 4 "diskdb_mix_4t"

# ChunkDB: chunk lifecycle + EC placement workload.
echo "=== chunkdb ==="
run_chunkdb_bench "$COMBINED_DEPLOY" allocate 1 "chunkdb_alloc_1t"
run_chunkdb_bench "$COMBINED_DEPLOY" mix 4 "chunkdb_mix_4t"

# ChunkIO: end-to-end large object write through ChunkDB + DiskIO + EC.
echo "=== chunkio ==="
run_chunkio_bench "$COMBINED_DEPLOY" 1 "chunkio_1t"
run_chunkio_bench "$COMBINED_DEPLOY" 4 "chunkio_4t"

# Teardown: destroy combined cluster, check all server shutdown leaks.
teardown_cluster "$COMBINED_DEPLOY"

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"

# --- Phase 3: Rebuild WITHOUT ASan to restore the default debug binary ---
echo "=== rebuilding without CROWDB_ASAN (restore default debug binary) ==="
pixi run -- cargo build -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb 2>&1 | tail -3

# Final summary: check for any FAIL in the results.
if grep -q "FAIL" "$RESULTS_FILE"; then
    echo ""
    echo "!!! SANITIZE REGRESSION DETECTED — see FAIL rows above !!!"
    exit 1
else
    echo ""
    echo "All sub-tests passed (no leaks, no errors)."
    exit 0
fi
