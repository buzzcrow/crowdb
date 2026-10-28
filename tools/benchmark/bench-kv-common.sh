#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Workload driver only: deployment is external, all accounting belongs to CLI.
set -euo pipefail
# Match the baseline runtime to available logical CPUs, independently of RPC workers.
export TOKIO_WORKER_THREADS="$(python -c 'import os; print(len(os.sched_getaffinity(0)))')"
CLI="${KV_BENCH_CLI:-$PWD/target/release/crowdb-cli}"
: "${KV_BENCH_DEPLOYMENT:?set KV_BENCH_DEPLOYMENT to the prestarted deployment state JSON}"
LOG_ROOT="${KV_BENCH_LOG_ROOT:-$PWD/.crowdb-runtime/artifacts/bench/kv-$(date +%Y%m%d-%H%M%S)-$$}"
mkdir -p "$LOG_ROOT"
DURATION="${KV_BENCH_DURATION:-20}"
WARMUP="${KV_BENCH_WARMUP:-5}"
TAIL="${KV_BENCH_TAIL:-5}"
KEYSPACE="${KV_BENCH_KEYSPACE:-1000000}"
CASES="${KV_BENCH_CASES:-}"
selected_cases=0
known_cases=""
last_dataset=""
# Optional observer (perf/samply); it wraps the client, never the deployment.
BENCH_PREFIX=()

selected() {
    known_cases+=" $1"
    if [[ -n "$CASES" && " $CASES " != *" $1 "* ]]; then return 1; fi
    selected_cases=$((selected_cases + 1))
}

load_deployment() {
    local state="$1"
    mapfile -t endpoints < <(jq -er '.endpoints[]' "$state")
    deployment=()
    if [[ $(jq -r '.kind // "container"' "$state") == native-process ]]; then
        mapfile -t pids < <(jq -er '.pids[]' "$state")
        [[ ${#endpoints[@]} == 3 && ${#pids[@]} == 3 ]] || { echo 'Expected three native PIDs/endpoints' >&2; exit 2; }
        for i in 0 1 2; do deployment+=(--endpoint "${endpoints[$i]}" --server-pid "${pids[$i]}"); done
    else
        mapfile -t containers < <(jq -er '.containers[]' "$state")
        [[ ${#endpoints[@]} == 3 && ${#containers[@]} == 3 ]] || { echo 'Expected three containers/endpoints' >&2; exit 2; }
        for i in 0 1 2; do deployment+=(--endpoint "${endpoints[$i]}" --container "${containers[$i]}"); done
    fi
}

invoke() {
    local label="$1" operation="$2"; shift 2
    local output="$LOG_ROOT/$label.json"
    echo ">>> $label ($operation): warmup=$WARMUP measurement=$DURATION tail=$TAIL"
    if ! CROWDB_RUNTIME_ROOT="$LOG_ROOT/$label-runtime" "${BENCH_PREFIX[@]}" "$CLI" bench paxos-kv "$operation" \
        "${deployment[@]}" --label "$label" --warmup-secs "$WARMUP" \
        --duration-secs "$DURATION" --tail-secs "$TAIL" \
        --max-dataset-bytes "${KV_BENCH_MAX_DATASET_BYTES:-2147483648}" --output "$output" "$@" >"$LOG_ROOT/$label.stdout" 2>"$LOG_ROOT/$label.stderr"; then
        cat "$LOG_ROOT/$label.stdout" "$LOG_ROOT/$label.stderr" >&2
        [[ ! -f "$output" ]] || cat "$output" >&2
        return 1
    fi
    if ! jq -e '.passed == true' "$output" >/dev/null; then
        cat "$LOG_ROOT/$label.stdout" "$LOG_ROOT/$label.stderr" >&2
        echo "Missing or failed benchmark report: $output" >&2
        return 1
    fi
    jq -r '.history_line // "dataset prepared and verified"' "$output" | tee -a "$LOG_ROOT/history-comments.txt"
}

prepare_dataset() {
    local signature="$KV_BENCH_DEPLOYMENT/$KEYSPACE/$*"
    if [[ "$signature" != "$last_dataset" ]]; then
        invoke "prepare-$selected_cases" prepare --key-space "$KEYSPACE" --loader-num 32 "$@"
        last_dataset="$signature"
    fi
}

finish_cases() {
    for name in $CASES; do
        [[ " $known_cases " == *" $name "* ]] || { echo "Unknown case: $name" >&2; exit 2; }
    done
    [[ "$selected_cases" -gt 0 ]] || { echo 'No selected cases' >&2; exit 2; }
    echo "Reports: $LOG_ROOT; append dated results and conditions to the calling .sh history."
}
load_deployment "$KV_BENCH_DEPLOYMENT"
