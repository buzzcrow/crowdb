#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
runtime_root="${CROWDB_RUNTIME_ROOT:-$repo_root/.crowdb-runtime}"
mode="${1:-env}"

case "$mode" in
    env | all-disposable) ;;
    *)
        echo "usage: $0 [env|all-disposable]" >&2
        exit 2
        ;;
esac

if [ ! -d "$runtime_root" ]; then
    echo "[clean-runtime] runtime root does not exist: $runtime_root"
    exit 0
fi
runtime_root=$(cd "$runtime_root" && pwd -P)
if [ "$runtime_root" = "/" ] || [ "$runtime_root" = "$repo_root" ]; then
    echo "[clean-runtime] refusing unsafe runtime root: $runtime_root" >&2
    exit 2
fi

process_matches() {
    local pid="$1" expected_start="$2" stat observed_start
    [ -n "$pid" ] && [ "$pid" != "$$" ] || return 1
    if [ -r "/proc/$pid/stat" ]; then
        stat=$(cat "/proc/$pid/stat" 2>/dev/null || true)
        [ -n "$stat" ] || return 1
        observed_start=$(printf '%s\n' "$stat" | sed 's/^.*) //' | awk '{print $20}')
        [ "$observed_start" = "$expected_start" ]
        return
    fi
    [ "$expected_start" = "process-$pid" ] && kill -0 "$pid" 2>/dev/null
}

terminate_recorded_processes() {
    local ephemeral="$runtime_root/ephemeral"
    [ -d "$ephemeral" ] || return 0
    while IFS= read -r -d '' manifest; do
        while IFS=$'\t' read -r pid expected_start; do
            process_matches "$pid" "$expected_start" || continue
            kill -TERM "$pid" 2>/dev/null || true
        done < <(jq -r '.processes[]? | [.pid, .start] | @tsv' "$manifest" 2>/dev/null || true)
    done < <(find "$ephemeral" -type f -name namespace.json -print0)

    for _ in 1 2 3 4 5; do
        local any_alive=0
        while IFS= read -r -d '' manifest; do
            while IFS=$'\t' read -r pid expected_start; do
                process_matches "$pid" "$expected_start" && any_alive=1
            done < <(jq -r '.processes[]? | [.pid, .start] | @tsv' "$manifest" 2>/dev/null || true)
        done < <(find "$ephemeral" -type f -name namespace.json -print0)
        [ "$any_alive" -eq 0 ] && break
        sleep 0.1
    done

    while IFS= read -r -d '' manifest; do
        while IFS=$'\t' read -r pid expected_start; do
            process_matches "$pid" "$expected_start" && kill -KILL "$pid" 2>/dev/null || true
        done < <(jq -r '.processes[]? | [.pid, .start] | @tsv' "$manifest" 2>/dev/null || true)
    done < <(find "$ephemeral" -type f -name namespace.json -print0)
}

prune_ephemeral_claims() {
    local registry="$runtime_root/ports/claims.json"
    [ -f "$registry" ] || return 0
    local replacement="$registry.clean-$$"
    if command -v flock >/dev/null 2>&1; then
        exec 9<>"$registry"
        flock 9
        jq '[.[] | select(.mode == "persistent")]' "$registry" >"$replacement"
        cat "$replacement" >"$registry"
        rm -f "$replacement"
        flock -u 9
        exec 9>&-
    else
        jq '[.[] | select(.mode == "persistent")]' "$registry" >"$replacement"
        cat "$replacement" >"$registry"
        rm -f "$replacement"
    fi
}

terminate_recorded_processes
rm -rf "$runtime_root/ephemeral"
prune_ephemeral_claims

if [ "$mode" = "all-disposable" ]; then
    rm -rf "$runtime_root/artifacts"
fi

echo "[clean-runtime] removed disposable runtime state; preserved $runtime_root/persistent"
