#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

# Shared retained-artifact lifecycle for regression sentinels.
# The caller sets REGRESSION_LOG_ROOT before sourcing this file.

if [ -z "${REGRESSION_LOG_ROOT:-}" ]; then
    echo "ERROR: set REGRESSION_LOG_ROOT before sourcing bench-regression-common.sh" >&2
    exit 2
fi

REGRESSION_CONFIG="${REGRESSION_CONFIG:-$REGRESSION_LOG_ROOT/console.toml}"
REGRESSION_CLI="${REGRESSION_CLI:-./target/release/crowdb-cli}"
export REGRESSION_CLI

regression_init() {
    mkdir -p "$REGRESSION_LOG_ROOT"
    if [ -z "${CROWDB_REGRESSION_FIXTURE_BIN:-}" ]; then
        pixi run cargo build --release -p crowdb-console-shared --example regression-fixture --features test-util
    fi
}

regression_cli() {
    regression_client_for "$REGRESSION_CONFIG" "$@"
}

regression_client_for() {
    local state="$1"
    shift
    REGRESSION_CLI="$REGRESSION_CLI" pixi run python tools/benchmark/fixture.py \
        --state "$state" client -- "$@"
}

regression_fixture_for() {
    local state="$1"
    shift
    pixi run python tools/benchmark/fixture.py --state "$state" --owner-pid "$$" "$@"
}

regression_fixture() {
    regression_fixture_for "$REGRESSION_CONFIG" "$@"
}

regression_fixture_log_root() {
    pixi run python tools/benchmark/fixture.py --state "$REGRESSION_CONFIG" root
}

regression_destroy() {
    if [ -f "$REGRESSION_CONFIG" ]; then
        regression_fixture stop
    fi
}

# Wipe each listed KV data group, then restart the auxiliary storage stack once.
regression_reset_stack() {
    local groups=("$@") index last
    if [ "${#groups[@]}" -eq 0 ]; then
        echo "ERROR: regression_reset_stack requires at least one KV group" >&2
        return 2
    fi
    last=$((${#groups[@]} - 1))
    for index in "${!groups[@]}"; do
        if [ "$index" -eq "$last" ]; then
            regression_fixture reset --store 0 --group "${groups[$index]}" --restart-storage
        else
            regression_fixture reset --store 0 --group "${groups[$index]}"
        fi
    done
}

regression_require_metric_section() {
    local file="$1" section="$2"
    [ -s "$file" ] && grep -Eq -- "^${section}$" "$file"
}

regression_require_metric_counter() {
    local file="$1" pattern="$2"
    [ -s "$file" ] && grep -Eq -- "$pattern" "$file"
}

regression_require_metric_files() {
    local name_pattern="$1"
    shift
    local found=0 file section
    while IFS= read -r -d '' file; do
        found=$((found + 1))
        for section in "$@"; do
            if ! regression_require_metric_section "$file" "$section"; then
                echo "ERROR: metrics file $file lacks section $section" >&2
                return 1
            fi
        done
    done < <(find "$REGRESSION_LOG_ROOT" -type f -name "$name_pattern" -print0)
    if [ "$found" -eq 0 ]; then
        echo "ERROR: no metrics files matching $name_pattern" >&2
        return 1
    fi
}

regression_require_no_service_errors() {
    local root status=0
    root=$(regression_fixture_log_root) || return 1
    grep -ERn --include='*.log' -- 'fd .*not registered|DiskNotExist' "$root/services" || status=$?
    # One means no log matched. Matching errors and scan failures both fail.
    if [ "$status" -ne 1 ]; then
        echo "ERROR: service log validation failed (grep status $status)" >&2
        return 1
    fi
}
