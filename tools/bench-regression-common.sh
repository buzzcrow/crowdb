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

regression_init() {
    mkdir -p "$REGRESSION_LOG_ROOT"
}

regression_cli() {
    pixi run -- "$REGRESSION_CLI" --log-root "$REGRESSION_LOG_ROOT" \
        --config "$REGRESSION_CONFIG" "$@"
}

regression_destroy() {
    if [ -f "$REGRESSION_CONFIG" ]; then
        timeout 30 pixi run -- "$REGRESSION_CLI" --log-root "$REGRESSION_LOG_ROOT" \
            --config "$REGRESSION_CONFIG" cluster destroy || true
    fi
}

regression_require_metric_section() {
    local file="$1" section="$2"
    [ -s "$file" ] && rg -q "^${section}$" "$file"
}

regression_require_metric_counter() {
    local file="$1" pattern="$2"
    [ -s "$file" ] && rg -q "$pattern" "$file"
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
