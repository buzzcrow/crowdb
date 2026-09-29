#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

source tools/pixi-tasks/prepare-iceberg.sh
revision=6976e020b894f6a6777704df2b8c4458cb291ae9
export CROWDB_ICEBERG_RCK_ROOT="${CROWDB_ICEBERG_RCK_ROOT:-${RUNNER_TEMP:-$PIXI_PROJECT_ROOT/target}/iceberg-rck}"
if [[ ! -e "$CROWDB_ICEBERG_RCK_ROOT" ]]; then
    mkdir -p "$CROWDB_ICEBERG_RCK_ROOT"
    git -C "$CROWDB_ICEBERG_RCK_ROOT" init
    git -C "$CROWDB_ICEBERG_RCK_ROOT" fetch --depth 1 https://github.com/apache/iceberg.git "$revision"
    git -C "$CROWDB_ICEBERG_RCK_ROOT" checkout --detach FETCH_HEAD
fi
[[ -d "$CROWDB_ICEBERG_RCK_ROOT/.git" ]] || {
    echo "Iceberg RCK source directory is not a checkout: $CROWDB_ICEBERG_RCK_ROOT" >&2
    exit 1
}
actual=$(git -C "$CROWDB_ICEBERG_RCK_ROOT" rev-parse HEAD)
[[ "$actual" == "$revision" ]] || {
    echo "Iceberg RCK requires $revision; found $actual at $CROWDB_ICEBERG_RCK_ROOT (checkout preserved)" >&2
    exit 1
}
pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_rck_test -- --ignored --test-threads=1
