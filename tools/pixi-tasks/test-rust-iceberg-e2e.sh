#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

source tools/pixi-tasks/prepare-iceberg.sh
pixi run -e default -- cargo build --locked --manifest-path \
    app/crowdb-access-server/tests/common/iceberg_rust/Cargo.toml
export CROWDB_ICEBERG_RUST_CLIENT_BIN="$PIXI_PROJECT_ROOT/app/crowdb-access-server/tests/common/iceberg_rust/target/debug/crowdb-iceberg-rust-client-fixture"
test -x "$CROWDB_ICEBERG_RUST_CLIENT_BIN"
pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_rust_sdk_test --test iceberg_rust_retired_sdk_test -- --ignored --test-threads=1
