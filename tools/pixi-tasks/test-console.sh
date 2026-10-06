#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "[test-console] macOS basic validation: console unit tests and server build"
    cargo build -p crowdb-kv-server
    cargo test -p crowdb-console-shared --lib
    cargo test -p crowdb-cli --tests
    exit 0
fi

pixi run clean-env
cargo build -p crowdb-kv-server
cargo test -p crowdb-console-shared --tests
cargo test -p crowdb-cli --tests
cargo build -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
cargo test -p crowdb-web --tests
