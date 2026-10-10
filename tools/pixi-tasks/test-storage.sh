#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cargo test -p crowdb-chunkdb-client --tests
cargo test -p crowdb-chunk-kv --tests
cargo test -p crowdb-chunk-kv-client --tests
# Root-catalog transfer tests start a KV cluster through the test harness.
cargo build -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
cargo test -p crowdb-chunk-kv-server --tests
cargo test -p crowdb-chunk-stream --tests
cargo test -p crowdb-kv-server --tests
cargo test -p crowdb-diskdb --tests
cargo test -p crowdb-diskdb-client --tests
cargo test -p crowdb-chunkdb --tests
cargo test -p crowdb-chunk-client --tests
cargo test -p crowdb-diskio-client --tests
