#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

ctest --test-dir lib/crowdb-tree/build --output-on-failure
lib/crowdb-tree/build/crowdb-common-build/crowdbcommon_tests
ctest --test-dir lib/crowdb-rpc/build --output-on-failure
ctest --test-dir app/crowdb-diskio/build --output-on-failure
cargo test -p crowdb-tree-ffi --tests
cargo test -p crowdb-rpc-ffi --tests
