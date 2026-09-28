#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cargo build --release --workspace --exclude crowdb-kv-client
cargo build --release -p crowdb-kv-client --features ffi
cd app/crowdb-web/ui
npm run build
