#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cargo build -p crowdb-kv-server -p crowdb-diskdb -p crowdb-cli
export CROWDB_KV_SERVER_BINARY=$(pwd)/target/debug/crowdb-kv-server
cd app/crowdb-web/ui
npm test
npx playwright test --config=e2e/realBackend.config.ts
