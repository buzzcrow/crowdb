#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cargo build -p crowdb-web -p crowdb-kv-server -p crowdb-diskdb -p crowdb-cli -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
cmake --build app/crowdb-diskio/build -j 4
pixi run -e iceberg-e2e python -c "import pyarrow; import pyiceberg"
export CROWDB_KV_SERVER_BINARY=$(pwd)/target/debug/crowdb-kv-server
export CROWDB_WEB_BINARY=$(pwd)/target/debug/crowdb-web
cd app/crowdb-web/ui
npm test
npx playwright test --config=e2e/realBackend.config.ts
