#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run -e default -- cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
pixi run -e default -- cmake --build app/crowdb-diskio/build -j 4 --target crowdb-diskio
pixi run -e default -- cargo build -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
pixi run -e default clean-env
pixi run -e default test-access-server
CROWDB_RUNTIME_ROOT="$PIXI_PROJECT_ROOT/.crowdb-runtime/ephemeral/iceberg-e2e" CROWDB_ICEBERG_E2E_PYTHON="$PIXI_PROJECT_ROOT/.pixi/envs/iceberg-e2e/bin/python" pixi run -e default -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_full_stack_test -- --nocapture

CROWDB_ICEBERG_E2E_PYTHON="$CONDA_PREFIX/bin/python" pixi run -e default -- cargo test -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_namespace_sdk_test official_complete_listing_rejects_each_spool_limit_and_releases_resources -- --ignored --exact
CROWDB_ICEBERG_E2E_PYTHON="$CONDA_PREFIX/bin/python" pixi run -e default -- cargo test -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_gc_control_test official_sdk_foreground_progresses_under_gc_backlog -- --ignored --exact
