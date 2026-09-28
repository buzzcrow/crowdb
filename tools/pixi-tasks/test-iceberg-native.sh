#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

source tools/pixi-tasks/prepare-iceberg.sh
pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_file_storage_test --test iceberg_gc_control_test \
    --test iceberg_gc_capacity_test --test iceberg_file_http_test -- --test-threads=1
pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_file_http_test native_file_ -- --ignored --test-threads=1
pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_commit_crash_test native_table_publication_recovers_before_and_after_every_durable_write \
    -- --ignored --exact --test-threads=1
