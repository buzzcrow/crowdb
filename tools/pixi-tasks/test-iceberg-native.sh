#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

if [[ -n "${CROWDB_TPC_LOADER_PYTHON:-}" ]]; then
    source tools/pixi-tasks/prepare-iceberg.sh
    pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
        --test iceberg_file_http_test tpc_loader_parallel_stress -- --ignored --exact --test-threads=1
fi
pixi run -e default -- cargo test --release -p crowdb-access-server \
    --test iceberg_commit_recovery_test --test iceberg_file_recovery_test -- --test-threads=1

pixi run -e default -- python e2e-test/run.py --layer iceberg --scope iceberg.files \
    --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" --concurrency 1 --cpus 8 --memory-mib 8192
pixi run -e default -- python e2e-test/run.py --layer iceberg --scope iceberg.gc \
    --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" --concurrency 1 --cpus 8 --memory-mib 8192
