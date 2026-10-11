#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run -e default -- cargo test --release -p crowdb-access-server \
    --test iceberg_commit_recovery_test --test iceberg_file_recovery_test -- --test-threads=1

pixi run -e default -- python e2e-test/run.py --layer iceberg --scope iceberg.files \
    --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" --concurrency 1 --node-cpus "${CROWDB_E2E_NODE_CPUS:-0.5}" --cpus "${CROWDB_E2E_CPUS:-8}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-8192}"
pixi run -e default -- python e2e-test/run.py --layer iceberg --scope iceberg.gc \
    --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" --concurrency 1 --node-cpus "${CROWDB_E2E_NODE_CPUS:-0.5}" --cpus "${CROWDB_E2E_CPUS:-8}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-8192}"
