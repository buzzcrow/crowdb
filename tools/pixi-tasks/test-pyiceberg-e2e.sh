#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

for scope in iceberg.metadata iceberg.namespace iceberg.catalog; do
    pixi run -e default -- python e2e-test/run.py --layer iceberg --scope "$scope" \
        --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" --concurrency 1 --node-cpus "${CROWDB_E2E_NODE_CPUS:-1}" --cpus "${CROWDB_E2E_CPUS:-8}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-8192}"
done
CROWDB_ICEBERG_E2E_PYTHON="$CONDA_PREFIX/bin/python" pixi run -e default -- cargo test --release \
    -p crowdb-access-server --features iceberg-e2e --test iceberg_namespace_sdk_test \
    official_complete_listing_rejects_each_spool_limit_and_releases_resources -- --ignored --exact
