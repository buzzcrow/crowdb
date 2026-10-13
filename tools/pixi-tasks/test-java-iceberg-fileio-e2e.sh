#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
pixi run -e default -- python e2e-test/run.py --layer iceberg --scope iceberg.sdk.java \
    --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" --node-cpus "${CROWDB_E2E_NODE_CPUS:-1}" --cpus "${CROWDB_E2E_CPUS:-16}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-16384}"
