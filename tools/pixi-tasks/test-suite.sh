#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run test-cpp
pixi run test-core
pixi run test-storage
pixi run test-access
pixi run -e iceberg-e2e test-iceberg-sdk-component
pixi run test-console
pixi run test-console-ui
pixi run clean-env
# One canonical execution for required server domains and isolation pairs.
pixi run test-container-e2e --layer all --require-isolation \
    --concurrency "${CROWDB_E2E_CONCURRENCY:-2}" \
    --node-cpus "${CROWDB_E2E_NODE_CPUS:-0.25}" \
    --cpus "${CROWDB_E2E_CPUS:-16}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-32768}"
