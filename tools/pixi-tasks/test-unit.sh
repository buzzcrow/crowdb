#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run test-common
pixi run test-harness
pixi run test-protocol
pixi run test-kv-core
pixi run test-kv-client
pixi run test-chunkdb-client
pixi run test-chunk-kv
pixi run test-chunk-stream
pixi run test-chunk-kv-client
pixi run test-chunk-kv-server
pixi run test-access-iceberg
