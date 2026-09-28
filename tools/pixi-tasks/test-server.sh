#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run clean-env
pixi run test-kv-server
pixi run test-diskdb
pixi run test-diskdb-client
pixi run test-chunkdb
pixi run test-chunk-client
pixi run test-diskio-client
pixi run test-access-server
pixi run test-monitor
