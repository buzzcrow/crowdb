#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run test-tree-ct
pixi run test-common-ct
pixi run test-rpc-ct
pixi run test-diskio-ct
pixi run test-tree-ffi
pixi run test-rpc-ffi
