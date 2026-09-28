#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cmake -S lib/crowdb-tree -B lib/crowdb-tree/build -DCMAKE_BUILD_TYPE=Release
cmake --build lib/crowdb-tree/build -j
cmake --build lib/crowdb-tree/build -j --target crowdb_rpc_tests
cmake -S lib/crowdb-rpc -B lib/crowdb-rpc/build -DCMAKE_BUILD_TYPE=Release
cmake --build lib/crowdb-rpc/build -j
cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
cmake --build app/crowdb-diskio/build -j
