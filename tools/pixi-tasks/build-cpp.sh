#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cmake -S lib/crowdb-tree -B lib/crowdb-tree/build -DCMAKE_BUILD_TYPE=Release -DCROWDB_TREE_TEST_UTIL=ON
cmake --build lib/crowdb-tree/build -j32
cmake --build lib/crowdb-tree/build -j32 --target crowdb_rpc_tests
cmake -S lib/crowdb-rpc -B lib/crowdb-rpc/build -DCMAKE_BUILD_TYPE=Release -DCROWDB_RPC_TEST_UTIL=ON
cmake --build lib/crowdb-rpc/build -j32
cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
cmake --build app/crowdb-diskio/build -j32
