#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cmake -S lib/crowdb-tree -B lib/crowdb-tree/build-asan \
  -DCMAKE_BUILD_TYPE=Debug -DCROWDB_TREE_SANITIZER=address \
  -DCROWDB_TREE_TEST_UTIL=ON -DCROWDB_RPC_TEST_UTIL=ON
# RPC is excluded from the default build, but its tests are registered in CTest.
cmake --build lib/crowdb-tree/build-asan --parallel --target crowdb_tree_tests crowdbcommon_tests crowdb_rpc_tests
if command -v setarch > /dev/null 2>&1; then
  setarch -R ctest --test-dir lib/crowdb-tree/build-asan --output-on-failure --stop-on-failure
else
  ctest --test-dir lib/crowdb-tree/build-asan --output-on-failure --stop-on-failure
fi
