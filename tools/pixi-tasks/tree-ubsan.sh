#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cmake -S lib/crowdb-tree -B lib/crowdb-tree/build-ubsan \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS_DEBUG="-O1 -g" \
  -DCROWDB_TREE_SANITIZER=undefined \
  -DCROWDB_TREE_TEST_UTIL=ON -DCROWDB_RPC_TEST_UTIL=ON
# RPC tests are registered with CTest but excluded from the default build.
cmake --build lib/crowdb-tree/build-ubsan --parallel 4 \
  --target crowdb_tree_tests crowdbcommon_tests crowdb_rpc_tests
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
if command -v setarch > /dev/null 2>&1; then
  setarch -R ctest --test-dir lib/crowdb-tree/build-ubsan --output-on-failure --stop-on-failure
else
  ctest --test-dir lib/crowdb-tree/build-ubsan --output-on-failure --stop-on-failure
fi
