#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

# GCC cannot instrument the atomic fences in Folly. Use the pinned pixi Clang
# and a separate cache so an earlier GCC build cannot retain its toolchain.
# Optimize sanitizer instrumentation while retaining Debug assertions.
build_dir=lib/crowdb-tree/build-tsan-clang23
cmake -S lib/crowdb-tree -B "$build_dir" \
  -DCMAKE_C_COMPILER="${CONDA_PREFIX:?}/bin/clang-23" \
  -DCMAKE_CXX_COMPILER="$CONDA_PREFIX/bin/clang++-23" \
  -DCMAKE_BUILD_TYPE=Debug -DCROWDB_TREE_SANITIZER=thread \
  -DCMAKE_C_FLAGS_DEBUG='-O1 -g' -DCMAKE_CXX_FLAGS_DEBUG='-O1 -g' \
  -DCROWDB_TREE_TEST_UTIL=ON -DCROWDB_RPC_TEST_UTIL=ON
# RPC is excluded from the default build, but its tests are registered in CTest.
cmake --build "$build_dir" --parallel --target crowdb_tree_tests crowdbcommon_tests crowdb_rpc_tests
if command -v setarch > /dev/null 2>&1; then
  setarch -R ctest --test-dir "$build_dir" --output-on-failure --stop-on-failure
else
  ctest --test-dir "$build_dir" --output-on-failure --stop-on-failure
fi
