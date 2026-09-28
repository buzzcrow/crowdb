#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cmake -S lib/crowdb-tree -B lib/crowdb-tree/build-ubsan -DCMAKE_BUILD_TYPE=Debug -DCROWDB_TREE_SANITIZER=undefined
cmake --build lib/crowdb-tree/build-ubsan -j
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
if command -v setarch > /dev/null 2>&1; then
  setarch -R ctest --test-dir lib/crowdb-tree/build-ubsan --output-on-failure
else
  ctest --test-dir lib/crowdb-tree/build-ubsan --output-on-failure
fi
