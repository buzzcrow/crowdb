#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

cmake -S lib/crowdb-tree -B lib/crowdb-tree/build-tsan -DCMAKE_BUILD_TYPE=Debug -DCROWDB_TREE_SANITIZER=thread
cmake --build lib/crowdb-tree/build-tsan -j
if command -v setarch > /dev/null 2>&1; then
  setarch -R ctest --test-dir lib/crowdb-tree/build-tsan --output-on-failure
else
  ctest --test-dir lib/crowdb-tree/build-tsan --output-on-failure
fi
