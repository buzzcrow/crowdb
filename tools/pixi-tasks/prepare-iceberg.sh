#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

# Source from the pinned iceberg-e2e environment.
export CROWDB_ICEBERG_E2E_PYTHON="$CONDA_PREFIX/bin/python"
export CROWDB_ICEBERG_E2E_MVN="$CONDA_PREFIX/bin/mvn"
export JAVA_HOME="$CONDA_PREFIX/lib/jvm"
pixi run -e default -- cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
pixi run -e default -- cmake --build app/crowdb-diskio/build -j 4 --target crowdb-diskio
pixi run -e default -- cargo build --release -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
pixi run -e default clean-env
