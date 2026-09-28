#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run -e default -- cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
pixi run -e default -- cmake --build app/crowdb-diskio/build -j 4 --target crowdb-diskio
pixi run -e default -- cargo build --release -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
CROWDB_RUNTIME_ROOT="$PIXI_PROJECT_ROOT/.crowdb-runtime/ephemeral/iceberg-java-e2e" pixi run -e default clean-env
CROWDB_RUNTIME_ROOT="$PIXI_PROJECT_ROOT/.crowdb-runtime/ephemeral/iceberg-java-e2e" CROWDB_ICEBERG_E2E_MVN="$CONDA_PREFIX/bin/mvn" JAVA_HOME="$CONDA_PREFIX/lib/jvm" pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test official_java_ -- --ignored --nocapture --test-threads=1
