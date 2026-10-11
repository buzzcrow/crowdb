#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

# Deterministic HTTP protocol peers; no production server binaries or image.
export CROWDB_ICEBERG_E2E_PYTHON="${CONDA_PREFIX:?}/bin/python"
export CROWDB_ICEBERG_E2E_MVN="$CONDA_PREFIX/bin/mvn"
export JAVA_HOME="${JAVA_HOME:?run in the iceberg-e2e environment}"
pixi run -e iceberg-e2e -- mvn --batch-mode --no-transfer-progress \
    -f e2e-test/clients/iceberg-java/pom.xml dependency:go-offline compile exec:help
pixi run -e default -- cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_namespace_sdk_test --test iceberg_table_sdk_test \
    --test iceberg_commit_sdk_test --test iceberg_java_response_loss_test \
    -- --ignored --test-threads=1
