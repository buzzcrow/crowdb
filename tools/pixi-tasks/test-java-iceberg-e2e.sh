#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run -e iceberg-e2e -- mvn --batch-mode --no-transfer-progress \
    -f e2e-test/clients/iceberg-java/pom.xml \
    dependency:go-offline compile exec:help
pixi run -e default -- env CROWDB_ICEBERG_E2E_MVN="${CONDA_PREFIX:?}/bin/mvn" JAVA_HOME="${JAVA_HOME:?run this task in the iceberg-e2e environment}" cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_namespace_sdk_test official_catalog_continues_through_empty_namespace_pages -- --ignored --exact
pixi run -e default -- env CROWDB_ICEBERG_E2E_MVN="${CONDA_PREFIX:?}/bin/mvn" JAVA_HOME="${JAVA_HOME:?run this task in the iceberg-e2e environment}" cargo test --release -p crowdb-access-server --features iceberg-e2e \
    --test iceberg_table_sdk_test --test iceberg_commit_sdk_test \
    --test iceberg_java_response_loss_test -- --ignored --test-threads=1
pixi run -e iceberg-e2e test-java-iceberg-fileio-e2e
