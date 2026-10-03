#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run -e default build-cpp
pixi run -e default -- cargo build -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
pixi run -e default test-access-s3
pixi run -e default test-access-server
CROWDB_S3_E2E_PYTHON="$PIXI_PROJECT_ROOT/.pixi/envs/s3-e2e/bin/python" pixi run -e default -- cargo test -p crowdb-access-server --test s3_copy_body_test official_boto3 -- --include-ignored
CROWDB_S3_E2E_PYTHON="$PIXI_PROJECT_ROOT/.pixi/envs/s3-e2e/bin/python" pixi run -e default -- cargo test -p crowdb-access-server --features s3-e2e --test s3_full_stack_test -- --nocapture
