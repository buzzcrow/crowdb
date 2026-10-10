#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

CROWDB_S3_E2E_PYTHON="$CONDA_PREFIX/bin/python" pixi run -e default -- cargo test \
    -p crowdb-access-server --test s3_copy_body_test \
    official_boto3_recognizes_a_copy_error_after_http_200_and_keepalives \
    -- --ignored --exact
