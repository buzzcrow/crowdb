#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run test-cpp
pixi run test-unit
pixi run test-server
pixi run -e s3-e2e test-boto3-e2e
pixi run -e iceberg-e2e test-iceberg-e2e
pixi run -e iceberg-e2e test-iceberg-sdk
pixi run -e iceberg-e2e test-rust-iceberg-e2e
pixi run test-console
pixi run clean-env
pixi run test-console-ui
