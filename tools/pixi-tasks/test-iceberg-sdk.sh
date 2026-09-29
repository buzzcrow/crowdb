#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

pixi run -e iceberg-e2e test-java-iceberg-e2e
pixi run -e iceberg-e2e test-iceberg-rck
