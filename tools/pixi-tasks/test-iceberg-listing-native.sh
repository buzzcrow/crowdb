#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
source tools/pixi-tasks/prepare-iceberg.sh
pixi run -e default cargo test --release -p crowdb-access-server --features iceberg-e2e --test iceberg_file_http_test listing:: -- --include-ignored --test-threads=1
