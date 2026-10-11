#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

for package in \
    crowdb-access-multipart \
    crowdb-access-s3 \
    crowdb-access-iceberg \
    crowdb-access-server \
    crowdb-access-dataset \
    crowdb-monitor; do
    cargo test -p "$package" --tests
done

# The SDK-only case is part of this task, with its pinned Python environment.
pixi run -e s3-e2e test-boto3-copy-error
