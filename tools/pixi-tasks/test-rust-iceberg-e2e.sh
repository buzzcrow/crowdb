#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
pixi run -e default -- python e2e-test/run.py --layer iceberg --scope iceberg.sdk.rust \
    --cpus 16 --memory-mib 16384
