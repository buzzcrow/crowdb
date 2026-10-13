#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
if (( $# == 0 )); then
    set -- test-cpp test-core test-storage test-access test-access-sdk test-console test-console-ui
fi
exec python3 tools/test-metrics/measure.py "$@"
