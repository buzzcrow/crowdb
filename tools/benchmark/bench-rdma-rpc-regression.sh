#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Shared coroutine workload, structured readiness and canonical result files.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
exec pixi run -- python tools/benchmark/rpc_regression.py --transport rdma "$@"
