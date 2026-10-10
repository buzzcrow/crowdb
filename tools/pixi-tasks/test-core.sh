#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

for package in \
    crowdb-e2e \
    crowdb-common \
    crowdb-test-harness \
    crowdb-protocol \
    crowdb-kv \
    crowdb-kv-client; do
    cargo test -p "$package" --tests
done
