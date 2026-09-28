#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

git config core.hooksPath .githooks
pixi run install-ui-deps
cargo install cargo-tarpaulin || echo "Warning: cargo-tarpaulin install failed (expected on macOS)"
cargo install --locked samply || echo "Warning: samply install failed"
cargo install --locked inferno || echo "Warning: inferno install failed"
