#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

# Real server and browser flows belong to the root container runner.
cargo test -p crowdb-console-shared --tests
cargo test -p crowdb-cli --tests
cargo test -p crowdb-web --tests
