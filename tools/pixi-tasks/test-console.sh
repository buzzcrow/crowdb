#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"

if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "[test-console] macOS basic validation: console unit tests and server build"
    cargo build -p crowdb-kv-server
    cargo test -p crowdb-console-shared --lib
    cargo test -p crowdb-cli --tests
    exit 0
fi

pixi run clean-env
pixi run build-cpp
pixi run install-ui-deps
(cd app/crowdb-web/ui && npm run build)
cargo build -p crowdb-web -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
# Complete simulated S3 clusters share host storage; run their lifecycle
# cases sequentially so concurrent device sync cannot starve Paxos leases.
cargo test -p crowdb-console-shared --tests -- --test-threads=1
cargo test -p crowdb-cli --tests
cargo test -p crowdb-web --tests

# These browser cases require distinct owned fixture phases.
CROWDB_NATIVE_PLAN_PREREQUISITES=1 \
    CROWDB_NATIVE_UI_E2E_GREP='native diagnostics: waiting plan resumes' \
    cargo test -p crowdb-web --test native_cluster_provisioning_test \
    one_rack_three_nodes_provision_all_services_without_metadata_repairs -- --exact --nocapture
CROWDB_NATIVE_JOURNAL_WINDOWS=1 \
    CROWDB_NATIVE_UI_E2E_GREP='native diagnostics: large Journal replaces' \
    cargo test -p crowdb-web --test native_cluster_provisioning_test \
    native_page_and_iceberg_inspection -- --exact --nocapture
CROWDB_NATIVE_COUNT_ACCEPTANCE=1 CROWDB_NATIVE_TRANSITION_ACCEPTANCE=1 \
    CROWDB_NATIVE_UI_E2E_GREP='native diagnostics: production split displays' \
    cargo test -p crowdb-web --test native_cluster_provisioning_test \
    one_rack_three_nodes_provision_all_services_without_metadata_repairs -- --exact --nocapture
