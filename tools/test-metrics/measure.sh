#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
if (( $# == 0 )); then
    set -- test-tree-ct test-common-ct test-tree-ffi test-rpc-ct test-rpc-ffi test-diskio-ct \
        test-common test-protocol test-kv-core test-kv-client test-chunkdb-client \
        test-chunk-kv test-chunk-stream test-chunk-kv-client test-chunk-kv-server \
        test-kv-server test-diskdb test-diskdb-client test-chunkdb test-chunk-client \
        test-diskio-client test-access-s3 test-access-iceberg test-access-server test-monitor \
        test-console-shared test-console-cli test-console-server test-console-ui \
        test-boto3-e2e test-pyiceberg-e2e test-iceberg-native \
        test-java-iceberg-e2e test-rust-iceberg-e2e test-iceberg-rck
fi
exec python3 tools/test-metrics/measure.py "$@"
