#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
language=${1:?java, js or go}
mode=${2:-test}
case "$language" in java|js|go) ;; *) echo 'Expected java, js or go' >&2; exit 2 ;; esac
client="app/crowdb-access-server/tests/common/s3_sdks/$language"
if [[ "$mode" == prepare ]]; then
    case "$language" in
        java) cd "$client"; mvn -q --batch-mode package dependency:build-classpath -Dmdep.outputFile=target/classpath ;;
        js) cd "$client"; npm ci --ignore-scripts; npm run build ;;
        go) cd "$client"; go build -mod=readonly -o sdk-check . ;;
    esac
    exit
fi
if [[ "$mode" == verify ]]; then
    CROWDB_S3_SDK_FAULT= timeout --signal=TERM --kill-after=30s 300s \
        pixi run -e "s3-$language-sdk" -- bash tools/pixi-tasks/test-s3-sdk.sh "$language" run
    status=0
    CROWDB_S3_SDK_FAULT=after-mpu timeout --signal=TERM --kill-after=30s 300s \
        pixi run -e "s3-$language-sdk" -- bash tools/pixi-tasks/test-s3-sdk.sh "$language" run || status=$?
    # Clients return 42 only for the injected failure after successful cleanup.
    [[ "$status" == 42 ]] || { echo "$language: injected failure/cleanup failed (exit $status)" >&2; exit 1; }
    echo "$language: injected failure cleanup verified"
    exit
fi
if [[ "$mode" == run ]]; then
    export CROWDB_S3_E2E_ACCESS_KEY=${CROWDB_S3_E2E_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:?Supply test credentials}}
    export CROWDB_S3_E2E_SECRET_KEY=${CROWDB_S3_E2E_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:?Supply test credentials}}
    : "${CROWDB_S3_E2E_ENDPOINT:?Supply a test endpoint}"
    cd "$client"
    case "$language" in
        java) exec java -cp "target/classes:$(cat target/classpath)" TestS3 ;;
        js) exec node dist/test.js ;;
        go) exec ./sdk-check ;;
    esac
fi
[[ "$mode" == test ]] || { echo 'Expected test, prepare, verify or run' >&2; exit 2; }
pixi run -e "s3-$language-sdk" -- bash tools/pixi-tasks/test-s3-sdk.sh "$language" prepare
if [[ -n "${CROWDB_S3_E2E_ENDPOINT:-}" ]]; then
    exec bash tools/pixi-tasks/test-s3-sdk.sh "$language" verify
fi
export CROWDB_RUNTIME_ROOT="$PIXI_PROJECT_ROOT/.crowdb-runtime/ephemeral/s3-$language-sdk"
pixi run -e default clean-env
trap 'pixi run -e default clean-env' EXIT
pixi run -e default build-cpp
pixi run -e default -- cargo build -p crowdb-web -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb -p crowdb-chunk-kv-server -p crowdb-access-server
CROWDB_S3_E2E_SDK="$language" pixi run -e default -- cargo test -p crowdb-access-server --features s3-e2e --test s3_full_stack_test -- --nocapture
