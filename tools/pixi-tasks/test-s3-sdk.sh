#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
language=${1:?java, js or go}
mode=${2:-test}
case "$language" in java|js|go) ;; *) echo 'Expected java, js or go' >&2; exit 2 ;; esac
if [[ "$mode" != test ]]; then
    exec pixi run -e "s3-$language-sdk" -- bash e2e-test/clients/s3-sdk.sh "$language" "$mode"
fi
if [[ -n "${CROWDB_S3_E2E_ENDPOINT:-}" ]]; then
    pixi run -e "s3-$language-sdk" -- bash e2e-test/clients/s3-sdk.sh "$language" prepare
    exec pixi run -e "s3-$language-sdk" -- bash e2e-test/clients/s3-sdk.sh "$language" verify
fi
exec pixi run -e default -- python e2e-test/run.py --layer s3 --scope s3.sdk \
    --case "s3.sdk::$language" --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" \
    --concurrency 1 --node-cpus "${CROWDB_E2E_NODE_CPUS:-1}" --cpus "${CROWDB_E2E_CPUS:-8}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-8192}"
