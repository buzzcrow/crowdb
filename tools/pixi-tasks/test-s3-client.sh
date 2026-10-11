#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
client=${1:?aws or rclone}
case "$client" in
    aws) method=test_aws_cli_workflow ;;
    rclone) method=test_rclone_workflow ;;
    *) echo 'expected aws or rclone' >&2; exit 2 ;;
esac
if [[ -n "${CROWDB_S3_E2E_ENDPOINT:-}" ]]; then
    exec pixi run -e s3-e2e -- python e2e-test/clients/s3/clients.py "$client"
fi
exec pixi run -e default -- python e2e-test/run.py --layer s3 --scope s3.protocol \
    --case "s3.protocol::$method" --image "${CROWDB_E2E_IMAGE:-crowdb-node:dev}" \
    --concurrency 1 --node-cpus "${CROWDB_E2E_NODE_CPUS:-0.5}" --cpus "${CROWDB_E2E_CPUS:-8}" --memory-mib "${CROWDB_E2E_MEMORY_MIB:-8192}"
