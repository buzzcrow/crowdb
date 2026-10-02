#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
client=${1:?aws, rclone or fuse}
if [[ -n "${CROWDB_S3_E2E_ENDPOINT:-}" ]]; then
    if [[ "$client" == fuse ]]; then
        exec python app/crowdb-access-server/tests/s3_e2e/fuse_client.py
    fi
    exec python app/crowdb-access-server/tests/s3_e2e/clients.py "$client"
fi
case "$client" in
    aws) export CROWDB_S3_E2E_ONLY=test_aws_cli_workflow ;;
    rclone) export CROWDB_S3_E2E_ONLY=test_rclone_workflow ;;
    fuse) export CROWDB_S3_E2E_ONLY=test_s3fs_mounted_workflow ;;
    *) echo 'expected aws, rclone or fuse' >&2; exit 2 ;;
esac
pixi run clean-env
exec bash tools/pixi-tasks/test-s3-e2e.sh
