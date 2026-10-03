#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
client=${1:?aws or rclone}
if [[ -n "${CROWDB_S3_E2E_ENDPOINT:-}" ]]; then
    exec python app/crowdb-access-server/tests/s3_e2e/clients.py "$client"
fi
case "$client" in
    aws) export CROWDB_S3_E2E_ONLY=test_aws_cli_workflow ;;
    rclone) export CROWDB_S3_E2E_ONLY=test_rclone_workflow ;;
    *) echo 'expected aws or rclone' >&2; exit 2 ;;
esac
pixi run -e default clean-env
exec bash tools/pixi-tasks/test-s3-e2e.sh
