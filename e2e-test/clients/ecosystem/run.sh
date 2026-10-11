#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
case "${CROWDB_ECOSYSTEM_PROFILE:-all}" in
    all) profile=ecosystem; memory=10240 ;;
    python) profile=ecosystem-python; memory=6144 ;;
    *) echo 'Expected CROWDB_ECOSYSTEM_PROFILE=all or python' >&2; exit 2 ;;
esac
arguments=(--profile "$profile" --concurrency 1 --node-cpus 1 --cpus 4
    --memory-mib "$memory" --image "${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}")
if [[ -n "${CROWDB_ECOSYSTEM_ARTIFACTS:-}" ]]; then
    arguments+=(--artifacts "$CROWDB_ECOSYSTEM_ARTIFACTS")
fi
exec pixi run python e2e-test/run.py "${arguments[@]}" "$@"
