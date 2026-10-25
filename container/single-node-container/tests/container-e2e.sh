#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

exec pixi run test-container-e2e --layer deployment \
    --image "${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}" \
    --cpus "${CROWDB_E2E_CPUS:-4}" \
    --memory-mib "${CROWDB_E2E_MEMORY_MIB:-8192}" "$@"
