#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
case "${1:-image}" in
    stage) exec bash container/oci-image/stage.sh ;;
    image)
        bash container/oci-image/image-build.sh
        pixi run --manifest-path container/oci-image/pixi.toml import "$PWD/target/crowdb.oci.tar" --image "${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}"
        ;;
    *) echo 'Expected stage or image' >&2; exit 1 ;;
esac
