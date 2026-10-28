#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
case "${1:-image}" in
    stage) exec bash container/oci-image/stage.sh ;;
    image) exec bash container/oci-image/image-build.sh ;;
    *) echo 'Expected stage or image' >&2; exit 1 ;;
esac
