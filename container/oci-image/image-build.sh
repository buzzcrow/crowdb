#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
[[ $(uname -sm) == 'Linux x86_64' ]] || { echo 'Linux amd64 builder required; macOS Linux-VM backend is reserved' >&2; exit 1; }
output="$PWD/target/crowdb.oci.tar"
load_image=${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}
args=()
preflight_args=()
while (($#)); do
    case "$1" in
        --output) output=$(realpath -m "${2:?output path required}"); shift 2 ;;
        --staged) staged=true; shift ;;
        --load) load_image=${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}; shift ;;
        --no-load) load_image=; shift ;;
        --load-image) load_image=${2:?image reference required}; shift 2 ;;
        --platform)
            [[ ${2:?platform required} == linux/amd64 ]] || { echo 'Only linux/amd64 is supported' >&2; exit 1; }
            args+=("--platform" "$2"); shift 2 ;;
        --cache) args+=("--cache" "$(realpath -m "${2:?cache path required}")"); shift 2 ;;
        --privileged) args+=("$1"); preflight_args+=("$1"); shift ;;
        *) args+=("$1"); shift ;;
    esac
done
if [[ -n $load_image ]]; then
    command -v docker >/dev/null || { echo 'Docker loading requires docker; use --no-load for an OCI archive only' >&2; exit 1; }
    docker info --format '{{.ID}}' >/dev/null
fi
pixi run --manifest-path container/oci-image/pixi.toml python "$PWD/container/oci-image/preflight.py" --setup-apparmor "${preflight_args[@]}"
if [[ "${staged:-false}" != true ]]; then bash container/oci-image/stage.sh; fi
pixi run --manifest-path container/oci-image/pixi.toml build \
    --context "$PWD/target/container-runtime" --output "$output" "${args[@]}"
if [[ -n ${load_image:-} ]]; then
    pixi run --manifest-path container/oci-image/pixi.toml import "$output" --image "$load_image"
    echo "[oci-build] loaded $load_image into Docker; archive: $output"
else
    echo "[oci-build] wrote OCI archive: $output"
fi
