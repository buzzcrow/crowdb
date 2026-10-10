#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
[[ $(uname -sm) == 'Linux x86_64' ]] || { echo 'Linux amd64 builder required; macOS Linux-VM backend is reserved' >&2; exit 1; }
output="$PWD/target/crowdb.oci.tar"
args=()
preflight_args=()
while (($#)); do
    case "$1" in
        --output) output=$(realpath -m "${2:?output path required}"); shift 2 ;;
        --staged) staged=true; shift ;;
        --platform)
            [[ ${2:?platform required} == linux/amd64 ]] || { echo 'Only linux/amd64 is supported' >&2; exit 1; }
            args+=("--platform" "$2"); shift 2 ;;
        --cache) args+=("--cache" "$(realpath -m "${2:?cache path required}")"); shift 2 ;;
        --privileged) args+=("$1"); preflight_args+=("$1"); shift ;;
        *) args+=("$1"); shift ;;
    esac
done
pixi run --manifest-path container/oci-image/pixi.toml python "$PWD/container/oci-image/preflight.py" "${preflight_args[@]}"
if [[ "${staged:-false}" != true ]]; then bash container/oci-image/stage.sh; fi
exec pixi run --manifest-path container/oci-image/pixi.toml build \
    --context "$PWD/target/container-runtime" --output "$output" "${args[@]}"
