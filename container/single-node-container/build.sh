#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

mode=${1:-image}
[[ "$mode" == stage || "$mode" == image ]] || { echo 'Expected stage or image' >&2; exit 1; }
[[ $(uname -sm) == 'Linux x86_64' ]] || { echo 'Container artifacts require a Linux amd64 build host' >&2; exit 1; }
cd "$(git rev-parse --show-toplevel)"
for tool in patchelf strip ldd; do
    command -v "$tool" >/dev/null || { echo "Missing packaging tool: $tool" >&2; exit 1; }
done
if [[ "$mode" == image ]]; then
    docker info >/dev/null
fi

# Build on the host, reusing the existing Cargo, CMake and npm artifacts.
cargo build --locked --release -p crowdb-kv-client --features ffi
cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release
cmake --build app/crowdb-diskio/build -j 4 --target crowdb-diskio
cargo build --locked --release --features crowdb-kv-client/ffi \
    -p crowdb-kv-client -p crowdb-monitor -p crowdb-kv-server -p crowdb-diskdb \
    -p crowdb-chunkdb -p crowdb-chunk-kv-server \
    -p crowdb-access-server -p crowdb-web
(cd app/crowdb-web/ui && npm ci --prefer-offline && npm run build)

# Docker receives only the assembled runtime, never the source tree or data.
staging=$(mktemp -d "$PWD/target/container-runtime.XXXXXX")
trap 'rm -rf "$staging"' EXIT
bash container/single-node-container/collect-libs.sh "$staging"
cp -a app/crowdb-web/ui/dist "$staging/ui"
cp -a container/single-node-container/templates "$staging/templates"
cp container/single-node-container/{Dockerfile,profile.toml,entrypoint.sh} "$staging/"
git rev-parse HEAD > "$staging/SOURCE_REVISION"
cp VERSION "$staging/VERSION"
rm -rf target/container-runtime
mv "$staging" target/container-runtime
trap - EXIT
[[ "$mode" == image ]] || exit 0

proxy_args=()
for name in http_proxy https_proxy all_proxy no_proxy; do
    value="${!name:-}"
    if [[ "$value" == *'@'* ]]; then
        echo 'Credential-bearing proxy settings are not accepted by the image build' >&2
        exit 1
    fi
    if [[ "$name" != no_proxy && -n "$value" && "$value" != *://* ]]; then value="http://$value"; fi
    if [[ -n "$value" ]]; then proxy_args+=(--build-arg "$name=$value"); fi
done
DOCKER_BUILDKIT=1 docker build --platform linux/amd64 \
    "${proxy_args[@]}" \
    --build-arg SOURCE_REVISION="$(cat target/container-runtime/SOURCE_REVISION)" \
    --build-arg PREVIEW_VERSION="$(cat VERSION)" \
    --tag "${CROWDB_CONTAINER_IMAGE:-crowdb-iceberg-single-node:dev}" target/container-runtime
