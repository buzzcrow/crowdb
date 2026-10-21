#!/bin/bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
[[ $(uname -sm) == "Linux x86_64" ]] || { echo "Linux amd64 build host required; macOS VM backend is reserved" >&2; exit 1; }
cd "$(git rev-parse --show-toplevel)"
source_tree=$(python container/oci-image/source_tree.py)
case "${CROWDB_KEEP_DEBUG_INFO:-0}" in
    0) cmake_debug_args=() ;;
    1)
        export CARGO_PROFILE_RELEASE_DEBUG=2
        cmake_debug_args=(-DCMAKE_CXX_FLAGS_RELEASE='-O3 -DNDEBUG -g')
        ;;
    *) echo 'CROWDB_KEEP_DEBUG_INFO must be 0 or 1' >&2; exit 1 ;;
esac
# Build on the host, reusing the existing Cargo, CMake and npm artifacts.
cargo build --locked --release -p crowdb-kv-client --features ffi
cmake -S app/crowdb-diskio -B app/crowdb-diskio/build -DCMAKE_BUILD_TYPE=Release "${cmake_debug_args[@]}"
cmake --build app/crowdb-diskio/build -j 4 --target crowdb-diskio
cargo build --locked --release --features crowdb-kv-client/ffi \
    -p crowdb-kv-client -p crowdb-monitor -p crowdb-kv-server -p crowdb-diskdb \
    -p crowdb-chunkdb -p crowdb-chunk-kv-server \
    -p crowdb-access-server -p crowdb-web
(cd app/crowdb-web/ui && npm ci --prefer-offline && npm run build)
if [[ $(python container/oci-image/source_tree.py) != "$source_tree" ]]; then
    echo 'Source tree changed while building; rebuild the shared OCI image' >&2
    exit 1
fi

# Docker receives only the assembled runtime, never the source tree or data.
staging=$(mktemp -d "$PWD/target/container-runtime.XXXXXX")
trap 'rm -rf "$staging"' EXIT
bash container/oci-image/collect-libs.sh "$staging"
cp -a app/crowdb-web/ui/dist "$staging/ui"
cp container/oci-image/Dockerfile "$staging/"
cp container/oci-image/node-profile.toml "$staging/profile.toml"
cp container/single-node-container/entrypoint.sh "$staging/"
git rev-parse HEAD > "$staging/SOURCE_REVISION"
printf '%s\n' "$source_tree" > "$staging/SOURCE_TREE"
cp VERSION "$staging/VERSION"
rm -rf target/container-runtime
mv "$staging" target/container-runtime
trap - EXIT
