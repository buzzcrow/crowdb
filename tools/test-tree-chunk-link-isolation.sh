#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build_dir="$repo_root/lib/crowdb-tree/build"
fixture_dir="$repo_root/tools/tree-link-isolation"
probe_dir=$(mktemp -d)
trap 'rm -rf "$probe_dir"' EXIT

cmake -S "$repo_root/lib/crowdb-tree" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release
cmake --build "$build_dir" --target crowdb-tree -j

include_flags=(
    -I"$repo_root/lib/crowdb-tree/include"
    -I"$repo_root/lib/crowdb-common/cpp/include"
)
link_libraries=(
    "$build_dir/libcrowdb-tree.a"
    "$build_dir/crowdb-rpc-build/libcrowdb-rpc.a"
    "$build_dir/crowdb-common-build/libcrowdbcommon.a"
    -luring -llz4 -lspdlog -lfmt -lz -lisal -lfolly -lglog -lgflags -levent -ldl -pthread
)

for compiler in g++ clang++; do
    command -v "$compiler" >/dev/null
    "$compiler" -std=c++20 -fsyntax-only "${include_flags[@]}" "$fixture_dir/public_header.cpp"
done

c++ -std=c++20 "${include_flags[@]}" "$fixture_dir/local.cpp" "${link_libraries[@]}" \
    -Wl,-Map="$probe_dir/tree-link-local.map" -o "$probe_dir/tree-link-local"
c++ -std=c++20 "${include_flags[@]}" "$fixture_dir/chunk.cpp" "${link_libraries[@]}" \
    -Wl,-Map="$probe_dir/tree-link-chunk.map" -o "$probe_dir/tree-link-chunk"

nm -C "$probe_dir/tree-link-local" >"$probe_dir/tree-link-local.symbols"
nm -C "$probe_dir/tree-link-chunk" >"$probe_dir/tree-link-chunk.symbols"
if grep -E 'ChunkPageStore|RpcChunkTransport|ct_chunk_|ct_rpc_chunk_' "$probe_dir/tree-link-local.symbols" \
    >/dev/null; then
    echo "ordinary tree link unexpectedly extracted chunk backend symbols" >&2
    grep -E 'ChunkPageStore|RpcChunkTransport|ct_chunk_|ct_rpc_chunk_' "$probe_dir/tree-link-local.symbols" >&2
    exit 1
fi
if grep -E 'libcrowdb-tree\.a\((chunk_|rpc_chunk_)' "$probe_dir/tree-link-local.map" >/dev/null; then
    echo "ordinary tree link unexpectedly extracted a chunk archive member" >&2
    exit 1
fi
grep 'libcrowdb-tree.a(c_api.cpp.o)' "$probe_dir/tree-link-local.map" >/dev/null
grep 'libcrowdb-tree.a(chunk_page_store.cpp.o)' "$probe_dir/tree-link-chunk.map" >/dev/null
grep 'libcrowdb-tree.a(rpc_chunk_transport.cpp.o)' "$probe_dir/tree-link-chunk.map" >/dev/null
grep 'ct_chunk_page_store_open' "$probe_dir/tree-link-chunk.symbols" >/dev/null
grep 'ct_rpc_chunk_transport_open' "$probe_dir/tree-link-chunk.symbols" >/dev/null

"$probe_dir/tree-link-local"
"$probe_dir/tree-link-chunk"
