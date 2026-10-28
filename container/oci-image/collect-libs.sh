#!/bin/bash
set -euo pipefail

build_root=$(git rev-parse --show-toplevel)
pixi_lib_root=$(readlink -f "$build_root/.pixi/envs/default/lib")
release_root=$(readlink -f "$build_root/target/release")
output=${1:?runtime staging directory is required}
case "${CROWDB_KEEP_DEBUG_INFO:-0}" in
    0) debug_info=stripped ;;
    1) debug_info=retained ;;
    *) echo 'CROWDB_KEEP_DEBUG_INFO must be 0 or 1' >&2; exit 1 ;;
esac
mkdir -p "$output/bin" "$output/lib"

# libibverbs loads HCA providers (mlx5, efa, ... ) dynamically, so they do
# not appear in ldd output. Keep the provider ABI matched with the pixi
# libibverbs runtime instead of relying on the base image's older providers.
provider_root="$build_root/.pixi/envs/default/lib/libibverbs"
provider_config="$build_root/.pixi/envs/default/etc/libibverbs.d"
if [[ -d "$provider_root" && -d "$provider_config" ]]; then
    mkdir -p "$output/lib/libibverbs" "$output/etc/libibverbs.d"
    cp -L "$provider_root"/*.so "$output/lib/libibverbs/"
    cp -a "$provider_config"/*.driver "$output/etc/libibverbs.d/"
fi

for binary in \
    crowdb-monitor crowdb-paxos-kv-server crowdb-diskdb crowdb-diskio \
    crowdb-chunkdb crowdb-chunk-kv-server crowdb-access-server \
    crowdb-web; do
    if [[ "$binary" == crowdb-diskio ]]; then
        source="$build_root/app/crowdb-diskio/build/crowdb-diskio"
    else
        source="$build_root/target/release/$binary"
    fi
    if [[ ! -x "$source" ]]; then
        echo "preview binary is missing: $source" >&2
        exit 1
    fi
    echo "packing $binary"
    cp "$source" "$output/bin/$binary"
done

for binary in "$output"/bin/* "$output"/lib/libibverbs/*.so; do
    [[ -f "$binary" ]] || continue
    if ! LD_LIBRARY_PATH="$build_root/.pixi/envs/default/lib:$build_root/target/release" ldd -r "$binary" > "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
    if grep -Eq 'not found|undefined symbol' "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
    while read -r name arrow path _remainder; do
        if [[ "$arrow" != '=>' ]]; then
            continue
        fi
        resolved_path=$(readlink -f "$path")
        case "$resolved_path" in
            "$pixi_lib_root/"*|"$release_root/"*)
                if ! cp -L "$resolved_path" "$output/lib/$name"; then
                    echo "cannot package dependency $name from $resolved_path" >&2
                    exit 1
                fi
                ;;
        esac
    done < "$output/dependencies.txt"
    patchelf --set-rpath '/opt/crowdb/lib' "$binary"
done
if [[ ! -f "$output/lib/libcrowdb_paxos_kv_client.so" ]]; then
    echo 'DiskIO FFI library was not collected' >&2
    exit 1
fi
if [[ ! -f "$output/lib/libcrypto.so.3" ]]; then
    echo 'the pixi OpenSSL runtime was not collected' >&2
    exit 1
fi
for library in "$output"/lib/*; do
    [[ -f "$library" ]] || continue
    patchelf --set-rpath '/opt/crowdb/lib' "$library"
done
rm "$output/dependencies.txt"

if [[ "$debug_info" == stripped ]]; then
    for artifact in "$output"/bin/* "$output"/lib/* "$output"/lib/libibverbs/*.so; do
        [[ -f "$artifact" ]] || continue
        strip --strip-debug "$artifact"
    done
fi
printf '%s\n' "$debug_info" > "$output/DEBUG_INFO"

for binary in "$output"/bin/* "$output"/lib/* "$output"/lib/libibverbs/*.so; do
    [[ -f "$binary" ]] || continue
    if ! LD_LIBRARY_PATH="$output/lib" ldd -r "$binary" > "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
    if grep -Eq 'not found|undefined symbol' "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
done
rm "$output/dependencies.txt"
