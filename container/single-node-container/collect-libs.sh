#!/bin/bash
set -euo pipefail

build_root=$(git rev-parse --show-toplevel)
output=${1:?runtime staging directory is required}
mkdir -p "$output/bin" "$output/lib"

for binary in \
    crowdb-monitor crowdb-kv-server crowdb-diskdb crowdb-diskio \
    crowdb-chunkdb crowdb-chunk-kv-server crowdb-access-server \
    crowdb-iceberg crowdb-web; do
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

for binary in "$output"/bin/*; do
    if ! LD_LIBRARY_PATH="$build_root/.pixi/envs/default/lib:$build_root/target/release" ldd "$binary" > "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
    if grep -q 'not found' "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
    while read -r name arrow path remainder; do
        if [[ "$arrow" != '=>' ]]; then
            continue
        fi
        resolved_path=$(readlink -f "$path")
        case "$resolved_path" in
            "$build_root/.pixi/envs/default/lib/"*|"$build_root/target/release/"*)
                if ! cp -L "$resolved_path" "$output/lib/$name"; then
                    echo "cannot package dependency $name from $resolved_path" >&2
                    exit 1
                fi
                ;;
        esac
    done < "$output/dependencies.txt"
    patchelf --set-rpath '/opt/crowdb/lib' "$binary"
done
if [[ ! -f "$output/lib/libcrowdb_kv_client.so" ]]; then
    echo 'DiskIO FFI library was not collected' >&2
    exit 1
fi
for library in "$output"/lib/*; do
    patchelf --set-rpath '/opt/crowdb/lib' "$library"
done
rm "$output/dependencies.txt"

if [[ "${CROWDB_PACKAGE_SYMBOLS:-0}" == 1 ]]; then
    for artifact in "$output"/bin/* "$output"/lib/libcrowdb*.so; do
        [[ -f "$artifact" ]] || continue
        if ! readelf -W -S "$artifact" | grep -E '[[:space:]]\.debug_line[[:space:]]' >/dev/null; then
            echo "Missing source-line symbols: $artifact" >&2
            exit 1
        fi
        symbol="$output/symbols/$(basename "$(dirname "$artifact")")/$(basename "$artifact").debug"
        mkdir -p "$(dirname "$symbol")"
        objcopy --only-keep-debug "$artifact" "$symbol"
        objcopy --add-gnu-debuglink="$symbol" "$artifact"
    done
fi
for artifact in "$output"/bin/* "$output"/lib/*; do
    strip --strip-debug "$artifact"
done

for binary in "$output"/bin/*; do
    LD_LIBRARY_PATH="$output/lib" ldd "$binary" > "$output/dependencies.txt"
    if grep -q 'not found' "$output/dependencies.txt"; then
        cat "$output/dependencies.txt" >&2
        exit 1
    fi
done
rm "$output/dependencies.txt"
