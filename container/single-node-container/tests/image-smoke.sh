#!/bin/bash
set -euo pipefail

image=${CROWDB_CONTAINER_IMAGE:-crowdb-iceberg-single-node:dev}
docker image inspect "$image" >/dev/null
image_bytes=$(docker image inspect --format '{{.Size}}' "$image")
if ((image_bytes > 325000000)); then
    echo "single-node container image exceeds 325 MB: $image_bytes bytes" >&2
    exit 1
fi
test "$(docker image inspect --format '{{.Architecture}}' "$image")" = amd64
test "$(docker image inspect --format '{{.Config.User}}' "$image")" = crowdb:crowdb
test "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image")" = "$(cat VERSION)"
test "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image")" = "$(git rev-parse HEAD)"
volumes=$(docker image inspect --format '{{json .Config.Volumes}}' "$image")
jq -e 'has("/opt/crowdb/data")' <<<"$volumes" >/dev/null
exposed=$(docker image inspect --format '{{json .Config.ExposedPorts}}' "$image")
for port in 80 81 8080; do
    jq -e --arg port "$port/tcp" 'has($port)' <<<"$exposed" >/dev/null
done
for port in 10000 13000 15200; do
    jq -e --arg port "$port/tcp" 'has($port) | not' <<<"$exposed" >/dev/null
done

docker run --rm --network none --entrypoint /opt/crowdb/bin/crowdb-monitor "$image" validate /opt/crowdb/etc/profile.toml
docker run --rm --network none --entrypoint /bin/sh "$image" -ec '
    for tool in pixi cargo rustc gcc g++ cmake npm; do
        if command -v "$tool" >/dev/null 2>&1; then
            echo "Build tool was packaged into the runtime image: $tool" >&2
            exit 1
        fi
    done
    for library in /opt/crowdb/lib/libboost_regex* /opt/crowdb/lib/libicu*; do
        if [ -e "$library" ]; then
            echo "Unused Boost.Regex/ICU dependency was packaged: $library" >&2
            exit 1
        fi
    done
'
capability=$(docker run --rm --network none --entrypoint /sbin/getcap "$image" /opt/crowdb/bin/crowdb-access-server)
[[ "$capability" == *'cap_net_bind_service=ep' ]]

iceberg_output=$(docker run --rm --network none --entrypoint /opt/crowdb/bin/crowdb-access-server "$image" iceberg 2>&1) && {
    echo "Iceberg started without required configuration" >&2
    exit 1
}
printf '%s\n' "$iceberg_output"
[[ "$iceberg_output" == *'Error: NotPresent'* ]]
