#!/bin/bash
set -euo pipefail

image=crowdb-single-node-preview:dev
docker image inspect "$image" >/dev/null
test "$(docker image inspect --format '{{.Architecture}}' "$image")" = amd64
test "$(docker image inspect --format '{{.Config.User}}' "$image")" = crowdb:crowdb
test "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image")" = "$(cat VERSION)"

docker run --rm --network none --entrypoint /opt/crowdb/bin/crowdb-monitor "$image" validate /opt/crowdb/etc/profile.toml
capability=$(docker run --rm --network none --entrypoint /sbin/getcap "$image" /opt/crowdb/bin/crowdb-iceberg)
[[ "$capability" == *'cap_net_bind_service=ep' ]]

iceberg_output=$(docker run --rm --network none --entrypoint /opt/crowdb/bin/crowdb-iceberg "$image" 2>&1) && {
    echo "Iceberg started without required configuration" >&2
    exit 1
}
printf '%s\n' "$iceberg_output"
[[ "$iceberg_output" == *'Error: NotPresent'* ]]

output=$(docker run --rm --network none "$image" 2>&1) && {
    echo "preview accepted an unmounted data root" >&2
    exit 1
}
printf '%s\n' "$output"
[[ "$output" == *'requires one volume mounted at /opt/crowdb/data'* ]]
