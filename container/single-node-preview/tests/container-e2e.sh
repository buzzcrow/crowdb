#!/bin/bash
set -euo pipefail

image=crowdb-single-node-preview:dev
root=$(mktemp -d /tmp/crowdb-preview-e2e.XXXXXX)
name="crowdb-preview-e2e-$$"
chmod 0777 "$root"

cleanup() {
    local status=$?
    if (( status != 0 )); then
        echo "container E2E failed; diagnostic logs follow" >&2
        docker logs "$name" >&2 || true
        if [[ -f "$root/log/monitor/monitor.log" ]]; then
            cat "$root/log/monitor/monitor.log" >&2
        fi
    fi
    docker rm -fv "$name" >/dev/null 2>&1 || true
    docker run --rm --network none --user root \
        --mount "type=bind,source=$root,target=/data" \
        --entrypoint /bin/chmod "$image" -R 0777 /data >/dev/null 2>&1 || true
    rm -rf "$root"
}
trap cleanup EXIT

start_container() {
    local storage_mode=${1:-bind}
    local mount_args=()
    if [[ "$storage_mode" == bind ]]; then
        mount_args=(--mount "type=bind,source=$root,target=/opt/crowdb/data")
    fi
    docker run -d --name "$name" \
        "${mount_args[@]}" \
        -p 127.0.0.1::80 -p 127.0.0.1::8010 -p 127.0.0.1::8080 \
        "$image" >/dev/null
    for attempt in $(seq 1 240); do
        state=$(docker inspect --format '{{.State.Status}}' "$name")
        if [[ "$state" != running ]]; then
            docker logs "$name"
            return 1
        fi
        health=$(docker inspect --format '{{.State.Health.Status}}' "$name")
        if [[ "$health" == healthy ]]; then
            return 0
        fi
        sleep 1
    done
    docker logs "$name"
    return 1
}

port() {
    local published
    published=$(docker port "$name" "$1/tcp")
    printf '%s\n' "${published##*:}"
}

verify_public_services() {
    local iceberg_port s3_port web_port token
    iceberg_port=$(port 80)
    s3_port=$(port 8010)
    web_port=$(port 8080)
    curl --fail --silent --show-error --max-time 5 \
        "http://127.0.0.1:$s3_port/_crowdb/health/ready" >/dev/null
    curl --fail --silent --show-error --max-time 5 \
        "http://127.0.0.1:$web_port/api/authority" | jq -e '.source == "group0" and .available == true' >/dev/null
    curl --fail --silent --show-error --max-time 5 \
        "http://127.0.0.1:$web_port/api/preview" | jq -e '.source == "group0" and (.services | length) > 0' >/dev/null
    token=$(printf '%s\n' "$client_env" | sed -n 's/^ICEBERG_TOKEN=//p')
    [[ -n "$token" ]]
    printf 'header = "Authorization: Bearer %s"\nurl = "http://127.0.0.1:%s/v1/config"\n' "$token" "$iceberg_port" |
        curl --config - --fail --silent --show-error --max-time 5 | jq -e '.defaults != null' >/dev/null
    for internal in 10000 10100 11000 13000 15100 15200; do
        if docker port "$name" "$internal/tcp" >/dev/null 2>&1; then
            echo "internal port $internal is published" >&2
            return 1
        fi
    done
}

start_container
echo "checking empty-volume boot"
docker exec "$name" crowdb-monitor readiness
client_env=$(docker exec "$name" crowdb-monitor credentials show --format env)
[[ "$client_env" == *'AWS_ACCESS_KEY_ID='* && "$client_env" == *'ICEBERG_TOKEN='* ]]
[[ $(docker exec "$name" stat -c %a /opt/crowdb/data/secrets/server.env) == 600 ]]
[[ $(docker exec "$name" stat -c %a /opt/crowdb/data/secrets/client.env) == 600 ]]
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
verify_public_services
sleep 12
docker exec "$name" crowdb-monitor readiness
if grep -Rq 'local split planned' "$root/log/kv"; then
    echo 'disabled chunk-KV balance planned a split' >&2
    exit 1
fi

docker stop --time 15 "$name" >/dev/null
docker rm "$name" >/dev/null
start_container
echo "checking persisted-volume restart"
docker exec "$name" crowdb-monitor readiness
[[ "$(docker exec "$name" crowdb-monitor credentials show --format env)" == "$client_env" ]]
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
verify_public_services
docker stop --time 15 "$name" >/dev/null
docker rm -v "$name" >/dev/null
start_container anonymous
echo "checking default anonymous-volume boot"
docker inspect "$name" | jq -e '.[0].Mounts | any(.Destination == "/opt/crowdb/data" and .Type == "volume")' >/dev/null
docker exec "$name" crowdb-monitor readiness
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
docker logs "$name" 2>&1 | grep -F 'For data you want to keep across container recreation' >/dev/null
echo "container E2E passed"
