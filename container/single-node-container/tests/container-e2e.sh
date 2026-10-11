#!/bin/bash
set -euo pipefail

image=${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}
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
        if [[ -n "${CROWDB_PREVIEW_TEST_ARTIFACTS:-}" ]]; then
            mkdir -p "$CROWDB_PREVIEW_TEST_ARTIFACTS"
            docker logs "$name" >"$CROWDB_PREVIEW_TEST_ARTIFACTS/container.log" 2>&1 || true
            if [[ -d "$root/log" ]]; then
                cp -R "$root/log" "$CROWDB_PREVIEW_TEST_ARTIFACTS/service-logs"
            fi
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
    docker run -d --name "$name" -e CROWDB_STARTUP_MODE=single \
        "${mount_args[@]}" \
        "$image" >/dev/null
    for attempt in $(seq 1 240); do
        state=$(docker inspect --format '{{.State.Status}}' "$name")
        if [[ "$state" != running ]]; then
            docker logs "$name"
            return 1
        fi
        if docker exec "$name" crowdb-monitor readiness; then
            return 0
        fi
        sleep 1
    done
    docker logs "$name"
    return 1
}

verify_listener_failure_propagation() {
    local failed=$1 output status
    if output=$(timeout 30 docker exec "$name" /bin/sh -ec '
        failed=$1
        config=/tmp/crowdb-access-listener-failure.toml
        case "$failed" in
            s3)
                sed "s/0.0.0.0:9092/127.0.0.1:18080/" /opt/crowdb/run/config/access.toml > "$config"
                ;;
            iceberg)
                sed "s/0.0.0.0:9091/127.0.0.1:18181/" /opt/crowdb/run/config/access.toml > "$config"
                ;;
            *) exit 2 ;;
        esac
        set -a
        . /opt/crowdb/data/secrets/server.env
        set +a
        export CROWDB_ACCESS_LOG_DIR=/tmp/crowdb-access-listener-failure-log
        export CROWDB_ICEBERG_PUBLIC_URI=http://127.0.0.1:9092
        export CROWDB_S3_PUBLIC_URI=http://127.0.0.1:9091
        exec /opt/crowdb/bin/crowdb-access-server --config "$config"
    ' _ "$failed" 2>&1); then
        echo "combined access process accepted an occupied $failed listener" >&2
        return 1
    else
        status=$?
    fi
    if (( status == 124 )) || [[ "$output" != *'Address already in use'* ]]; then
        echo "combined access $failed failure did not terminate as expected: status=$status" >&2
        printf '%s\n' "$output" >&2
        return 1
    fi
    docker exec "$name" crowdb-monitor readiness
}


start_container
echo "checking empty-volume boot"
docker exec "$name" crowdb-monitor readiness
client_env=$(docker exec "$name" crowdb-monitor credentials show --format env)
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
echo "checking combined access listener failure propagation"
verify_listener_failure_propagation s3
verify_listener_failure_propagation iceberg
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
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
docker rm -v "$name" >/dev/null
echo "container E2E passed"
