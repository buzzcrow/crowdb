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
        -p 127.0.0.1:9092:9092 -p 127.0.0.1::9091 -p 127.0.0.1::9090 -p 127.0.0.1::9093 \
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

port() {
    local published
    published=$(docker port "$name" "$1/tcp")
    printf '%s\n' "${published##*:}"
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

verify_child_recovery() {
    local service=$1 signal=$2 expected_event=$3 old_pid old_generation new_pid new_generation state
    state=$(docker exec "$name" cat /opt/crowdb/run/status/monitor.json)
    old_pid=$(jq -er --arg service "$service" '.services[$service].pid' <<<"$state")
    old_generation=$(jq -er --arg service "$service" '.services[$service].generation' <<<"$state")
    docker exec "$name" kill -"$signal" "$old_pid"
    for attempt in $(seq 1 90); do
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") != running ]]; then
            echo "container exited while recovering $service" >&2
            return 1
        fi
        state=$(docker exec "$name" cat /opt/crowdb/run/status/monitor.json)
        new_pid=$(jq -er --arg service "$service" '.services[$service].pid // empty' <<<"$state") || true
        new_generation=$(jq -er --arg service "$service" '.services[$service].generation' <<<"$state")
        if [[ $(jq -r '.phase' <<<"$state") == ready && "$new_pid" != "$old_pid" && -n "$new_pid" ]] &&
            (( new_generation > old_generation )); then
            docker exec "$name" cat /opt/crowdb/data/log/monitor/monitor.log |
                jq -se --arg service "$service" --arg kind "$expected_event" \
                    'any(.[]; .service == $service and .kind == $kind)' >/dev/null
            docker exec "$name" crowdb-monitor readiness
            return 0
        fi
        sleep 1
    done
    echo "$service did not recover after $signal" >&2
    return 1
}

verify_restart_exhaustion() {
    local old_pid state exit_code
    for attempt in $(seq 1 5); do
        verify_child_recovery web KILL child_exited
    done
    state=$(docker exec "$name" cat /opt/crowdb/run/status/monitor.json)
    old_pid=$(jq -er '.services.web.pid' <<<"$state")
    docker exec "$name" kill -KILL "$old_pid"
    for attempt in $(seq 1 40); do
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") == exited ]]; then
            exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$name")
            [[ "$exit_code" != 0 ]]
            docker logs "$name" 2>&1 | grep -F '"kind":"restart_exhausted"' >/dev/null
            return 0
        fi
        sleep 1
    done
    echo 'container stayed running after restart budget exhaustion' >&2
    return 1
}

verify_recovery_identity_rejection() {
    local old_pid ready_before ready_after exit_code
    ready_before=$(docker exec "$name" cat /opt/crowdb/data/log/monitor/monitor.log |
        jq -s '[.[] | select(.kind == "ready")] | length')
    old_pid=$(docker exec "$name" cat /opt/crowdb/run/status/monitor.json | jq -er '.services.web.pid')
    docker exec --user root "$name" /bin/sh -c 'printf "invalid credentials\n" > /opt/crowdb/data/secrets/server.env'
    docker exec "$name" kill -KILL "$old_pid"
    for attempt in $(seq 1 40); do
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") == exited ]]; then
            exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$name")
            [[ "$exit_code" != 0 ]]
            ready_after=$(docker run --rm --network none --user root \
                --mount "type=bind,source=$root,target=/data" \
                --entrypoint /bin/sh "$image" -c \
                'cat /data/log/monitor/monitor.log' |
                jq -s '[.[] | select(.kind == "ready")] | length')
            [[ "$ready_after" == "$ready_before" ]]
            docker logs "$name" 2>&1 | grep -F 'server credentials are incomplete' >/dev/null
            return 0
        fi
        sleep 1
    done
    echo 'container restored readiness with changed durable credentials' >&2
    return 1
}


start_container
echo "checking empty-volume boot"
docker exec "$name" crowdb-monitor readiness
client_env=$(docker exec "$name" crowdb-monitor credentials show --format env)
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
node container/single-node-container/tests/web-ui.cjs "http://127.0.0.1:$(port 9090)" "$name"
echo "checking combined access listener failure propagation"
verify_listener_failure_propagation s3
verify_listener_failure_propagation iceberg
node container/single-node-container/tests/web-ui.cjs "http://127.0.0.1:$(port 9090)"
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
echo "checking restart budget exhaustion"
verify_restart_exhaustion
python container/single-node-container/tests/logs.py "$root" "$name"
docker rm "$name" >/dev/null
start_container
echo "checking recovery rejects changed durable identity"
verify_recovery_identity_rejection
docker rm -v "$name" >/dev/null
start_container anonymous
echo "checking default anonymous-volume boot"
docker inspect "$name" | jq -e '.[0].Mounts | any(.Destination == "/opt/crowdb/data" and .Type == "volume")' >/dev/null
docker exec "$name" crowdb-monitor readiness
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
docker logs "$name" 2>&1 | grep -F 'For data you want to keep across container recreation' >/dev/null
echo "checking monitor death exits the container"
docker kill --signal=KILL "$name" >/dev/null
[[ $(docker wait "$name") != 0 ]]
echo "container E2E passed"
