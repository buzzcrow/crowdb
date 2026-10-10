#!/bin/bash
set -euo pipefail

image=${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}
root=$(mktemp -d /tmp/crowdb-preview-e2e.XXXXXX)
layout_binary=$(mktemp /tmp/crowdb-chunk-layout.XXXXXX)
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
    rm -f "$layout_binary"
}
trap cleanup EXIT

cargo build --locked --release -p crowdb-chunkdb-client --example single_node_chunk_layout
cp target/release/examples/single_node_chunk_layout "$layout_binary"
patchelf --set-rpath /opt/crowdb/lib "$layout_binary"
chmod 0755 "$layout_binary"

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

verify_public_services() {
    local iceberg_port s3_port web_port dataset_port token
    iceberg_port=$(port 9092)
    s3_port=$(port 9091)
    web_port=$(port 9090)
    dataset_port=$(port 9093)
    docker exec "$name" crowdb-monitor readiness >/dev/null
    docker exec "$name" curl --fail --silent --show-error --max-time 5 \
        http://127.0.0.1:9094/_crowdb/health/ready >/dev/null
    curl --fail --silent --show-error --max-time 5 \
        "http://127.0.0.1:$web_port/api/authority" | jq -e '.source == "group0" and .available == true' >/dev/null
    curl --fail --silent --show-error --max-time 5 \
        "http://127.0.0.1:$web_port/api/preview" | jq -e '.source == "group0" and (.services | length) > 0' >/dev/null
    [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 5 "http://127.0.0.1:$dataset_port/v1/dataset/open")" == 404 ]]
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

verify_clients() {
    local operation=$1
    export AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY ICEBERG_TOKEN
    AWS_DEFAULT_REGION=$(printf '%s\n' "$client_env" | sed -n 's/^AWS_DEFAULT_REGION=//p')
    AWS_ACCESS_KEY_ID=$(printf '%s\n' "$client_env" | sed -n 's/^AWS_ACCESS_KEY_ID=//p')
    AWS_SECRET_ACCESS_KEY=$(printf '%s\n' "$client_env" | sed -n 's/^AWS_SECRET_ACCESS_KEY=//p')
    ICEBERG_TOKEN=$(printf '%s\n' "$client_env" | sed -n 's/^ICEBERG_TOKEN=//p')
    export CROWDB_PREVIEW_S3_ENDPOINT="http://127.0.0.1:$(port 9091)"
    export CROWDB_PREVIEW_ICEBERG_URI="http://127.0.0.1:$(port 9092)"
    pixi run -e s3-e2e python container/single-node-container/tests/s3-client.py "$operation"
    pixi run -e s3-e2e python container/single-node-container/tests/s3-cli-client.py "$operation"
    pixi run -e iceberg-e2e python container/single-node-container/tests/iceberg-client.py "$operation"
}

verify_chunk_layouts() {
    docker cp "$layout_binary" "$name:/tmp/chunk-layout-check"
    docker exec "$name" /tmp/chunk-layout-check
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

verify_web_logical() {
    local web_port status
    web_port=$(port 9090)
    curl --fail --silent --show-error --max-time 10 \
        "http://127.0.0.1:$web_port/api/stores" | jq -e 'any(.[]; .store_id == 0)' >/dev/null
    for endpoint in /api/stores /api/racks /api/node/admit; do
        status=$(curl --silent --show-error --max-time 10 --output /dev/null \
            --write-out '%{http_code}' --request POST --header 'Content-Type: application/json' \
            --data '{}' "http://127.0.0.1:$web_port$endpoint")
        if [[ "$status" != 403 ]]; then
            echo "Read-only single-node mutation $endpoint returned $status instead of 403" >&2
            return 1
        fi
    done
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

verify_invalid_manifest_rejected() {
    docker run --rm --network none --user root \
        --mount "type=bind,source=$root,target=/data" \
        --entrypoint /bin/sh "$image" -c \
        'printf "invalid manifest" > /data/bootstrap/manifest.json'
    docker run -d --name "$name" -e CROWDB_STARTUP_MODE=single \
        --mount "type=bind,source=$root,target=/opt/crowdb/data" "$image" >/dev/null
    for attempt in $(seq 1 30); do
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") == exited ]]; then
            [[ $(docker inspect --format '{{.State.ExitCode}}' "$name") != 0 ]]
            docker logs "$name" 2>&1 | grep -F 'bootstrap manifest cannot be decoded' >/dev/null
            return 0
        fi
        sleep 1
    done
    echo 'container accepted a corrupt bootstrap manifest' >&2
    return 1
}

verify_invalid_profile_rejected() {
    printf 'invalid = true\n' >"$root/bad-profile.toml"
    docker run -d --name "$name" -e CROWDB_STARTUP_MODE=single \
        --mount "type=bind,source=$root/bad-profile.toml,target=/opt/crowdb/etc/profile.toml,readonly" \
        "$image" >/dev/null
    for attempt in $(seq 1 30); do
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") == exited ]]; then
            [[ $(docker inspect --format '{{.State.ExitCode}}' "$name") != 0 ]]
            docker logs "$name" 2>&1 | grep -F 'failed to decode deployment profile' >/dev/null
            return 0
        fi
        sleep 1
    done
    echo 'container accepted an invalid deployment profile' >&2
    return 1
}

verify_interrupted_bootstrap() {
    local manifest deployment_id completed_steps recovered
    docker run -d --name "$name" -e CROWDB_STARTUP_MODE=single \
        --mount "type=bind,source=$root,target=/opt/crowdb/data" \
        "$image" >/dev/null
    manifest=
    for attempt in $(seq 1 400); do
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") != running ]]; then
            echo 'container exited during bootstrap interruption setup' >&2
            return 1
        fi
        manifest=$(docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json 2>/dev/null) || true
        if jq -e '.state == "initializing" and any(.steps[]; .complete)' <<<"$manifest" >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
    done
    if ! jq -e '.state == "initializing" and any(.steps[]; .complete)' <<<"$manifest" >/dev/null; then
        echo 'bootstrap did not expose a completed step before readiness' >&2
        return 1
    fi
    deployment_id=$(jq -er '.deployment_id' <<<"$manifest")
    completed_steps=$(jq -c '[.steps[] | select(.complete) | .name]' <<<"$manifest")
    docker kill --signal=KILL "$name" >/dev/null
    [[ $(docker wait "$name") != 0 ]]
    docker rm "$name" >/dev/null
    start_container
    recovered=$(docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json)
    jq -e --arg deployment_id "$deployment_id" --argjson completed_steps "$completed_steps" \
        '. as $manifest | .state == "ready" and .deployment_id == $deployment_id and
         all(.steps[]; .complete) and
         all($completed_steps[]; . as $name | any($manifest.steps[]; .name == $name and .complete))' \
        <<<"$recovered" >/dev/null
    docker exec "$name" crowdb-monitor readiness
}

echo "checking interrupted bootstrap recovery"
verify_interrupted_bootstrap
echo "checking empty-volume boot"
docker exec "$name" crowdb-monitor readiness
client_env=$(docker exec "$name" crowdb-monitor credentials show --format env)
[[ "$client_env" == *'AWS_ACCESS_KEY_ID='* && "$client_env" == *'ICEBERG_TOKEN='* ]]
[[ $(docker exec "$name" stat -c %a /opt/crowdb/data/secrets/server.env) == 600 ]]
[[ $(docker exec "$name" stat -c %a /opt/crowdb/data/secrets/client.env) == 600 ]]
docker exec "$name" cat /opt/crowdb/data/bootstrap/manifest.json | jq -e '.state == "ready"' >/dev/null
[[ $(docker exec "$name" stat -c %a /opt/crowdb/data/crash) == 700 ]]
[[ $(docker exec --user crowdb "$name" readlink /proc/1/cwd) == /opt/crowdb/data/crash ]]
kv_pid=$(docker exec "$name" cat /opt/crowdb/run/status/monitor.json | jq -er '.services.kv.pid')
[[ $(docker exec --user crowdb "$name" readlink "/proc/$kv_pid/cwd") == /opt/crowdb/data/crash ]]
verify_public_services
node container/single-node-container/tests/web-ui.cjs "http://127.0.0.1:$(port 9090)" "$name"
echo "checking S3 and Iceberg client writes"
verify_clients write
echo "checking single-node protocol chunk layouts"
verify_chunk_layouts
echo "checking combined access listener failure propagation"
verify_listener_failure_propagation s3
verify_listener_failure_propagation iceberg
echo "checking Web logical writes"
verify_web_logical
for service in kv diskdb diskio chunkdb chunk-kv access web; do
    echo "checking $service crash recovery"
    verify_child_recovery "$service" KILL child_exited
done
for service in kv diskdb diskio chunkdb chunk-kv access web; do
    echo "checking $service hang recovery"
    verify_child_recovery "$service" STOP probe_failed
done
verify_public_services
node container/single-node-container/tests/web-ui.cjs "http://127.0.0.1:$(port 9090)"
verify_clients read
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
verify_clients read
echo "checking restart budget exhaustion"
verify_restart_exhaustion
python container/single-node-container/tests/logs.py "$root" "$name"
docker rm "$name" >/dev/null
start_container
verify_public_services
verify_clients read
echo "checking recovery rejects changed durable identity"
verify_recovery_identity_rejection
docker rm -v "$name" >/dev/null
echo "checking corrupt manifest rejection"
verify_invalid_manifest_rejected
docker rm -v "$name" >/dev/null
echo "checking invalid profile rejection"
verify_invalid_profile_rejected
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
