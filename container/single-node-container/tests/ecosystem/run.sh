#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail
cd "${PIXI_PROJECT_ROOT:?}"
image=${CROWDB_CONTAINER_IMAGE:-crowdb-node:dev}
profile=${CROWDB_ECOSYSTEM_PROFILE:-all}
[[ "$profile" == all || "$profile" == python ]] || { echo 'Expected all or python profile' >&2; exit 2; }
docker image inspect "$image" >/dev/null
fixture=$(mktemp -d /tmp/crowdb-ecosystem.XXXXXX)
name="crowdb-ecosystem-${fixture##*.}"
trino_name="$name-trino"
mkdir -p "$fixture/data" "$fixture/results" "$fixture/private"
chmod 0777 "$fixture/data"
export CROWDB_ECOSYSTEM_STATE="$fixture/state.json"
export CROWDB_ECOSYSTEM_RESULTS="$fixture/results"
client_env=

cleanup() {
    local status=$?
    trap - EXIT
    if (( status != 0 )); then
        docker logs "$name" >"$fixture/private/container.log" 2>&1 || true
        docker logs "$trino_name" >"$fixture/private/trino.log" 2>&1 || true
        # Only the credential-redacted diagnostics and operation reports leave
        # the owned fixture. Never archive secrets, a volume or raw runtime logs.
        python container/single-node-container/tests/ecosystem/diagnostics.py "$fixture" || status=1
    fi
    if [[ -n "${CROWDB_ECOSYSTEM_ARTIFACTS:-}" ]]; then
        if ! mkdir -p "$CROWDB_ECOSYSTEM_ARTIFACTS" || ! cp -R "$fixture/results/." "$CROWDB_ECOSYSTEM_ARTIFACTS/"; then
            status=1
        fi
    fi
    docker rm -fv "$trino_name" >/dev/null 2>&1 || true
    docker rm -fv "$name" >/dev/null 2>&1 || true
    docker run --rm --network none --user root \
        --mount "type=bind,source=$fixture/data,target=/data" \
        --entrypoint /bin/chmod "$image" -R 0777 /data >/dev/null 2>&1 || true
    rm -rf "$fixture"
    exit "$status"
}
trap cleanup EXIT

start() {
    docker run -d --name "$name" -e CROWDB_STARTUP_MODE=single \
        --mount "type=bind,source=$fixture/data,target=/opt/crowdb/data" \
        -p 127.0.0.1:9092:9092 -p 127.0.0.1::9091 -p 127.0.0.1::9090 "$image" >/dev/null
    for ((attempt=0; attempt<240; attempt++)); do
        [[ $(docker inspect --format '{{.State.Status}}' "$name") == running ]] || return 1
        if docker exec "$name" crowdb-monitor readiness; then return 0; fi
        sleep 1
    done
    echo 'Owned ecosystem container did not become healthy' >&2
    return 1
}

run_client() {
    local label=$1
    shift
    # Keep failure output private until credential redaction at teardown.
    "$@" >"$fixture/private/$label.log" 2>&1 || return $?
    python container/single-node-container/tests/ecosystem/diagnostics.py "$fixture" "$label"
}

start
client_env=$(docker exec "$name" crowdb-monitor credentials show --format env)
export ICEBERG_TOKEN
ICEBERG_TOKEN=$(sed -n 's/^ICEBERG_TOKEN=//p' <<<"$client_env")
[[ -n "$ICEBERG_TOKEN" ]]
export CROWDB_PREVIEW_ICEBERG_URI=http://127.0.0.1:9092
docker inspect "$name" | python container/single-node-container/tests/ecosystem/public_ports.py
docker image inspect --format '{{.Id}}' "$image" >"$fixture/results/image-id.txt"
git rev-parse HEAD >"$fixture/results/source-revision.txt"
base=container/single-node-container/tests/ecosystem
run_client python-write python "$base/python_client.py" write
run_client python-before python "$base/python_client.py" verify
run_client duckdb python "$base/duckdb_client.py"

java -version >"$fixture/results/java-version.txt" 2>&1
if [[ "$profile" == all ]]; then
    run_client spark-mutate bash "$base/engine.sh" spark mutate
    run_client flink-mutate bash "$base/engine.sh" flink mutate
    run_client python-capture python "$base/python_client.py" capture
    snapshot=$(python -c 'import json,os; print(json.load(open(os.environ["CROWDB_ECOSYSTEM_STATE"]))["engine_snapshot"])')
    run_client python-handoff-before python "$base/python_client.py" handoff
    run_client spark-before bash "$base/engine.sh" spark verify "$snapshot"
    run_client flink-before bash "$base/engine.sh" flink verify
    run_client trino-before python "$base/trino_client.py" "$trino_name" "$fixture/private"
fi

docker stop --time 15 "$name" >/dev/null
docker rm -v "$name" >/dev/null
start
[[ $(docker exec "$name" crowdb-monitor credentials show --format env) == "$client_env" ]]
docker inspect "$name" | python "$base/public_ports.py"
run_client python-after python "$base/python_client.py" verify
if [[ "$profile" == all ]]; then
    run_client duckdb-after python "$base/duckdb_client.py" final
else
    run_client duckdb-after python "$base/duckdb_client.py"
fi
if [[ "$profile" == all ]]; then
    run_client python-handoff-after python "$base/python_client.py" handoff
    run_client spark-after bash "$base/engine.sh" spark verify "$snapshot"
    run_client flink-after bash "$base/engine.sh" flink verify
    run_client trino-after python "$base/trino_client.py" "$trino_name" "$fixture/private"
fi
printf 'Passed %s profile against image %s, including persisted-volume restart\n' "$profile" "$image"
