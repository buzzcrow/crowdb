#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
cd "$root"

image=${CROWDB_CONTAINER_IMAGE:-crowdb-iceberg-single-node:dev}
name=${CROWDB_CONTAINER_NAME:-crowdb-single-node}

usage() {
    echo 'Usage: dev-container.sh {start|clean|inject [container IP|ID|name]}' >&2
    exit 2
}

wait_until_healthy() {
    local status
    for _ in $(seq 1 240); do
        status=$(docker inspect --format '{{.State.Status}}' "$name" 2>/dev/null || true)
        if [[ "$status" != running ]]; then
            docker logs "$name" >&2 || true
            echo "Container $name stopped before becoming healthy" >&2
            return 1
        fi
        status=$(docker inspect --format '{{.State.Health.Status}}' "$name")
        if [[ "$status" == healthy ]]; then
            echo "Container $name is healthy"
            return 0
        fi
        sleep 1
    done
    docker logs "$name" >&2 || true
    echo "Timed out waiting for container $name to become healthy" >&2
    return 1
}

start_container() {
    if docker container inspect "$name" >/dev/null 2>&1; then
        if [[ $(docker inspect --format '{{.State.Status}}' "$name") == running ]]; then
            echo "Container $name is already running"
            wait_until_healthy
            return
        fi
        docker rm "$name" >/dev/null
    fi

    if ! docker run --detach --name "$name" \
        --expose 9090 --expose 9091 --expose 9092 --expose 9093 \
        "$image"; then
        if docker container inspect "$name" >/dev/null 2>&1; then
            docker rm --force --volumes "$name" >/dev/null
        fi
        return 1
    fi
    if ! wait_until_healthy; then
        docker rm --force --volumes "$name" >/dev/null 2>&1 || true
        return 1
    fi
    docker exec "$name" crowdb-monitor credentials show --format env
}

select_inject_container() {
    local target=${1:-${CROWDB_CONTAINER_NAME:-}}
    local rows id candidate_name candidate_image source ips ip
    local -a candidates=() matches=()
    rows=$(docker ps --no-trunc --format '{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Label "org.opencontainers.image.source"}}') || return
    while IFS=$'\t' read -r id candidate_name candidate_image source; do
        [[ -n "$id" ]] || continue
        if [[ "$source" != https://github.com/buzzcrow/crowdb &&
              "$candidate_image" != "$image" && "$candidate_image" != *crowdb* ]]; then
            continue
        fi
        ips=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{if .IPAddress}}{{.IPAddress}} {{end}}{{end}}' "$id") || return
        candidates+=("$id  $candidate_name  $candidate_image  ${ips:-<no IP>}")
        if [[ -z "$target" || "$target" == "$candidate_name" || "$id" == "$target"* ]]; then
            matches+=("$id")
        else
            for ip in $ips; do
                if [[ "$target" == "$ip" ]]; then
                    matches+=("$id")
                    break
                fi
            done
        fi
    done <<< "$rows"
    if ((${#matches[@]} != 1)); then
        if [[ -n "$target" ]]; then
            echo "Cannot uniquely select a running CROWDB container for '$target'." >&2
        else
            echo 'Expected one running CROWDB container; specify its IP, ID, or name.' >&2
        fi
        if ((${#candidates[@]})); then
            echo 'Candidates (ID  NAME  IMAGE  IP):' >&2
            printf '%s\n' "${candidates[@]}" >&2
        else
            echo "No running CROWDB containers found; use 'pixi run start-container'." >&2
        fi
        echo 'Usage: pixi run inject-container <container IP|ID|name>' >&2
        return 1
    fi
    name=${matches[0]}
}

inject_container() {
    select_inject_container "${1:-}" || return
    local credentials_file
    credentials_file=$(mktemp "${TMPDIR:-/tmp}/crowdb-container-credentials.XXXXXX")
    trap 'rm -f "$credentials_file"' RETURN
    docker exec "$name" crowdb-monitor credentials show --format env > "$credentials_file"
    local container_ip ip addresses
    local -a container_ips
    addresses=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{if .IPAddress}}{{.IPAddress}} {{end}}{{end}}' "$name") || return
    read -r -a container_ips <<< "$addresses"
    container_ip=${container_ips[0]:-}
    for ip in "${container_ips[@]}"; do
        if [[ "$ip" == "${1:-${CROWDB_CONTAINER_NAME:-}}" ]]; then
            container_ip=$ip
            break
        fi
    done
    if [[ -z "$container_ip" ]]; then
        echo "Container $name has no Docker network address" >&2
        return 1
    fi
    echo "Injecting into CROWDB container $name at $container_ip"
    pixi run -e iceberg-e2e python tools/load-tpch.py \
        --console-url "http://$container_ip:9090" \
        --credentials-file "$credentials_file" \
        --catalog-uri "http://$container_ip:9092" \
        --s3-endpoint "http://$container_ip:9091"
}

clean_container() {
    if docker container inspect "$name" >/dev/null 2>&1; then
        docker rm --force --volumes "$name"
    fi

    # Remove stopped containers created from the selected image.
    mapfile -t stopped_ids < <(docker ps --all --filter status=exited --filter ancestor="$image" --quiet)
    if ((${#stopped_ids[@]})); then
        docker rm --volumes "${stopped_ids[@]}"
    fi
    # Dangling images have no tags to identify their original project.
    # Docker preserves images referenced by any container.
    docker image prune --force
}

case "${1:-}" in
    start) start_container ;;
    inject)
        (($# <= 2)) || usage
        inject_container "${2:-}"
        ;;
    clean) clean_container ;;
    *) usage ;;
esac
