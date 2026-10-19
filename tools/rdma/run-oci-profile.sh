#!/usr/bin/env bash
# Run the R239 OCI profiles. The host owns the kernel driver and HCA; the
# image supplies userspace verbs, CM, and provider libraries.
set -Eeuo pipefail

profile="${1:-}"
image="${CROWDB_OCI_IMAGE:-crowdb:test}"
startup_mode="${CROWDB_STARTUP_MODE:-single}"
management_interface="${CROWDB_MANAGEMENT_INTERFACE:-${CROWDB_RDMA_LOCAL_IF:-enp35s0f0np0}}"
port_offset="${CROWDB_PORT_OFFSET:-0}"
rdma_cm_port=$((19101 + port_offset))
data_volume="${CROWDB_CONTAINER_DATA_VOLUME:-crowdb-rdma-${profile:-profile}}"
rdma_device="${CROWDB_RDMA_DEVICE:-}"
if [[ -z "$rdma_device" ]] && command -v rdma >/dev/null 2>&1; then
    rdma_device=$(rdma link show | awk -v netdev="$management_interface" \
        '$0 ~ (" netdev " netdev " ") {sub("/1$", "", $2); print $2; exit}')
fi
rdma_source_addr="${CROWDB_RDMA_SOURCE_ADDR:-}"
if [[ -z "$rdma_source_addr" ]] && command -v ip >/dev/null 2>&1; then
    rdma_source_addr=$(ip -4 -o addr show dev "$management_interface" scope global |
        awk 'NR == 1 {split($4, parts, "/"); print parts[1]}')
fi
rdma_source_env=()
if [[ -n "$rdma_source_addr" ]]; then
    rdma_source_env=(-e "CROWDB_RDMA_SOURCE_ADDR=$rdma_source_addr")
fi

if [[ -z "$profile" || "$profile" == "-h" || "$profile" == "--help" ]]; then
    cat >&2 <<'USAGE'
usage: run-oci-profile.sh tcp-only|shared-hca|shared-hca-unprivileged [image]

tcp-only  starts one container without RDMA devices or RDMA capability.
shared-hca starts one privileged host-network container using the shared HCA;
           invoke it once per logical container on the physical host.
shared-hca-unprivileged starts without --privileged and grants only the
                    verbs/CM devices, IPC_LOCK, and a bounded memlock limit.
USAGE
    exit 2
fi
if [[ $# -ge 2 ]]; then image="$2"; fi

case "$profile" in
    tcp-only)
        exec docker run --rm --network=host \
            --mount "type=volume,source=$data_volume,target=/opt/crowdb/data" \
            -e CROWDB_RDMA_ENABLED=0 \
            -e CROWDB_PORT_OFFSET="$port_offset" \
            -e CROWDB_STARTUP_MODE="$startup_mode" \
            -e CROWDB_MANAGEMENT_INTERFACE="$management_interface" \
            "$image"
        ;;
    shared-hca)
        [[ -n "$rdma_device" ]] || { echo "could not resolve RDMA device for $management_interface" >&2; exit 1; }
        exec docker run --rm --network=host --privileged --ulimit memlock=-1 \
            --mount "type=volume,source=$data_volume,target=/opt/crowdb/data" \
            -e CROWDB_RDMA_ENABLED=1 \
            -e CROWDB_RDMA_SHARED_HCA=1 \
            -e CROWDB_PORT_OFFSET="$port_offset" \
            -e CROWDB_RDMA_E2E_PORT="$rdma_cm_port" \
            "${rdma_source_env[@]}" \
            -e CROWDB_STARTUP_MODE="$startup_mode" \
            -e CROWDB_MANAGEMENT_INTERFACE="$management_interface" \
            -e CROWDB_RDMA_DEVICE="$rdma_device" \
            -e CROWDB_RDMA_PORT="${CROWDB_RDMA_PORT:-1}" \
            "$image"
        ;;
    shared-hca-unprivileged)
        [[ -n "$rdma_device" ]] || { echo "could not resolve RDMA device for $management_interface" >&2; exit 1; }
        exec docker run --rm --network=host \
            --mount "type=volume,source=$data_volume,target=/opt/crowdb/data" \
            --device /dev/infiniband/uverbs0 \
            --device /dev/infiniband/rdma_cm \
            --cap-add IPC_LOCK --ulimit memlock=67108864:67108864 \
            -e CROWDB_RDMA_ENABLED=1 \
            -e CROWDB_RDMA_SHARED_HCA=1 \
            -e CROWDB_PORT_OFFSET="$port_offset" \
            -e CROWDB_RDMA_E2E_PORT="$rdma_cm_port" \
            "${rdma_source_env[@]}" \
            -e CROWDB_STARTUP_MODE="$startup_mode" \
            -e CROWDB_MANAGEMENT_INTERFACE="$management_interface" \
            -e CROWDB_RDMA_DEVICE="$rdma_device" \
            -e CROWDB_RDMA_PORT="${CROWDB_RDMA_PORT:-1}" \
            "$image"
        ;;
    *)
        echo "unknown RDMA profile: $profile" >&2
        exit 2
        ;;
esac
