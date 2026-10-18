#!/usr/bin/env bash
# Run the R239 OCI profiles. The host owns the kernel driver and HCA; the
# image supplies userspace verbs, CM, and provider libraries.
set -Eeuo pipefail

profile="${1:-}"
image="${CROWDB_OCI_IMAGE:-crowdb:test}"
startup_mode="${CROWDB_STARTUP_MODE:-single}"
management_interface="${CROWDB_MANAGEMENT_INTERFACE:-eth0}"
port_offset="${CROWDB_PORT_OFFSET:-0}"
rdma_cm_port=$((19101 + port_offset))
if [[ -z "$profile" || "$profile" == "-h" || "$profile" == "--help" ]]; then
    cat >&2 <<'USAGE'
usage: run-oci-profile.sh tcp-only|shared-hca [image]

tcp-only  starts one container without RDMA devices or RDMA capability.
shared-hca starts one privileged host-network container using the shared HCA;
           invoke it once per logical container on the physical host.
USAGE
    exit 2
fi
if [[ $# -ge 2 ]]; then image="$2"; fi

case "$profile" in
    tcp-only)
        exec docker run --rm --network=host \
            -e CROWDB_RDMA_ENABLED=0 \
            -e CROWDB_PORT_OFFSET="$port_offset" \
            -e CROWDB_STARTUP_MODE="$startup_mode" \
            -e CROWDB_MANAGEMENT_INTERFACE="$management_interface" \
            "$image"
        ;;
    shared-hca)
        exec docker run --rm --network=host --privileged --ulimit memlock=-1 \
            -e CROWDB_RDMA_ENABLED=1 \
            -e CROWDB_RDMA_SHARED_HCA=1 \
            -e CROWDB_PORT_OFFSET="$port_offset" \
            -e CROWDB_RDMA_E2E_PORT="$rdma_cm_port" \
            -e CROWDB_STARTUP_MODE="$startup_mode" \
            -e CROWDB_MANAGEMENT_INTERFACE="$management_interface" \
            -e CROWDB_RDMA_DEVICE="${CROWDB_RDMA_DEVICE:-mlx5_0}" \
            -e CROWDB_RDMA_PORT="${CROWDB_RDMA_PORT:-1}" \
            "$image"
        ;;
    *)
        echo "unknown RDMA profile: $profile" >&2
        exit 2
        ;;
esac
