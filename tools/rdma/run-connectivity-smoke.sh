#!/usr/bin/env bash
# Verify the host RDMA data path before running container or benchmark tests.
set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mode=${1:-local}
device=${CROWDB_RDMA_DEVICE:-}
port=${CROWDB_RDMA_PORT:-1}
netdev=${CROWDB_RDMA_LOCAL_IF:-}
address=${CROWDB_RDMA_LOCAL_ADDR:-}

[[ "$mode" == local || "$mode" == namespace ]] || { echo 'usage: run-connectivity-smoke.sh [local|namespace]' >&2; exit 2; }
command -v ibv_devinfo >/dev/null || { echo 'missing ibv_devinfo' >&2; exit 2; }
command -v rdma >/dev/null || { echo 'missing rdma' >&2; exit 2; }

if [[ "$mode" == namespace ]]; then
    exec "$root_dir/tools/rdma/run-local-loop.sh" namespace
fi

if [[ -z "$device" ]]; then
    device=$(rdma link show | awk 'NR == 1 {sub("/1$", "", $2); print $2}')
fi
if [[ -z "$netdev" ]]; then
    netdev=$(rdma link show | awk -v d="$device" '$2 ~ ("^" d "(/|$)") && / netdev / {for (i = 1; i < NF; ++i) if ($i == "netdev") {print $(i + 1); exit}}')
fi
if [[ -z "$address" && -n "$netdev" ]]; then
    address=$(ip -4 -o addr show dev "$netdev" scope global | awk 'NR == 1 {split($4, p, "/"); print p[1]}')
fi
[[ -n "$device" && -n "$netdev" && -n "$address" ]] || {
    echo "unable to resolve RDMA device/netdev/address" >&2
    exit 1
}

echo "RDMA device=$device port=$port netdev=$netdev address=$address"
rdma link show
ibv_devinfo -d "$device" -i "$port"

export CROWDB_RDMA_DEVICE="$device"
export CROWDB_RDMA_PORT="$port"
export CROWDB_RDMA_LOCAL_IF="$netdev"
export CROWDB_RDMA_LOCAL_ADDR="$address"
exec "$root_dir/tools/rdma/run-local-loop.sh" local
