#!/usr/bin/env bash
# Run the RDMA CM/QP SEND/RECV smoke across two host-network containers using
# one local HCA address. This intentionally bypasses the multi-service preview
# profile so the test exercises only the transport path.
set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
image=${CROWDB_RDMA_TEST_IMAGE:-crowdb-node:dev}
device=${CROWDB_RDMA_DEVICE:-rocep35s0f0}
port=${CROWDB_RDMA_E2E_PORT:-19161}
client_address=${CROWDB_RDMA_LOCAL_ADDR:-10.200.1.1}
server_address=${CROWDB_RDMA_LOCAL_SERVER_ADDR:-$client_address}
test_bin=/tmp/crowdb_rpc_tests
server_name=crowdb-r239-rdma-server
client_name=crowdb-r239-rdma-client

cleanup() {
    docker rm -f "$server_name" "$client_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

[[ -x "$root_dir/lib/crowdb-rpc/build/crowdb_rpc_tests" ]] || {
    echo "missing RDMA test binary; run pixi run build-tests" >&2
    exit 2
}
[[ -d "$root_dir/.pixi/envs/default/lib" ]] || {
    echo "missing Pixi runtime libraries" >&2
    exit 2
}
command -v docker >/dev/null || { echo 'missing docker' >&2; exit 2; }

cleanup
common=(--network=host --device /dev/infiniband/uverbs0 --device /dev/infiniband/rdma_cm
    --cap-add IPC_LOCK --ulimit memlock=67108864:67108864
    --entrypoint "$test_bin"
    -v "$root_dir/lib/crowdb-rpc/build/crowdb_rpc_tests:$test_bin:ro"
    -v "$root_dir/.pixi/envs/default/lib:/tmp/crowdb-libs:ro"
    -v "$root_dir/.pixi/envs/default/etc/libibverbs.d:/tmp/crowdb-ibverbs.d:ro"
    -e LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/opt/crowdb/lib:/tmp/crowdb-libs
    -e LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libibverbs.so.1:/usr/lib/x86_64-linux-gnu/librdmacm.so.1
    -e RDMAV_DRIVERS_DIR=/opt/crowdb/lib/libibverbs
    -e IBV_CONFIG_DIR=/tmp/crowdb-ibverbs.d
    -e CROWDB_RDMA_E2E=1 -e CROWDB_RDMA_E2E_PORT="$port"
    -e CROWDB_RDMA_DEVICE="$device")

echo "container RDMA loop: image=$image device=$device client=$client_address server=$server_address port=$port"
docker run -d --name "$server_name" "${common[@]}" \
    -e CROWDB_RDMA_E2E_ROLE=server -e CROWDB_RDMA_E2E_LISTEN_ADDR="$server_address" "$image" \
    --gtest_filter=RdmaHardwareTest.CompleteCmQpSendReceiveFlow >/dev/null
sleep 1
docker run --rm --name "$client_name" "${common[@]}" \
    -e CROWDB_RDMA_E2E_ROLE=client -e CROWDB_RDMA_E2E_CONNECT_ADDR="$server_address" \
    -e CROWDB_RDMA_SOURCE_ADDR="$client_address" -e CROWDB_RDMA_E2E_LISTEN_ADDR="$server_address" "$image" \
    --gtest_filter=RdmaHardwareTest.CompleteCmQpSendReceiveFlow
docker wait "$server_name" >/dev/null
echo 'container RDMA loop passed.'
