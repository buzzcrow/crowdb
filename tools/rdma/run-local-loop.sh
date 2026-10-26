#!/usr/bin/env bash
# Run the RDMA hardware smoke test in local or namespace loop mode.

set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
pixi_bin=${PIXI_BIN:-/home/cj/.pixi/bin/pixi}
test_bin=${CROWDB_RDMA_TEST_BIN:-$root_dir/lib/crowdb-rpc/build/crowdb_rpc_tests}
mode=${1:-local}
port_offset=${CROWDB_PORT_OFFSET:-0}
port=${CROWDB_RDMA_E2E_PORT:-$((19101 + port_offset))}
filter=${CROWDB_RDMA_E2E_FILTER:-RdmaHardwareTest.CompleteCmQpSendReceiveFlow}
server_pid=""

CLIENT_NS=ns1
SERVER_NS=ns2

usage() {
    cat <<'USAGE'
usage: run-local-loop.sh [local|namespace]

  local      Run client and server in root on one RDMA port (default).
  namespace  Run client in ns1 and server in ns2 on the two configured ports.

The setup must be prepared with:
  sudo tools/rdma/setup-rdma.sh local
  sudo tools/rdma/setup-rdma.sh namespace
USAGE
}

die() {
    echo "error: $*" >&2
    exit 1
}

if [[ "$mode" == "-h" || "$mode" == "--help" ]]; then
    usage
    exit 0
fi
[[ "$mode" == local || "$mode" == namespace ]] ||
    die "unsupported mode '$mode'; use local or namespace"
[[ $# -eq 1 ]] || die "unexpected argument: ${2:-}"

command -v ip >/dev/null || die "missing command: ip"
command -v rdma >/dev/null || die "missing command: rdma"
[[ -x "$pixi_bin" ]] || die "pixi executable not found: $pixi_bin"
[[ -x "$test_bin" ]] || die "test binary not found: $test_bin"

if [[ "$mode" == namespace && $EUID -ne 0 ]]; then
    exec sudo -E "$0" "$@"
fi

run_in_ns() {
    local ns="$1"
    shift
    if [[ "$ns" == root ]]; then
        env "$@"
    else
        ip netns exec "$ns" env "$@"
    fi
}

ns_exists() {
    ip netns list | awk '{print $1}' | grep -Fxq "$1"
}

first_device() {
    rdma link show | awk 'NR == 1 {sub("/1$", "", $2); print $2}'
}

device_netdev() {
    local device="$1"
    rdma link show | awk -v device="$device" \
        '$2 ~ ("^" device "(/|$)") && $0 ~ / netdev / {
            for (i = 1; i < NF; ++i) if ($i == "netdev") { print $(i + 1); exit }
        }'
}

root_netdev_for_device() {
    local device="$1" path
    for path in "/sys/class/infiniband/$device/device/net/"*; do
        [[ -e "$path" ]] || continue
        basename "$path"
        return 0
    done
}

ns_netdev_for_device() {
    local ns="$1" device="$2" netdev
    netdev=$(ip netns exec "$ns" rdma link show 2>/dev/null |
        awk -v device="$device" '$2 ~ ("^" device "(/|$)") && $0 ~ / netdev / {
            for (i = 1; i < NF; ++i) if ($i == "netdev") { print $(i + 1); exit }
        }')
    if [[ -z "$netdev" ]]; then
        netdev=$(ip -n "$ns" -o link show |
            awk -F': ' '$2 !~ /^lo([:@]|$)/ {sub(/@.*/, "", $2); print $2; exit}')
    fi
    printf '%s\n' "$netdev"
}

addr_for() {
    local ns="$1" dev="$2"
    run_in_ns "$ns" ip -4 -o addr show dev "$dev" scope global |
        awk 'NR == 1 {split($4, parts, "/"); print parts[1]}'
}

if [[ "$mode" == local ]]; then
    device=${CROWDB_RDMA_DEVICE:-$(first_device)}
    netdev=${CROWDB_RDMA_LOCAL_IF:-$(device_netdev "$device")}
    netdev=${netdev:-$(root_netdev_for_device "$device")}
    client_addr=${CROWDB_RDMA_LOCAL_ADDR:-$(addr_for root "$netdev")}
    server_addr=${CROWDB_RDMA_LOCAL_SERVER_ADDR:-$client_addr}
    [[ -n "$device" ]] || die "could not discover an RDMA device"
    [[ -n "$netdev" ]] || die "could not find the local Ethernet netdev"
    [[ -n "$client_addr" && -n "$server_addr" ]] || die "could not find local RDMA addresses"
    client_ns=root
    server_ns=root
    client_device=$device
    server_device=$device
    client_if=$netdev
    server_if=$netdev
else
    ns_exists "$CLIENT_NS" || die "namespace does not exist: $CLIENT_NS"
    ns_exists "$SERVER_NS" || die "namespace does not exist: $SERVER_NS"
    devices=($(ls -1 /sys/class/infiniband | sort))
    (( ${#devices[@]} >= 2 )) || die "namespace mode requires two RDMA ports"
    client_device=${CROWDB_RDMA_CLIENT_DEVICE:-${devices[0]}}
    server_device=${CROWDB_RDMA_SERVER_DEVICE:-${devices[1]}}
    client_if=${CROWDB_RDMA_CLIENT_IF:-$(ns_netdev_for_device "$CLIENT_NS" "$client_device")}
    server_if=${CROWDB_RDMA_SERVER_IF:-$(ns_netdev_for_device "$SERVER_NS" "$server_device")}
    client_addr=${CROWDB_RDMA_CLIENT_ADDR:-$(addr_for "$CLIENT_NS" "$client_if")}
    server_addr=${CROWDB_RDMA_SERVER_ADDR:-$(addr_for "$SERVER_NS" "$server_if")}
    [[ -n "$client_if" && -n "$server_if" ]] || die "could not find namespace netdevs"
    [[ -n "$client_addr" && -n "$server_addr" ]] || die "could not find namespace IPv4 addresses"
    client_ns=$CLIENT_NS
    server_ns=$SERVER_NS
fi

echo "RDMA $mode loop: client=$client_device/$client_if@$client_addr server=$server_device/$server_if@$server_addr port=$port"

cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill -- "-$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

wire_slot_capacity=${CROWDB_RDMA_WIRE_SLOT_CAPACITY:-1024}
sq_depth=${CROWDB_RDMA_SQ_DEPTH:-1024}
rq_depth=${CROWDB_RDMA_RQ_DEPTH:-1024}
posted_receive_target=${CROWDB_RDMA_POSTED_RECEIVE_TARGET:-$rq_depth}
connections=${CROWDB_RDMA_TEST_CONNECTIONS:-1}
cq_depth=${CROWDB_RDMA_CQ_DEPTH:-$((connections * (sq_depth + rq_depth)))}
send_arena_bytes=${CROWDB_RDMA_SEND_ARENA_BYTES:-$((sq_depth * wire_slot_capacity))}
receive_arena_bytes=${CROWDB_RDMA_RECEIVE_ARENA_BYTES:-$((posted_receive_target * wire_slot_capacity))}
echo "RDMA config: sq_depth=$sq_depth rq_depth=$rq_depth posted_receive_target=$posted_receive_target wire_slot_capacity=$wire_slot_capacity cq_depth=$cq_depth send_arena_bytes=$send_arena_bytes receive_arena_bytes=$receive_arena_bytes"
common_env=(CROWDB_RDMA_E2E=1 CROWDB_RDMA_E2E_PORT="$port"
    CROWDB_RDMA_SQ_DEPTH="$sq_depth" CROWDB_RDMA_RQ_DEPTH="$rq_depth"
    CROWDB_RDMA_POSTED_RECEIVE_TARGET="$posted_receive_target"
    CROWDB_RDMA_WIRE_SLOT_CAPACITY="$wire_slot_capacity" CROWDB_RDMA_CQ_DEPTH="$cq_depth"
    CROWDB_RDMA_SEND_ARENA_BYTES="$send_arena_bytes" CROWDB_RDMA_RECEIVE_ARENA_BYTES="$receive_arena_bytes")
server_command=(env "${common_env[@]}"
    CROWDB_RDMA_E2E_ROLE=server \
    CROWDB_RDMA_E2E_LISTEN_ADDR="$server_addr" \
    CROWDB_RDMA_DEVICE="$server_device" \
    "$pixi_bin" run "$test_bin" --gtest_filter="$filter")
if [[ "$server_ns" != root ]]; then
    server_command=(ip netns exec "$server_ns" "${server_command[@]}")
fi
setsid "${server_command[@]}" &
server_pid=$!

sleep 1
if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid"
    die "server exited before the client started"
fi
run_in_ns "$client_ns" env "${common_env[@]}" \
    CROWDB_RDMA_E2E_ROLE=client \
    CROWDB_RDMA_E2E_LISTEN_ADDR="$server_addr" \
    CROWDB_RDMA_E2E_CONNECT_ADDR="$server_addr" \
    CROWDB_RDMA_SOURCE_ADDR="$client_addr" \
    CROWDB_RDMA_DEVICE="$client_device" \
    "$pixi_bin" run "$test_bin" --gtest_filter="$filter"

wait "$server_pid"
server_pid=""
echo "RDMA $mode loop test passed."
