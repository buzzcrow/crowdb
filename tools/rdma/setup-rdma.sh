#!/usr/bin/env bash
# Prepare the two RDMA smoke-test modes for this ConnectX-5 host.
#
# local uses one port and one root-network address. namespace uses both ports
# and places the client and server netdevs in ns1 and ns2.

set -Eeuo pipefail

# Print a command first, then execute the same argv. This keeps the setup
# output readable without Bash xtrace echoing every internal shell statement.
run_cmd() {
    printf '****> '
    printf '%q ' "$@"
    printf '\n'
    "$@"
}

run_shell() {
    local command="$1"
    printf '****> %s\n' "$command"
    bash -o pipefail -c "$command"
}

CLIENT_NS="ns1"
SERVER_NS="ns2"
CLIENT_IF="${CROWDB_RDMA_CLIENT_IF:-enp35s0f0np0}"
SERVER_IF="${CROWDB_RDMA_SERVER_IF:-enp35s0f1np1}"
CLIENT_RDMA=""
SERVER_RDMA=""
CLIENT_IP="10.200.1.1/30"
SERVER_IP="10.200.1.2/30"
SERVER_ADDR="10.200.1.2"
SERVER_PID=""
MODE="${1:-local}"
NS_USER="${CROWDB_RDMA_NS_USER:-${SUDO_USER:-${USER:-cj}}}"

sudo_run() {
    run_cmd sudo "$@"
}

ns_exists() {
    ip netns list | awk '{print $1}' | grep -Fxq "$1"
}

netdev_in_ns() {
    local ns="$1" dev="$2"
    sudo_run ip netns exec "$ns" ip link show dev "$dev" >/dev/null 2>&1
}

wait_for_rdma() {
    local i
    for i in $(seq 1 10); do
        CLIENT_RDMA=$(rdma link show | awk -v netdev="$CLIENT_IF" \
            '$0 ~ " netdev " netdev " " {sub("/1$", "", $2); print $2; exit}')
        if [[ "$MODE" == local ]]; then
            SERVER_RDMA="$CLIENT_RDMA"
        else
            SERVER_RDMA=$(rdma link show | awk -v netdev="$SERVER_IF" \
                '$0 ~ " netdev " netdev " " {sub("/1$", "", $2); print $2; exit}')
        fi
        # Some rdma-core versions omit the netdev field. The device order is
        # stable for this two-port adapter, so retain a safe fallback after
        # confirming both expected netdevs exist in the root or namespaces.
        if [[ -z "$CLIENT_RDMA" && -z "$SERVER_RDMA" ]] &&
           (ip link show dev "$CLIENT_IF" >/dev/null 2>&1 || netdev_in_ns ns1 "$CLIENT_IF") &&
           (ip link show dev "$SERVER_IF" >/dev/null 2>&1 || netdev_in_ns ns2 "$SERVER_IF"); then
            CLIENT_RDMA="rocep35s0f0"
            SERVER_RDMA="rocep35s0f1"
        fi
        if [[ -n "$CLIENT_RDMA" && -n "$SERVER_RDMA" && \
              -e "/sys/class/infiniband/${CLIENT_RDMA}" && \
              -e "/sys/class/infiniband/${SERVER_RDMA}" ]]; then
            return 0
        fi
        sleep 1
    done
    echo "RDMA links for ${CLIENT_IF} and ${SERVER_IF} did not appear:" >&2
    rdma link show >&2 || true
    return 1
}

check_commands() {
    local cmd
    for cmd in ip rdma ethtool modprobe ping setcap getcap realpath sysctl; do
        command -v "$cmd" >/dev/null || {
            echo "Missing command: $cmd" >&2
            return 1
        }
    done
}

enable_local_rdma_loopback() {
    if [[ "$MODE" == local ]]; then
        [[ -n "$CLIENT_IF" && -n "$SERVER_IF" ]] || return 0
        sudo_run sysctl -w net.ipv4.conf.all.accept_local=1
        sudo_run sysctl -w "net.ipv4.conf.${CLIENT_IF}.accept_local=1"
        return 0
    fi
    [[ -n "$CLIENT_IF" && -n "$SERVER_IF" ]] || return 0
    for ns in "$CLIENT_NS" "$SERVER_NS"; do
        sudo_run ip netns exec "$ns" sysctl -w net.ipv4.conf.all.accept_local=1
    done
    if [[ -n "$CLIENT_IF" ]]; then
        sudo_run ip netns exec "$CLIENT_NS" sysctl -w "net.ipv4.conf.${CLIENT_IF}.accept_local=1"
    fi
    if [[ -n "$SERVER_IF" ]]; then
        sudo_run ip netns exec "$SERVER_NS" sysctl -w "net.ipv4.conf.${SERVER_IF}.accept_local=1"
    fi
}

grant_namespace_access() {
    local ip_bin
    ip_bin=$(realpath "$(command -v ip)")
    [[ -n "$NS_USER" && "$NS_USER" != root ]] || return 0
    echo "Granting $NS_USER access to enter RDMA test namespaces through $ip_bin"
    sudo_run setcap cap_sys_admin+ep "$ip_bin"
    sudo_run getcap "$ip_bin"
}

check_netdev_driver() {
    local ns="$1" dev="$2"
    if netdev_in_ns "$ns" "$dev"; then
        run_shell "sudo ip netns exec $ns ethtool -i $dev"
        run_shell "sudo ip netns exec $ns ethtool $dev | grep -E 'Speed:|Link detected:'" || true
    elif ip link show dev "$dev" >/dev/null 2>&1; then
        run_shell "ethtool -i $dev"
        run_shell "ethtool $dev | grep -E 'Speed:|Link detected:'" || true
    else
        echo "netdev not found in root or its namespace: $dev" >&2
        return 1
    fi
}

check_rdma() {
    sudo_run modprobe mlx5_core
    sudo_run modprobe mlx5_ib
    wait_for_rdma
    echo "RDMA client=${CLIENT_RDMA} server=${SERVER_RDMA}"
}

apply_namespaces() {
    if [[ "$MODE" == local ]]; then
        sudo_run ip link set dev "$CLIENT_IF" up
        sudo_run ip link set dev "$CLIENT_IF" mtu 9000
        sudo_run ip addr replace "$CLIENT_IP" dev "$CLIENT_IF"
        return 0
    fi
    if [[ -z "$CLIENT_IF" || -z "$SERVER_IF" ]]; then
        echo "RDMA devices are present, but no Ethernet netdev is associated with both ports." >&2
        return 1
    fi
    for ns in "$CLIENT_NS" "$SERVER_NS"; do
        if ! ns_exists "$ns"; then
            sudo_run ip netns add "$ns"
        fi
    done

    # In shared mode the RDMA devices remain visible in both namespaces.
    # Moving only the physical Ethernet netdevs isolates the IP/RDMA-CM paths.
    if ! netdev_in_ns "$CLIENT_NS" "$CLIENT_IF"; then
        if ip link show dev "$CLIENT_IF" >/dev/null 2>&1; then
            sudo_run ip link set dev "$CLIENT_IF" netns "$CLIENT_NS"
        fi
    fi
    if ! netdev_in_ns "$SERVER_NS" "$SERVER_IF"; then
        if ip link show dev "$SERVER_IF" >/dev/null 2>&1; then
            sudo_run ip link set dev "$SERVER_IF" netns "$SERVER_NS"
        fi
    fi

    sudo_run ip -n "$CLIENT_NS" link set lo up
    sudo_run ip -n "$SERVER_NS" link set lo up
    sudo_run ip -n "$CLIENT_NS" link set "$CLIENT_IF" up
    sudo_run ip -n "$SERVER_NS" link set "$SERVER_IF" up

    # Jumbo Ethernet frames allow the RoCE provider to use a 4096-byte MTU.
    sudo_run ip -n "$CLIENT_NS" link set "$CLIENT_IF" mtu 9000
    sudo_run ip -n "$SERVER_NS" link set "$SERVER_IF" mtu 9000

    sudo_run ip -n "$CLIENT_NS" addr replace "$CLIENT_IP" dev "$CLIENT_IF"
    sudo_run ip -n "$SERVER_NS" addr replace "$SERVER_IP" dev "$SERVER_IF"
}

show_status() {
    if [[ "$MODE" == local ]]; then
        echo "=== local ==="
        sudo_run ip -br addr show dev "$CLIENT_IF"
        sudo_run rdma link show
        sudo_run ethtool -i "$CLIENT_IF"
        return 0
    fi
    if [[ -z "$CLIENT_IF" || -z "$SERVER_IF" ]]; then
        echo "No per-port netdevs to show; RDMA devices remain in shared host mode."
        return 0
    fi
    echo "=== ns1 / client ==="
    sudo_run ip netns exec "$CLIENT_NS" ip -br addr
    sudo_run ip netns exec "$CLIENT_NS" rdma link show
    sudo_run ip netns exec "$CLIENT_NS" ethtool -i "$CLIENT_IF"
    run_shell "sudo ip netns exec $CLIENT_NS ethtool $CLIENT_IF | grep -E 'Speed:|Link detected:'" || true
    sudo_run ip netns exec "$CLIENT_NS" ip -d link show dev "$CLIENT_IF"

    echo "=== ns2 / server ==="
    sudo_run ip netns exec "$SERVER_NS" ip -br addr
    sudo_run ip netns exec "$SERVER_NS" rdma link show
    sudo_run ip netns exec "$SERVER_NS" ethtool -i "$SERVER_IF"
    run_shell "sudo ip netns exec $SERVER_NS ethtool $SERVER_IF | grep -E 'Speed:|Link detected:'" || true
    sudo_run ip netns exec "$SERVER_NS" ip -d link show dev "$SERVER_IF"

    echo "=== connectivity ==="
    sudo_run ip netns exec "$CLIENT_NS" ping -I "$CLIENT_IF" -c 3 -W 1 "$SERVER_ADDR"
}

cleanup_namespaces() {
    set +e
    if [[ -n "$SERVER_PID" ]]; then
        sudo_run kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi

    if netdev_in_ns "$CLIENT_NS" "$CLIENT_IF"; then
        sudo_run ip -n "$CLIENT_NS" link set "$CLIENT_IF" netns 1
    fi
    if netdev_in_ns "$SERVER_NS" "$SERVER_IF"; then
        sudo_run ip -n "$SERVER_NS" link set "$SERVER_IF" netns 1
    fi

    sudo_run ip netns del "$CLIENT_NS" 2>/dev/null || true
    sudo_run ip netns del "$SERVER_NS" 2>/dev/null || true
}


setup() {
    sudo_run -v
    check_commands
    check_rdma
    apply_namespaces
    enable_local_rdma_loopback
    grant_namespace_access
    show_status
    echo
    echo "One-shot $MODE-mode RDMA setup complete."
}

clean() {
    sudo_run -v
    cleanup_namespaces
}

case "$MODE" in
    local|namespace) setup ;;
    clean) clean ;;
    -h|--help)
        echo "Usage: $0 [local|namespace|clean]"
        ;;
    *)
        echo "Usage: $0 [local|namespace|clean]" >&2
        exit 2
        ;;
esac
