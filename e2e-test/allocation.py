# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Shared owned network, node mounts and release-container allocation."""

import ipaddress
import json
import uuid

LABEL = "org.crowdb.e2e.run"


def create_network(cluster):
    # Record before allocation because a timed-out request may have succeeded.
    cluster.network_created = True
    network_args, subnet = (), None
    if cluster.advertise:
        identity = uuid.UUID(cluster.run_id.removeprefix("crowdb-e2e-"))
        subnet = ipaddress.ip_network(f"10.{identity.bytes[0]}.{identity.bytes[1]}.0/24")
        if subnet.num_addresses < cluster.node_count + 3:
            raise ValueError("fixture network cannot fit the advertised nodes")
        network_args = ("--subnet", str(subnet))
    cluster.command("network", "create", "--label", f"{LABEL}={cluster.run_id}", *network_args, cluster.run_id)
    return subnet


def node_data(cluster, index, prepare_logs=True):
    data = (cluster.data_root / f"node{index}" / "data").resolve()
    data.mkdir(parents=True)
    if prepare_logs:
        (data / "log").mkdir()
        (data / "log" / "container.log").touch()
    cluster.data_paths.append(data)
    (cluster.artifacts / "mounts.json").write_text(json.dumps({
        "container_data": "/opt/crowdb/data",
        "host_data": [str(path) for path in cluster.data_paths]}, indent=2))
    return data


def start_node(cluster, index, data, arguments, address=None, environment=(), mounts=(),
               cpus="0.5", memory="512m", running=True):
    if not running and address is None:
        raise ValueError("a deferred node needs an explicit advertised address")
    name = f"{cluster.run_id}-node{index}"
    # A timed-out allocation can already exist; record it before contacting Docker.
    cluster.containers.append(name)
    cluster.servers.append(name)
    network_args = ("--ip", address) if address else ()
    environment_args = tuple(item for value in environment for item in ("--env", value))
    mount_args = tuple(item for value in mounts for item in ("--mount", value))
    operation = ("run", "-d") if running else ("create",)
    cluster.command(*operation, "--name", name, "--label", f"{LABEL}={cluster.run_id}",
                    "--network", cluster.run_id, "--network-alias", f"node{index}",
                    *network_args, "--cpus", cpus, "--memory", memory, "--pids-limit", "256",
                    "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                    *mount_args, *environment_args, "--ulimit", "core=-1", cluster.image, *arguments)
    info = json.loads(cluster.command("inspect", name))[0]
    network = info["NetworkSettings"]["Networks"][cluster.run_id]
    if not running:
        if network.get("IPAMConfig", {}).get("IPv4Address") != address:
            raise AssertionError("deferred node lost its explicit network address")
        return address
    return network["IPAddress"]
