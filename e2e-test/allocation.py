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
               cpus="0.5", memory="512m", running=True, preserve_network=False):
    if not running and address is None:
        raise ValueError("a deferred node needs an explicit advertised address")
    name = f"{cluster.run_id}-node{index}"
    network_owner = None
    if preserve_network:
        if address is None or not running:
            raise ValueError("a retained network needs a running node with an explicit address")
        network_owner = name + "-network"
        cluster.containers.append(network_owner)
        cluster.command("run", "-d", "--name", network_owner, "--label", f"{LABEL}={cluster.run_id}",
                        "--network", cluster.run_id, "--ip", address, "--cpus", "0.05",
                        "--memory", "16m", "--pids-limit", "8", "--entrypoint", "/bin/sleep",
                        cluster.image, "infinity")
        owner = json.loads(cluster.command("inspect", network_owner))[0]
        if owner["NetworkSettings"]["Networks"][cluster.run_id]["IPAddress"] != address:
            raise AssertionError("retained network lost its explicit address")
    # A timed-out allocation can already exist; record it before contacting Docker.
    cluster.containers.append(name)
    cluster.servers.append(name)
    network_args = (("--network", f"container:{network_owner}") if network_owner else
                    ("--network", cluster.run_id, "--network-alias", f"node{index}",
                     *(("--ip", address) if address else ())))
    environment_args = tuple(item for value in environment for item in ("--env", value))
    mount_args = tuple(item for value in mounts for item in ("--mount", value))
    data_mount = ("--mount", f"type=bind,source={data},target=/opt/crowdb/data") if data is not None else ()
    cpus = str(getattr(cluster, "node_cpus", cpus))
    operation = ("run", "-d") if running else ("create",)
    cluster.command(*operation, "--name", name, "--label", f"{LABEL}={cluster.run_id}",
                    *network_args, "--cpus", cpus, "--memory", memory, "--pids-limit", "256",
                    *data_mount,
                    *mount_args, *environment_args, "--ulimit", "core=-1", cluster.image, *arguments)
    info = json.loads(cluster.command("inspect", name))[0]
    if network_owner:
        if info["HostConfig"]["NetworkMode"] not in (f"container:{network_owner}", f"container:{owner['Id']}"):
            raise AssertionError("server lost its retained network namespace")
        return address
    network = info["NetworkSettings"]["Networks"][cluster.run_id]
    if not running:
        if network.get("IPAMConfig", {}).get("IPv4Address") != address:
            raise AssertionError("deferred node lost its explicit network address")
        return address
    return network["IPAddress"]
