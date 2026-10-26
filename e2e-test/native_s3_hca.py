# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit shared-HCA profile; isolated storage directories and loopback endpoints."""

import ipaddress
import json
import socket
import uuid
from pathlib import Path

from allocation import LABEL, observed_address
from native_s3_fixture import NativeS3Cluster


def loopback_subnet(cluster):
    identity = uuid.UUID(cluster.run_id.removeprefix("crowdb-e2e-"))
    subnet = ipaddress.ip_network(f"127.{identity.bytes[0]}.{identity.bytes[1]}.0/24")
    (cluster.artifacts / "host-network.json").write_text(json.dumps({
        "mode": "host", "loopback_subnet": str(subnet), "shared_hca": cluster.sdk_environment,
        "note": "Same-host HCA local loop, not cross-host or isolated-NIC performance",
    }, indent=2))
    return subnet


def start_host_node(cluster, index, data, arguments, address, environment, mounts, memory, pids_limit):
    if not ipaddress.ip_address(address).is_loopback:
        raise ValueError("HCA fixture service endpoints must use its owned loopback range")
    arguments = list(arguments)
    role = arguments[1]
    if role == "kv":
        with socket.socket() as reservation:
            reservation.bind(("0.0.0.0", 0))
            arguments[arguments.index("--ports") + 1] = str(reservation.getsockname()[1])
    for mount in mounts:
        fields = dict(part.split("=", 1) for part in mount.split(",") if "=" in part)
        source = Path(fields["source"]).resolve()
        if source.parent != cluster.artifacts.resolve() or source.suffix != ".toml":
            raise ValueError("HCA profile only rewrites its own generated service configuration")
        source.write_text(source.read_text().replace('"0.0.0.0:', f'"{address}:'))
    env = {"CROWDB_RDMA_ENABLED": "0"}
    devices = ()
    if role == "diskio":
        env.update(cluster.server_rdma_environment)
        devices = cluster.hca_devices
    name = f"{cluster.run_id}-node{index}"
    cluster.containers.append(name)
    cluster.servers.append(name)
    env_args = tuple(item for value in (*environment, *(f"{k}={v}" for k, v in env.items()))
                     for item in ("--env", value))
    mount_args = tuple(item for value in mounts for item in ("--mount", value))
    cluster.command("run", "-d", "--name", name, "--label", f"{LABEL}={cluster.run_id}",
                    "--network", "host", "--cpu-shares", "1024", "--memory", memory,
                    "--pids-limit", str(pids_limit), "--env", "TOKIO_WORKER_THREADS=1",
                    "--ulimit", "memlock=-1:-1", *devices, *env_args, *mount_args,
                    "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                    cluster.image, *arguments)
    info = json.loads(cluster.command("inspect", name))[0]
    return observed_address(cluster, info, address)


class NativeS3HcaCluster(NativeS3Cluster):
    host_network = True

    def __init__(self, image, artifacts, bundle, *, address, device, **kwargs):
        ipaddress.ip_address(address)
        verbs = sorted((Path("/sys/class/infiniband") / device / "device/infiniband_verbs").glob("uverbs*"))
        if len(verbs) != 1 or not Path("/dev/infiniband/rdma_cm").exists():
            raise ValueError("explicit HCA profile requires one visible verbs device and rdma_cm")
        paths = ("/dev/infiniband/rdma_cm", f"/dev/infiniband/{verbs[0].name}")
        self.hca_devices = tuple(item for path in paths for item in ("--device", path))
        self.sdk_environment = {"CROWDB_NATIVE_S3_RDMA": "1", "CROWDB_RDMA_SOURCE_ADDR": address,
                                "CROWDB_RDMA_DEVICE": device}
        self.server_rdma_environment = {
            "CROWDB_RDMA_ENABLED": "1", "CROWDB_RDMA_ALLOW_TCP_FALLBACK": "1",
            "CROWDB_RDMA_SOURCE_ADDR": address, "CROWDB_RDMA_DEVICE": device,
            "CROWDB_RDMA_WIRE_LAYOUT": "1", "CROWDB_RDMA_CQ_COUNT": "1",
            "CROWDB_RDMA_CQ_DEPTH": "512", "CROWDB_RDMA_SQ_DEPTH": "16",
            "CROWDB_RDMA_RQ_DEPTH": "16", "CROWDB_RDMA_POSTED_RECEIVE_TARGET": "16",
            "CROWDB_RDMA_SEND_ARENA_BYTES": str(16 << 20),
            "CROWDB_RDMA_RECEIVE_ARENA_BYTES": str(16 * ((1 << 20) + 1024)),
            "CROWDB_RDMA_AGGREGATION_ENABLED": "0",
        }
        super().__init__(image, artifacts, bundle, **kwargs)

    def diskio_port(self, _index):
        with socket.socket() as reservation:
            reservation.bind((self.sdk_environment["CROWDB_RDMA_SOURCE_ADDR"], 0))
            return reservation.getsockname()[1]
