# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Direct Docker resource creation; the OCI node owns all cluster preparation."""

import json
import math
import uuid

from docker_local_state import CLUSTER, OWNER, check_daemon, load, require_owned, seal

CONTAINER_PORTS = (9090, 16000, 17000, 18000)


def container_ports(record):
    return (9090, 9091, 9092, 9093) if record["version"] == 1 else CONTAINER_PORTS


def arguments(record):
    return ["--label", f"{CLUSTER}={record['cluster']}", "--label", f"{OWNER}={record['owner']}"]


def ensure(docker, record, kind, name):
    names = {item.get("Name", "").lstrip("/"): item for item in docker.objects(kind)}
    if name in names:
        require_owned(names[name], record, kind)
        return names[name]
    check_daemon(docker, record)
    docker.call(kind, "create", *arguments(record), name)
    item = json.loads(docker.call(kind, "inspect", name))[0]
    require_owned(item, record, kind)
    return item


def accepted(docker, args):
    previous = load(args.cluster)
    if previous:
        check_daemon(docker, previous)
        for key, requested in (("nodes", args.nodes), ("port_base", args.port_base),
                               ("image", args.image), ("cpus", args.cpus), ("memory_mib", args.memory_mib)):
            if requested is not None and previous[key] != requested:
                raise ValueError(f"{key} differs from retained cluster inputs")
        return previous
    image = args.image or "crowdb-node:dev"
    try:
        metadata = json.loads(docker.call("image", "inspect", image))[0]
    except RuntimeError as error:
        raise ValueError("image is not loaded; run pixi run oci-build first") from error
    if (metadata["Os"], metadata["Architecture"]) != ("linux", "amd64"):
        raise ValueError("local node image must be Linux amd64")
    if metadata.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.source") != "https://github.com/buzzcrow/crowdb":
        raise ValueError("image is not a CROWDB node image")
    record = {"version": 2, "cluster": args.cluster, "owner": str(uuid.uuid4()), "runtime": docker.identity(),
              "nodes": args.nodes if args.nodes is not None else 1, "port_base": args.port_base if args.port_base is not None else 9090, "image": image,
              "image_id": metadata["Id"], "cpus": args.cpus if args.cpus is not None else 1,
              "memory_mib": args.memory_mib if args.memory_mib is not None else 1536}
    if record["nodes"] not in (1, 3) or not 1 <= record["port_base"] <= 65512:
        raise ValueError("local cluster requires one or three nodes and a valid port range")
    if not math.isfinite(record["cpus"]) or record["cpus"] <= 0 or record["memory_mib"] < 512:
        raise ValueError("CPU must be positive and memory at least 512 MiB")
    seal(record)
    return record


def verify_container(item, record, index, volume, network):
    require_owned(item, record, "container")
    host = item["HostConfig"]
    base = record["port_base"] + (index - 1) * 10
    ports = {f"{port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(base+offset)}]
             for offset, port in enumerate(container_ports(record))}
    expected_environment = {"CROWDB_CLUSTER": record["cluster"], "CROWDB_INITIAL_VOTERS": str(record["nodes"])}
    for offset, protocol in enumerate((None, "S3", "ICEBERG", "DATASET")):
        if protocol:
            expected_environment[f"CROWDB_{protocol}_PUBLIC_URI"] = f"http://127.0.0.1:{base+offset}"
    environment = dict(value.split("=", 1) for value in item["Config"].get("Env", []) if "=" in value)
    mounts = [mount for mount in item["Mounts"] if mount["Destination"] == "/opt/crowdb/data"]
    if (item["Image"] != record["image_id"] or host["NanoCpus"] != round(record["cpus"] * 1_000_000_000)
            or host["Memory"] != record["memory_mib"] * 1024 * 1024
            or host.get("PortBindings") != ports or host.get("NetworkMode") != network
            or len(mounts) != 1 or mounts[0].get("Type") != "volume" or mounts[0].get("Name") != volume
            or not mounts[0].get("RW")
            or any(environment.get(key) != value for key, value in expected_environment.items())):
        raise ValueError("retained container resources, endpoints, image or deployment inputs differ")


def run(docker, record):
    check_daemon(docker, record)
    # Inspect the sealed local image ID before resource effects; run never pulls.
    docker.call("image", "inspect", record["image_id"])
    network = "crowdb-" + record["cluster"]
    bridge = ensure(docker, record, "network", network)
    if bridge.get("Driver") != "bridge":
        raise ValueError("local cluster requires its owned Docker bridge")
    containers = {item["Name"].lstrip("/"): item for item in docker.objects("container")}
    for index in range(1, record["nodes"] + 1):
        name = f"{network}-{index}"
        volume = name + "-data"
        ensure(docker, record, "volume", volume)
        if name in containers:
            item = containers[name]
            verify_container(item, record, index, volume, network)
            if not item["State"]["Running"]:
                check_daemon(docker, record)
                docker.call("start", item["Id"])
            continue
        base = record["port_base"] + (index - 1) * 10
        command = ["run", "-d", "--pull", "never", "--name", name, *arguments(record),
                   "--network", network, "--cpus", record["cpus"], "--memory", f"{record['memory_mib']}m",
                   "--mount", f"type=volume,source={volume},target=/opt/crowdb/data",
                   "-e", "CROWDB_CLUSTER=" + record["cluster"], "-e", f"CROWDB_INITIAL_VOTERS={record['nodes']}"]
        for offset, protocol in enumerate((None, "S3", "ICEBERG", "DATASET")):
            command += ["-p", f"127.0.0.1:{base+offset}:{container_ports(record)[offset]}"]
            if protocol:
                command += ["-e", f"CROWDB_{protocol}_PUBLIC_URI=http://127.0.0.1:{base+offset}"]
        check_daemon(docker, record)
        identity = docker.call(*command, record["image_id"])
        item = json.loads(docker.call("container", "inspect", identity))[0]
        verify_container(item, record, index, volume, network)
    print(f"Started {record['nodes']} node(s) for {record['cluster']}; UI http://127.0.0.1:{record['port_base']}/")
    print("Node monitors continue discovery and preparation; inspect progress in the UI or with docker logs.")
