# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Direct Docker and unchanged Compose examples with exact test ownership."""

import json
import os
from pathlib import Path
import subprocess

from fixture import LABEL, request, wait

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "com.docker.compose.project"


def ports(base):
    return {name: base + offset for offset, name in enumerate(("UI", "S3", "ICEBERG", "DATASET"))}


def compose_environment(cluster):
    values = dict(os.environ, CROWDB_IMAGE=cluster.image, CROWDB_CLUSTER=cluster.run_id)
    for index in range(cluster.node_count):
        for name, port in ports(cluster.port_base + index * 10).items():
            key = f"CROWDB_NODE{index + 1}_{name}_PORT" if cluster.node_count == 3 else f"CROWDB_{name}_PORT"
            values[key] = str(port)
    return values


def compose(cluster, *arguments):
    filename = "compose-three.yaml" if cluster.node_count == 3 else "compose.yaml"
    command = ["docker", "compose", "--project-name", cluster.run_id,
               "--file", str(ROOT / "container/oci-image" / filename), *arguments]
    result = subprocess.run(command, env=compose_environment(cluster), capture_output=True,
                            text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"Compose {arguments[0]} failed: {result.stdout}\n{result.stderr}")
    return result.stdout


def inventory(cluster, kind):
    label = PROJECT if cluster.implementation == "compose" else LABEL
    args = ("ps", "-aq") if kind == "container" else (kind, "ls", "-q")
    identities = cluster.command(*args, "--filter", f"label={label}={cluster.run_id}").splitlines()
    if not identities:
        return []
    return json.loads(cluster.command(kind, "inspect", *identities))


def validate_container(cluster, item):
    label = PROJECT if cluster.implementation == "compose" else LABEL
    labels = item.get("Config", {}).get("Labels", {})
    if labels.get(label) != cluster.run_id or item.get("Image") != cluster.image_id:
        raise ValueError("reference container ownership or image changed")
    if cluster.implementation == "compose":
        expected = {"node"} if cluster.node_count == 1 else {"node1", "node2", "node3"}
        if labels.get("com.docker.compose.service") not in expected:
            raise ValueError("unrelated service shares the reference Compose project")
    if item["Name"].removeprefix("/") not in cluster.expected_names:
        raise ValueError("unexpected reference container name")


def start(cluster):
    for kind in ("container", "network", "volume"):
        if inventory(cluster, kind):
            raise ValueError("reference scope already contains runtime resources")
    (cluster.artifacts / "runtime-intent.json").write_text(json.dumps({
        "project": cluster.run_id, "implementation": cluster.implementation,
        "image_id": cluster.image_id, "nodes": cluster.node_count,
        "port_base": cluster.port_base}, indent=2))
    cluster.runtime_accepted = True
    if cluster.implementation == "compose":
        cluster.command("compose", "version", "--short")
        compose(cluster, "up", "--detach", "--no-build", "--pull", "never")
        return
    cluster.network_created = True
    cluster.command("network", "create", "--label", f"{LABEL}={cluster.run_id}", cluster.run_id)
    create_docker_nodes(cluster, range(cluster.node_count))
    start_docker_nodes(cluster)


def create_docker_nodes(cluster, indexes):
    for index in indexes:
        name = cluster.expected_names[index]
        volume = f"{name}-data"
        cluster.command("volume", "create", "--label", f"{LABEL}={cluster.run_id}", volume)
        cluster.containers.append(name)
        arguments = ["create", "--name", name, "--label", f"{LABEL}={cluster.run_id}",
                     "--network", cluster.run_id, "--cpus", "1", "--memory", "1536m",
                     "--mount", f"type=volume,source={volume},target=/opt/crowdb/data",
                     "--env", f"CROWDB_CLUSTER={cluster.run_id}",
                     "--env", f"CROWDB_INITIAL_VOTERS={cluster.node_count}"]
        for protocol, port in ports(cluster.port_base + index * 10).items():
            internal = 9090 + (port - cluster.port_base - index * 10)
            arguments += ["--publish", f"127.0.0.1:{port}:{internal}"]
            if protocol != "UI":
                arguments += ["--env", f"CROWDB_{protocol}_PUBLIC_URI=http://127.0.0.1:{port}"]
        cluster.command(*arguments, cluster.image)


def start_docker_nodes(cluster):
    # A different start order must not require a peer-address or bootstrap helper.
    order = list(reversed(cluster.expected_names)) if cluster.node_count == 3 else cluster.expected_names
    if cluster.node_count == 3:
        for name in order[:2]:
            cluster.command("start", name)
        origin = cluster.endpoint(2)
        wait(lambda: request(origin, "/api/cluster/status"),
             lambda report: report["phase"] == "waiting", "two-node formation waiting for third voter")
        report = json.loads(cluster.invoke("cluster", "check", "--wait", "2", "--json",
                                           endpoint=origin, expected_exit=1))
        assert report["phase"] == "waiting" and report["observation"]["initial_voters"] == 3
        assert request(origin, "/api/nodes") == []
        assert request(origin, "/api/racks") == []
        for name in order[:2]:
            for path in ("accepted-node.json", "prepared-bootstrap.json"):
                cluster.command("exec", name, "test", "!", "-e", "/opt/crowdb/data/" + path)
        (cluster.artifacts / "missing-peer-proof.json").write_text(json.dumps(report, indent=2))
        order = order[2:]
    for name in order:
        cluster.command("start", name)


def recreate(cluster):
    items = inventory(cluster, "container")
    for item in items:
        validate_container(cluster, item)
    validate_networks(cluster, items)
    volumes = inventory(cluster, "volume")
    if {item["Name"] for item in volumes} != expected_volumes(cluster):
        raise ValueError("reference retained volumes are missing or have foreign ownership")
    if cluster.implementation == "compose":
        compose(cluster, "up", "--detach", "--force-recreate", "--no-build", "--pull", "never")
    else:
        for item in items:
            cluster.command("rm", "--force", item["Id"])
        create_docker_nodes(cluster, reversed(range(cluster.node_count)))
        for name in reversed(cluster.expected_names):
            cluster.command("start", name)


def expected_volumes(cluster):
    if cluster.implementation == "compose":
        services = ["node"] if cluster.node_count == 1 else ["node1", "node2", "node3"]
        return {f"{cluster.run_id}_{service}-data" for service in services}
    return {f"{name}-data" for name in cluster.expected_names}


def validate_networks(cluster, containers):
    owned_ids = {item["Id"] for item in containers}
    expected_name = cluster.run_id + ("_default" if cluster.implementation == "compose" else "")
    for network in inventory(cluster, "network"):
        if any(identity not in owned_ids for identity in network.get("Containers", {})):
            raise ValueError("reference network has an unrelated attachment")
        if network.get("Name") != expected_name:
            raise ValueError("unexpected reference network name")


def refresh(cluster):
    items = inventory(cluster, "container")
    for item in items:
        validate_container(cluster, item)
    items.sort(key=lambda item: item["Name"])
    cluster.servers = [item["Name"].removeprefix("/") for item in items]
    cluster.containers = list(cluster.servers)
    if len(items) != cluster.node_count:
        raise AssertionError("reference deployment did not create the intended node count")
    for item in items:
        if not item["State"]["Running"]:
            raise AssertionError("reference node exited: " + cluster.command("logs", item["Id"]))
        env = item["Config"]["Env"]
        assert not any(value.startswith("CROWDB_DISCOVERY_SEEDS=") for value in env)
        assert all(not network.get("IPAMConfig", {}).get("IPv4Address")
                   for network in item["NetworkSettings"]["Networks"].values()
                   if network.get("IPAMConfig") is not None)
    return items


def close(cluster):
    if not cluster.runtime_accepted:
        return
    containers = inventory(cluster, "container")
    for item in containers:
        validate_container(cluster, item)
    validate_networks(cluster, containers)
    volumes = inventory(cluster, "volume")
    if any(item["Name"] not in expected_volumes(cluster) for item in volumes):
        raise ValueError("unrelated volume shares the reference scope")
    if cluster.implementation == "compose":
        # Compose's volume deletion follows YAML names without checking our owner.
        compose(cluster, "down", "--timeout", "30")
    else:
        for item in containers:
            current = json.loads(cluster.command("inspect", item["Id"]))[0]
            validate_container(cluster, current)
            cluster.command("rm", "-f", current["Id"])
        for network in inventory(cluster, "network"):
            cluster.command("network", "rm", network["Id"])
    if cluster.timings["status"] == "pass":
        for volume in inventory(cluster, "volume"):
            if volume["Name"] not in expected_volumes(cluster):
                raise ValueError("unrelated volume shares the reference scope")
            cluster.command("volume", "rm", volume["Name"])
    assert not inventory(cluster, "container") and not inventory(cluster, "network")
    remaining = inventory(cluster, "volume")
    if cluster.timings["status"] == "pass" and remaining:
        raise AssertionError("successful reference case leaked owned volumes")
    (cluster.artifacts / "runtime-cleanup.json").write_text(json.dumps({
        "ok": True, "project": cluster.run_id, "retained_volumes": [item["Name"] for item in remaining]}, indent=2))
