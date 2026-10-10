#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Independent cluster cleanup, persistent endpoint changes and voting catch-up."""
import json
import os
import subprocess


def browser(origin, selection, **inputs):
    environment = dict(os.environ, CROWDB_NODE_UI_ORIGIN=origin, **inputs)
    subprocess.run(["npx", "playwright", "test", "--config=e2e/realBackend.config.ts",
                    "e2e/flows/12-cluster-discovery.spec.ts", "--grep", selection],
                   cwd="app/crowdb-web/ui", env=environment, check=True, timeout=120)


def verify(docker, request, wait, image, network, names, volumes, origins, cluster):
    name = network + "-4"
    names.append(name)
    volume = name + "-data"
    volumes.append(volume)
    docker("run", "-d", "--name", name, "--network", network, "--cpus", "2", "--memory", "1g",
           "--mount", f"type=volume,source={volume},target=/opt/crowdb/data",
           "-e", "CROWDB_STARTUP_MODE=manual", "-e", "CROWDB_PHYSICAL_HOST_ID=one-docker-host",
           "-p", "127.0.0.1::9090", image)
    mapping = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["9090/tcp"][0]
    origin = "http://127.0.0.1:" + mapping["HostPort"]
    snapshot = wait(lambda: request(origin, "/api/node/candidates"), lambda result: bool(result["local"]), "independent node discovery")
    identity = snapshot["local"]["advertisement"]["discovery_id"]
    request(origin, "/api/racks", {"id":1, "name":"independent-rack"})
    body = {"discovery_id":identity, "rack_id":1, "ssh_user":"crowdb", "ssh_port":2222, "ssh_password":"crowdb"}
    record = request(origin, "/api/node/admit", body)
    request(origin, "/api/cluster/init", {"nodes":[record["node_id"]]})
    independent = request(origin, "/api/node/status")
    assert independent["phase"] == "active" and independent["cluster_id"] != cluster, independent
    binding = json.loads(docker("exec", name, "cat", "/opt/crowdb/data/node-binding.json"))
    manifest = json.loads(docker("exec", name, "cat", "/opt/crowdb/data/prepared-bootstrap.json"))
    credentials = json.loads(docker("exec", "--user", "crowdb", name, "crowdb-monitor", "control", "--json", json.dumps({"command":"service_credentials", "cluster_id":independent["cluster_id"]})))
    wait(lambda: request(origins[0], "/api/node/candidates"), lambda result: any(
        node["advertisement"]["discovery_id"] == identity and node["state"] == "foreign_cluster" for node in result["candidates"]), "foreign cluster separation")
    browser(origins[0], "independent cluster", CROWDB_NODE_UI_FOREIGN_ORIGIN=origin, CROWDB_NODE_UI_FOREIGN_UUID=identity,
            CROWDB_WEB_E2E_OUTPUT=f"test-results/{network}-foreign")
    delayed = {"command":"start_kv", "node_id":record["node_id"], "bootstrap":binding["bootstrap"], "manifest":manifest, "credentials":credentials}
    try:
        docker("exec", "--user", "crowdb", name, "crowdb-monitor", "control", "--json", json.dumps(delayed))
    except RuntimeError as error:
        assert "retired" in str(error), error
    else:
        raise AssertionError("old independent cluster command restarted its replica")
    wait(lambda: request(origins[0], "/api/node/candidates"), lambda result: any(
        node["advertisement"]["discovery_id"] == identity and node["state"] == "unbound" for node in result["candidates"]), "cleaned node available")
    joined = request(origins[0], "/api/node/admit", body)
    assert joined["confirmed"] and request(origin, "/api/node/status")["cluster_id"] == cluster
    key = docker("exec", name, "cat", "/opt/crowdb/data/ssh/id_ed25519.pub")
    old = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Networks"][network]["IPAddress"]
    subnet = json.loads(docker("network", "inspect", network))[0]["IPAM"]["Config"][0]["Subnet"]
    changed = subnet.split("/")[0].rsplit(".", 1)[0] + ".250"
    docker("stop", "--time", "10", name)
    docker("network", "disconnect", network, name)
    docker("network", "connect", "--ip", changed, network, name)
    docker("start", name)
    mapping = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["9090/tcp"][0]
    origin = "http://127.0.0.1:" + mapping["HostPort"]
    assert old != changed
    wait(lambda: request(origins[0], "/api/node/candidates"), lambda result: any(
        node["advertisement"]["discovery_id"] == identity and node["state"] == "same_cluster" and changed in str(node["advertisement"]["monitor_endpoints"])
        for node in result["candidates"]), "persistent node new endpoint")
    assert docker("exec", name, "cat", "/opt/crowdb/data/ssh/id_ed25519.pub") == key
    browser(origins[0], "node update", CROWDB_NODE_UI_UPDATE_UUID=identity, CROWDB_WEB_E2E_OUTPUT=f"test-results/{network}-update")
    for peer in origins:
        updated = next(node for node in request(peer, "/api/node/admissions") if node["discovery_id"] == identity)
        assert updated["node_id"] == joined["node_id"] and updated["rack_id"] == 2 and updated["host"] == changed, updated
        assert updated["physical_host_id"] == "one-docker-host"
    origins.append(origin)
    for gid, replica_id in [(0, 601), (3, 600)]:
        request(origins[0], f"/api/stores/0/groups/{gid}/replicas", {"node_id":joined["node_id"], "replica_id":replica_id})
        topology = json.loads(docker("exec", name, "curl", "-fsS", "http://127.0.0.1:10000/topology"))
        group = next(group for store in topology["stores"] if store["store_id"] == 0 for group in store["groups"] if group["group_id"] == gid)
        assert group["local_replica_id"] == replica_id and group["read_state"]["contiguous_applied"] > 0, group
        for peer in names[:3]:
            topology = json.loads(docker("exec", peer, "curl", "-fsS", "http://127.0.0.1:10000/topology"))
            group = next(group for store in topology["stores"] if store["store_id"] == 0 for group in store["groups"] if group["group_id"] == gid)
            assert any(remote["id"] == replica_id and remote["voting"] for remote in group["remotes"]), group
    assert request(origin, "/api/stores/0/groups/3/kv/get?key=remote-quorum")["value_utf8"] == "wired"
    assert request(origin, "/api/node/status")["cluster_id"] == cluster
    print("Independent cluster cleanup, authenticated persistent IP/rack update and snapshot/WAL voting catch-up passed", flush=True)
