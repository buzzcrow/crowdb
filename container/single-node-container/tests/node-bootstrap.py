#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Real browser preparation and recovery from an independent UI."""
import json
import os
import subprocess
import urllib.error


def verify(docker, request, wait, verify_incomplete_preparation, PREFIX, names, origins):
    for origin in origins:
        wait(lambda: request(origin, "/api/node/candidates"), lambda result: len(result["candidates"]) == 3, "three-node discovery")
    origins[0] = verify_incomplete_preparation(names[0], origins[0])
    snapshot = wait(lambda: request(origins[0], "/api/node/candidates"),
                    lambda result: len(result["candidates"]) == 3 and all(
                        node["state"] == "unbound" for node in result["candidates"]), "unbound discovery after preparation cleanup")
    identities = {entry["advertisement"]["discovery_id"] for entry in snapshot["candidates"]}
    assert len(identities) == 3
    environment = dict(os.environ, CROWDB_NODE_UI_ORIGIN=origins[0],
                       CROWDB_NODE_UI_PREPARE_ONLY="1",
                       CROWDB_WEB_E2E_OUTPUT=f"test-results/{PREFIX}",
                       CROWDB_NODE_UI_IDENTITIES=json.dumps(sorted(identities)))
    subprocess.run(["npx", "playwright", "test", "--config=e2e/realBackend.config.ts",
                    "e2e/flows/12-cluster-discovery.spec.ts", "--grep", "Docker candidates"],
                   cwd="app/crowdb-web/ui", env=environment, check=True, timeout=120)
    prepared = request(origins[0], "/api/node/admissions")
    docker("pause", names[2])
    try:
        try:
            request(origins[0], "/api/cluster/init", {"nodes": [node["node_id"] for node in prepared]})
        except urllib.error.HTTPError as error:
            assert error.code == 500 and "SSH connection/authentication exceeded" in " ".join(error.__notes__), error
        else:
            raise AssertionError("unreachable selected node did not interrupt bootstrap")
    finally:
        docker("unpause", names[2])
    fixed = request(origins[0], "/api/node/status")
    assert fixed["phase"] == "bootstrap_in_progress", fixed
    environment.pop("CROWDB_NODE_UI_PREPARE_ONLY")
    environment["CROWDB_NODE_UI_ORIGIN"] = origins[1]
    environment["CROWDB_NODE_UI_RECOVERY_ORIGIN"] = origins[1]
    subprocess.run(["npx", "playwright", "test", "--config=e2e/realBackend.config.ts",
                    "e2e/flows/12-cluster-discovery.spec.ts", "--grep", "another UI resumes"],
                   cwd="app/crowdb-web/ui", env=environment, check=True, timeout=120)
    records = request(origins[0], "/api/node/admissions")
    print("Three nodes prepared and bootstrapped through the actual browser UI", flush=True)
    clusters = []
    for origin in origins:
        status = wait(lambda: request(origin, "/api/node/status"), lambda result: result["phase"] == "active", "shared active cluster")
        clusters.append(status["cluster_id"])
        racks = request(origin, "/api/racks?recursive=1")
        assert len(racks["items"][0]["nodes"]) == 3, racks
    assert len(set(clusters)) == 1
    assert clusters[0] == fixed["cluster_id"]
    for name in names:
        state = docker("exec", name, "sh", "-c", "cat /opt/crowdb/data/console/*.json")
        assert '"ssh_password": "crowdb"' not in state
        assert docker("exec", name, "stat", "-c", "%a", "/opt/crowdb/data/ssh/id_ed25519") == "600"
    print("All UIs read the same Group 0 topology", flush=True)
    local_identity = request(origins[0], "/api/node/candidates")["local"]["advertisement"]["discovery_id"]
    local_node = next(record["node_id"] for record in records if record["discovery_id"] == local_identity)
    request(origins[0], "/api/stores/0/groups", {"group_id": 2, "replica_id": 200, "nodes": [local_node]})
    request(origins[0], "/api/stores/0/groups", {"group_id": 3, "replica_id": 300,
                                                 "nodes": [record["node_id"] for record in records]})
    wait(lambda: request(origins[0], "/api/stores/0/groups/3/kv/put",
                         {"key": "remote-quorum", "value": "wired", "client_id": 991, "seq": 1}),
         lambda result: result["ok"], "three-node data quorum")
    assert request(origins[1], "/api/stores/0/groups/3/kv/get?key=remote-quorum")["value_utf8"] == "wired"
    return identities, records, clusters
