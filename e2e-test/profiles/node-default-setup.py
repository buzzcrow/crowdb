# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Accepted node setup survives browser closure and an actual monitor restart."""

import json
from bridge_node import browser, DATA


def verify(docker, request, wait, names, origins):
    selected = origins[:3]
    identities = [request(origin, "/api/node/candidates")["local"]["advertisement"]["discovery_id"]
                  for origin in selected]
    browser(selected[0], "Docker default setup: submit", "default-submit",
            CROWDB_NODE_UI_IDENTITIES=json.dumps(sorted(identities)))
    wait(lambda: request(selected[0], "/api/node/status"),
         lambda status: status["phase"] == "active", "default setup Group 0 publication")
    records = request(selected[0], "/api/node/admissions")
    assert len(records) == 3 and all(record["confirmed"] for record in records), records
    steps = {}
    for kind in ("paxos-kv", "diskdb", "diskio"):
        for record in records:
            node = record["node_id"]
            steps[node] = wait(lambda node=node: request(selected[0], f"/api/nodes/{node}/setup"),
                lambda status: next(step for step in status["services"] if step["kind"] == kind)["phase"] == "ready",
                f"default node {node} {kind} ready without browser")
    operations = {node: {step["kind"]: step["operation_id"] for step in status["services"]
                         if step["operation_id"]} for node, status in steps.items()}
    files = {}
    for record in records:
        node = record["node_id"]
        page = wait(lambda node=node: request(selected[0], f"/api/nodes/{node}/disk-files"),
                    lambda page: len(page["items"]) == 1 and page["items"][0]["phase"] == "registered",
                    f"default node {node} original file admission")
        operation = page["items"][0]
        assert not operation["request"]["sparse"]
        expected = 16 * 1024 ** 3
        assert operation["request"]["capacity_bytes"] == expected, operation
        assert operation["request"]["reserve_bytes"] == 1024 ** 3, operation
        name = names[identities.index(record["discovery_id"])]
        physical = docker("exec", name, "stat", "-c", "%d:%i:%s:%b", operation["path"])
        assert int(physical.split(":")[2]) == expected, physical
        assert int(physical.split(":")[3]) * 512 >= expected, physical
        files[node] = {"name": name, "operation": operation, "stat": physical}
    (DATA / "default-node-setup-before-restart.json").write_text(json.dumps(files, indent=2))
    docker("restart", "--time", "10", names[0])
    wait(lambda: request(selected[0], "/api/node/status"), lambda status: status["phase"] == "active",
         "default setup monitor returns with the same binding")
    for kind in ("chunkdb", "chunk-kv", "access-server"):
        for record in records:
            node = record["node_id"]
            status = wait(lambda node=node: request(selected[0], f"/api/nodes/{node}/setup"),
                lambda status: next(step for step in status["services"] if step["kind"] == kind)["phase"] == "ready",
                f"default node {node} {kind} ready after monitor restart")
            for step in status["services"]:
                if step["kind"] in operations[node]:
                    assert step["operation_id"] == operations[node][step["kind"]], step
    for node, file in files.items():
        status = request(selected[0], f"/api/nodes/{node}/setup")
        assert status["phase"] == "ready" and status["storage_mode"] == "automatic", status
        assert len(status["disks"]) == 1 and status["disks"][0]["phase"] == "ready", status
        path = file["operation"]["path"]
        current_stat = docker("exec", file["name"], "stat", "-c", "%d:%i:%s:%b", path)
        assert current_stat == file["stat"], {"node": node, "path": path,
            "before": file["stat"], "after": current_stat}
        observed = wait(lambda name=file["name"]: json.loads(docker("exec", name, "cat", "/opt/crowdb/run/status/monitor.json")),
            lambda observed: len(observed["services"]) == 7 and all(service["healthy"] for service in observed["services"].values()),
            f"default node {node} all actual processes healthy")
        assert observed["phase"] == "ready", observed
    browser(selected[0], "Docker default setup: progress", "default-progress")
    (DATA / "default-node-setup.json").write_text(json.dumps(files, indent=2))
    print("Default file/service setup continues after browser closure and monitor restart", flush=True)
