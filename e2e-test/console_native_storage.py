# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Private monitor file preparation on owned standard release nodes."""

import json
import uuid

from fixture import wait


def control(cluster, body):
    return json.loads(cluster.command("exec", "--user", "crowdb", cluster.servers[0],
        "crowdb-monitor", "control", "--json", json.dumps(body)))


def unbound_file_rejected(cluster):
    before = control(cluster, {"command": "disk_inventory"})
    request = {"operation_id": str(uuid.uuid4()), "cluster_id": "unbound",
        "node_id": 1, "capacity_bytes": 1024 ** 2, "sparse": False,
        "reserve_bytes": 1024 ** 3}
    try:
        control(cluster, {"command": "prepare_disk_file", "request": request})
    except RuntimeError as error:
        assert "waiting for Group 0 binding" in str(error), error
    else:
        raise AssertionError("unbound node accepted file preparation")
    inventory = control(cluster, {"command": "disk_inventory"})
    assert inventory["disks"] == before["disks"], inventory


def prepared_file_observation(cluster, operation_id):
    state = control(cluster, {"command": "disk_file_preparation", "operation_id": operation_id})
    if state["phase"] == "failed":
        raise AssertionError(f"monitor file preparation failed: {state}")
    return state


def verify_file_preparation(cluster):
    authority = cluster.api("/api/node/status")
    request = {"operation_id": str(uuid.uuid4()), "cluster_id": authority["cluster_id"],
        "node_id": cluster.node_ids[0], "capacity_bytes": 1024 ** 2,
        "sparse": False, "reserve_bytes": 1024 ** 3}
    accepted = control(cluster, {"command": "prepare_disk_file", "request": request})
    assert accepted["phase"] == "accepted", accepted
    assert accepted["request"] == request
    ready = wait(lambda: prepared_file_observation(cluster, request["operation_id"]),
        lambda result: result["phase"] == "prepared", "monitor-owned file preparation", seconds=10)
    assert ready["preparation_complete"] and ready["backing_identity"], ready
    inventory = control(cluster, {"command": "disk_inventory"})
    disk = next(disk for disk in inventory["disks"] if disk["path"] == ready["path"])
    assert disk["capacity_bytes"] == request["capacity_bytes"]
    assert disk["allocated_bytes"] >= request["capacity_bytes"]
    assert disk["backing_identity"] == ready["backing_identity"]
    assert disk["error"] is None
    # File preparation must not fabricate hardware membership.
    assert cluster.api(f"/api/nodes/{cluster.node_ids[0]}/disk-groups") == []
    try:
        control(cluster, {"command": "prepare_disk_file", "request": {**request, "sparse": True}})
    except RuntimeError as error:
        assert "inputs differ from accepted intent" in str(error), error
    else:
        raise AssertionError("monitor replaced an accepted file allocation mode")
    (cluster.artifacts / "file-preparation.json").write_text(json.dumps(ready, indent=2))


def file_operation(cluster, node, operation_id):
    state = cluster.api(f"/api/nodes/{node}/disk-files/{operation_id}")
    if state["phase"] in ("failed", "unknown"):
        raise AssertionError(f"file admission did not complete: {state}")
    return state


def verify_file_admission(cluster):
    """Real queued creation, monitor restart, DiskIO read and protected membership."""
    node = cluster.node_ids[0]
    group = 592
    cluster.api(f"/api/nodes/{node}/disk-groups", {
        "id": group, "name": "managed-file-acceptance", "store_id": 1, "group_id": 1}, status=201)
    requests = [{"operation_id": str(uuid.uuid4()), "disk_group_id": group,
        "capacity_bytes": 64 * 1024 ** 2, "sparse": False} for _ in range(2)]
    for request in requests:
        accepted = cluster.api(f"/api/nodes/{node}/disk-files", request, status=202)
        assert accepted["phase"] == "accepted", accepted
        assert accepted["request"]["operation_id"] == request["operation_id"], accepted
        wait(lambda: file_operation(cluster, node, request["operation_id"]),
            lambda result: result["phase"] == "waiting-for-diskio", "registered file waits for DiskIO", seconds=10)
    disks = cluster.api(f"/api/nodes/{node}/disk-groups/{group}/disks")
    assert len(disks) == 2, disks
    original = {disk["disk_id"]: disk for disk in disks}
    for disk in disks:
        assert disk["capacity_bytes"] == 64 * 1024 ** 2, disk
        try:
            cluster.api(f"/api/nodes/{node}/disk-groups/{group}/disks/{disk['disk_id']}", method="DELETE", status=204)
        except AssertionError as error:
            assert ": 409:" in str(error), error
        else:
            raise AssertionError("legacy deletion bypassed retained disk lifecycle")
    cluster.command("restart", cluster.servers[0])
    cluster.ready()
    for request in requests:
        resumed = wait(lambda: file_operation(cluster, node, request["operation_id"]),
            lambda result: result["phase"] == "waiting-for-diskio", "monitor resumes original file admission", seconds=10)
        assert resumed["request"]["operation_id"] == request["operation_id"], resumed
    page = cluster.api(f"/api/nodes/{node}/disk-files?limit=32")
    assert {request["operation_id"] for request in requests} <= {row["request"]["operation_id"] for row in page["items"]}, page
    deployment = cluster.api(f"/api/deployment-defaults?node_id={node}")["diskio"]
    deployment.update({"kind": "diskio", "instance_id": str(node), "rpc_port": 7200,
        "disk_group_id": 0, "test_single_node": False})
    cluster.api(f"/api/nodes/{node}/services/deploy", deployment, status=201)
    completed = [wait(lambda request=request: file_operation(cluster, node, request["operation_id"]),
        lambda result: result["phase"] == "registered", "DiskIO-verified file admission", seconds=10) for request in requests]
    for request in requests:
        cluster.api(f"/api/nodes/{node}/disk-files", request, status=202)
        wait(lambda request=request: file_operation(cluster, node, request["operation_id"]),
            lambda result: result["phase"] == "registered", "same-intent file replay", seconds=10)
    assert {disk["disk_id"]: disk for disk in cluster.api(f"/api/nodes/{node}/disk-groups/{group}/disks")} == original
    (cluster.artifacts / "file-admission.json").write_text(json.dumps(completed, indent=2))
    verify_missing_medium(cluster, node)


def verify_missing_medium(cluster, node):
    """An owned missing file remains admitted and needs validated recovery."""
    group = 593
    cluster.api(f"/api/nodes/{node}/disk-groups", {
        "id": group, "name": "missing-medium-acceptance", "store_id": 1, "group_id": 1}, status=201)
    request = {"operation_id": str(uuid.uuid4()), "disk_group_id": group,
        "capacity_bytes": 64 * 1024 ** 2, "sparse": False}
    cluster.api(f"/api/nodes/{node}/disk-files", request, status=202)
    prepared = wait(lambda: file_operation(cluster, node, request["operation_id"]),
        lambda result: result["phase"] == "registered", "health fixture admission", seconds=10)
    disks_url = f"/api/nodes/{node}/disk-groups/{group}/disks"
    original = cluster.api(disks_url)
    assert len(original) == 1, original
    disk = original[0]["disk_id"]
    endpoint = f"{disks_url}/{disk}"
    backing = cluster.api(f"{endpoint}/backing")
    path = prepared["path"]
    assert path == backing["container_path"]
    assert path.startswith("/opt/crowdb/data/disks/") and "/" not in path.removeprefix("/opt/crowdb/data/disks/")
    retained = f"/opt/crowdb/data/health-retained-{request['operation_id']}.img"
    cluster.command("exec", "--user", "crowdb", cluster.servers[0], "mv", "--", path, retained)
    try:
        closed = wait(lambda: cluster.api(f"{endpoint}/lifecycle"),
            lambda result: result["offline"], "missing registered medium quarantined", seconds=10)
        assert not closed["removed"], closed
        assert len(cluster.api(disks_url)) == 1
        assert cluster.api(f"{endpoint}/backing")["backing_identity"] == backing["backing_identity"]
        cluster.command("restart", cluster.servers[0])
        cluster.ready()
        inventory = control(cluster, {"command": "disk_inventory"})
        assert not any(row["path"] == path for row in inventory["disks"]), inventory
        assert cluster.api(f"{endpoint}/lifecycle")["offline"]
        assert len(cluster.api(disks_url)) == 1
    finally:
        cluster.command("exec", "--user", "crowdb", cluster.servers[0], "mv", "--", retained, path)
    assert cluster.api(f"{endpoint}/lifecycle")["offline"], "restored path must not reopen membership"
    inventory = control(cluster, {"command": "disk_inventory"})
    restored = next(row for row in inventory["disks"] if row["path"] == path)
    assert restored["backing_identity"] == backing["backing_identity"]
    assert restored["capacity_bytes"] == request["capacity_bytes"]
    (cluster.artifacts / "missing-medium.json").write_text(json.dumps({
        "closed": closed, "backing": backing, "restored": restored}, indent=2))
