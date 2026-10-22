# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Opt-in CLI accounting regression, owned by a normal disposable node."""

import json
import os
import subprocess
import time

from fixture import redact, request, wait


def quiesce_chunk_allocators(cluster):
    # The benchmark compares whole-group busy bytes. Its accounting requires
    # exclusive allocation activity, including background strip reservation.
    if cluster.node_count != 1:
        raise ValueError("pending-free fixture requires its owned single node")
    origin = cluster.endpoint(0)
    before = request(origin, "/api/servers")
    producers = []
    for kind in ("access-server", "chunk-kv", "chunkdb"):
        services = [item for item in before if item["service_type"] == kind]
        if len(services) != 1:
            raise AssertionError(f"expected one owned {kind} before allocator workload")
        producers.append(services[0]["id"])
    stopped = []
    for service_id in producers:
        request(origin, f"/api/services/{service_id}/stop", {})
        wait(lambda: request(origin, "/api/servers"),
             lambda items: any(item["id"] == service_id and item.get("pid") is None for item in items),
             f"confirmed stop of allocator producer {service_id}")
        stopped.append(service_id)
    after = request(origin, "/api/servers")
    preserved = ("paxos-kv", "diskdb", "diskio")
    for item in before:
        if item["service_type"] in preserved:
            current = next(row for row in after if row["id"] == item["id"])
            assert current["pid"] == item["pid"] and current["pid"], current
    (cluster.artifacts / "allocator-workload-precondition.json").write_text(json.dumps({
        "stopped_producers": stopped, "preserved_services": [
            {"id": item["id"], "pid": item["pid"]} for item in after
            if item["service_type"] in preserved]}, indent=2))


def pending_free(cluster):
    quiesce_chunk_allocators(cluster)
    name = "mixed_disk_workload_normalizes_frees_predating_its_baseline"
    env = cluster.environment()
    env.update(CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT=cluster.endpoint(0),
               CROWDB_E2E_CLI_BIN=str(cluster.bundle / "crowdb-cli"),
               LD_LIBRARY_PATH=str(cluster.bundle / "lib") + ":" + os.environ.get("LD_LIBRARY_PATH", ""))
    started = time.monotonic()
    result = subprocess.run([str(cluster.bundle / "cli-workload-test"), "--ignored", "--exact", name,
                             "--nocapture"], env=env, capture_output=True, text=True, timeout=120)
    output = redact(result.stdout + result.stderr)
    (cluster.artifacts / "client-pending-free.log").write_text(output)
    (cluster.artifacts / "client-pending-free.json").write_text(json.dumps({
        "case": cluster.case_name, "phase": "pending-free-accounting", "execution": "host",
        "seconds": time.monotonic() - started, "exit_code": result.returncode}, indent=2))
    print(output, end="", flush=True)
    if result.returncode or "test result: ok. 1 passed; 0 failed;" not in output:
        raise AssertionError("pending-free regression did not execute exactly one passing test")
