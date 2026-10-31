# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Sustained physical outage with current Access recovery and retained S3 reads."""

import json
import subprocess
import time

from fixture import request
from console_cli_io_fault import DiskIoOutage


def health_snapshot(cluster, label):
    observed = []
    for index, name in enumerate(cluster.servers):
        identity = cluster.inspect_owned(name)["Id"]
        port = cluster.access_listeners(index)["health_port"]
        url = f"http://{cluster.addresses[index]}:{port}/_crowdb/health/ready"
        try:
            value = cluster.command("exec", identity, "curl", "--silent", "--show-error",
                                    "--max-time", "3", url)
            observed.append(json.loads(value))
        except (RuntimeError, ValueError) as error:
            observed.append({"error": str(error)})
    (cluster.artifacts / f"access-health-{label}.json").write_text(json.dumps(observed, indent=2))
    return observed


def block_and_write(cluster, outage):
    with (cluster.artifacts / "fault-s3.log").open("w") as output:
        clients = []
        try:
            with outage:
                outage.verify()
                print("Blocked three owned DiskIO listeners across the 30-second I/O deadline", flush=True)
                for number in range(3):
                    clients.append(subprocess.Popen(cluster.arguments("s3", "object", "put",
                        "writer-recovery", f"during-{number}", "--text", "during-fault"),
                        env=cluster.environment(), stdout=output, stderr=output))
                time.sleep(40)
                outage.verify()
                unavailable = health_snapshot(cluster, "paused")
                assert all(value.get("ready") is False for value in unavailable), unavailable
                assert all(value.get("observation", {}).get("state") == "current"
                           for value in unavailable), unavailable
        finally:
            for client in clients:
                try:
                    client.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    client.kill()
                    client.wait()


def managed_processes(cluster):
    processes = {}
    for index, name in enumerate(cluster.servers):
        identity = cluster.inspect_owned(name)["Id"]
        node = cluster.read_owned("node-binding.json", index)["node_id"]
        status = json.loads(cluster.command("exec", identity, "cat",
                                          "/opt/crowdb/run/status/monitor.json"))
        for kind in ("chunk-kv", "access"):
            service = f"{kind}-{node}"
            current = status["services"][service]
            assert type(current.get("pid")) is int and current["pid"] > 1, current
            processes[service] = {key: current[key] for key in ("pid", "generation")}
    return processes


def terminal_writer_recovery(cluster, cluster_id):
    origin = f"http://{cluster.addresses[0]}:9090"
    inputs = cluster.state()
    identities = [cluster.inspect_owned(name)["Id"] for name in cluster.servers]
    cluster.invoke("s3", "bucket", "put", "writer-recovery")
    cluster.invoke("s3", "object", "put", "writer-recovery", "before",
                   "--text", "acknowledged-before-fault")
    before = request(origin, "/api/servers")
    processes_before = managed_processes(cluster)
    services = [service for service in before if service.get("service_type") == "diskio"]
    assert len(services) == 3, services
    pauses = []
    for service in services:
        index = next(index for index in range(3)
                     if cluster.read_owned("node-binding.json", index)["node_id"] == service["node_id"])
        pid = service["pid"]
        assert type(pid) is int and pid > 1, service
        pauses.append((identities[index], pid))
    (cluster.artifacts / "fault-injection.json").write_text(json.dumps({
        "diskio_processes": pauses, "fault": "owned-namespace DiskIO packet drop",
        "outage_seconds": 40}, indent=2))
    block_and_write(cluster, DiskIoOutage(cluster))
    # No application traffic until independent health observation reports recovery.
    started = time.monotonic()
    try:
        report = json.loads(cluster.invoke("cluster", "check", "--wait", "180", "--json"))
    finally:
        health = health_snapshot(cluster, "checked")
    assert report["phase"] == "ready" and report["observation"]["cluster_id"] == cluster_id, report
    recovery_seconds = time.monotonic() - started
    assert all(value.get("ready") is True for value in health), health
    assert all(value.get("observation", {}).get("state") == "current" for value in health), health
    assert inputs == cluster.state(), "fault recovery changed accepted launch inputs"
    assert identities == [cluster.inspect_owned(name)["Id"] for name in cluster.servers]
    processes_after = managed_processes(cluster)
    (cluster.artifacts / "process-recovery.json").write_text(json.dumps({
        "before": processes_before, "after": processes_after}, indent=2))
    old = {key: value["pid"] for key, value in processes_before.items() if key.startswith("chunk-kv-")}
    new = {key: value["pid"] for key, value in processes_after.items() if key.startswith("chunk-kv-")}
    restarted = [service for service in old if old[service] != new.get(service)]
    assert restarted, (old, new)
    access = lambda rows: {key: value for key, value in rows.items() if key.startswith("access-")}
    assert len(access(processes_before)) == 3 and access(processes_before) == access(processes_after), (
        "healthy Access processes restarted", processes_before, processes_after)
    recovery_logs = []
    for index, name in enumerate(cluster.servers):
        node = cluster.read_owned("node-binding.json", index)["node_id"]
        log = cluster.command("exec", identities[index], "sh", "-c",
            f"cat /opt/crowdb/data/log/chunk-kv-{node}/crowdb-chunk-kv-server-[0-9]*.log")
        (cluster.artifacts / f"chunk-session-{node}.log").write_text(log)
        recovery_logs.append(log)
    assert any("writer outcome requires durable replay; stopping for supervisor restart" in log
               for log in recovery_logs), "fault did not exercise terminal writer replay"
    cluster.invoke("s3", "object", "put", "writer-recovery", "after",
                   "--text", "acknowledged-after-fault")
    for address in cluster.addresses:
        for key in ("before", "after"):
            value = cluster.invoke("s3", "object", "get", "writer-recovery", key,
                                   endpoint=f"http://{address}:9090")
            assert value == f"acknowledged-{key}-fault", value
    (cluster.artifacts / "recovery-proof.json").write_text(json.dumps({
        "cluster_id": cluster_id, "old_pids": old, "new_pids": new, "restarted": restarted,
        "access_processes": access(processes_after), "recovery_seconds": recovery_seconds,
        "retained_reads": 3, "new_reads": 3}, indent=2))
    cluster.current_readiness(cluster_id)
    print("PASS automatic durable replay, cached Access recovery and operator stop fencing", restarted, flush=True)
