# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Sustained physical outage with current Access recovery and retained S3 reads."""

import json
import subprocess
import time

from fixture import request


def signal_diskio(cluster, identity, pid, signal):
    expected = "/opt/crowdb/bin/crowdb-diskio"
    check = f'test "$(readlink /proc/{pid}/exe)" = {expected} && kill -{signal} {pid}'
    if signal == "CONT":
        check = f"if test -e /proc/{pid}/exe; then {check}; fi"
    cluster.command("exec", "--user", "crowdb", identity, "sh", "-c", check)


def health_snapshot(cluster, label):
    observed = []
    for index, name in enumerate(cluster.servers):
        identity = cluster.inspect_owned(name)["Id"]
        url = f"http://{cluster.addresses[index]}:9094/_crowdb/health/ready"
        try:
            value = cluster.command("exec", identity, "curl", "--silent", "--show-error",
                                    "--max-time", "3", url)
            observed.append(json.loads(value))
        except (RuntimeError, ValueError) as error:
            observed.append({"error": str(error)})
    (cluster.artifacts / f"access-health-{label}.json").write_text(json.dumps(observed, indent=2))
    return observed


def pause_and_write(cluster, pauses):
    with (cluster.artifacts / "fault-s3.log").open("w") as output:
        clients = []
        try:
            for identity, pid in pauses:
                signal_diskio(cluster, identity, pid, "STOP")
                status = cluster.command("exec", identity, "cat", f"/proc/{pid}/status")
                assert "State:\tT" in status, status
            print("Paused three verified DiskIO processes across the 30-second I/O deadline", flush=True)
            for number in range(3):
                clients.append(subprocess.Popen(cluster.arguments("s3", "object", "put",
                    "writer-recovery", f"during-{number}", "--text", "during-fault"),
                    env=cluster.environment(), stdout=output, stderr=output))
            time.sleep(40)
            unavailable = health_snapshot(cluster, "paused")
            assert all(value.get("ready") is False for value in unavailable), unavailable
            assert all(value.get("observation", {}).get("state") == "current"
                       for value in unavailable), unavailable
        finally:
            for identity, pid in pauses:
                signal_diskio(cluster, identity, pid, "CONT")
            for client in clients:
                try:
                    client.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    client.kill()
                    client.wait()


def terminal_writer_recovery(cluster, cluster_id):
    origin = f"http://{cluster.addresses[0]}:9090"
    inputs = cluster.state()
    identities = [cluster.inspect_owned(name)["Id"] for name in cluster.servers]
    cluster.invoke("s3", "bucket", "put", "writer-recovery")
    cluster.invoke("s3", "object", "put", "writer-recovery", "before",
                   "--text", "acknowledged-before-fault")
    before = request(origin, "/api/servers")
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
        "diskio_processes": pauses, "pause_seconds": 40}, indent=2))
    pause_and_write(cluster, pauses)
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
    after = request(origin, "/api/servers")
    old = {service["id"]: service.get("pid") for service in before
           if service.get("service_type") == "chunk-kv"}
    new = {service["id"]: service.get("pid") for service in after
           if service.get("service_type") == "chunk-kv"}
    restarted = [service for service in old if old[service] != new.get(service)]
    assert restarted, (old, new)
    access = lambda rows: {service["id"]: service.get("pid") for service in rows
                           if service.get("service_type") == "access-server"}
    assert len(access(before)) == 3 and access(before) == access(after), "healthy Access processes restarted"
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
        "access_pids": access(after), "recovery_seconds": recovery_seconds,
        "retained_reads": 3, "new_reads": 3}, indent=2))
    cluster.current_readiness(cluster_id)
    print("PASS automatic durable replay, cached Access recovery and operator stop fencing", restarted, flush=True)
