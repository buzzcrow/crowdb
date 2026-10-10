#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Interrupted SSH admission and cross-UI managed-service lifecycle."""
import json
import urllib.error


def verify(docker, request, wait, IMAGE, PREFIX, names, volumes, origins, identities, records):
    # A new candidate stays unbound until another UI commits its admission.
    fourth = f"{PREFIX}-3"
    names.append(fourth)
    volume = f"{fourth}-data"
    volumes.append(volume)
    docker("run", "-d", "--name", fourth, "--network", PREFIX, "--cpus", "2", "--memory", "1g",
           "--mount", f"type=volume,source={volume},target=/opt/crowdb/data",
           "-e", "CROWDB_STARTUP_MODE=manual", "-e", "CROWDB_PHYSICAL_HOST_ID=one-docker-host",
           "-p", "127.0.0.1::9090", IMAGE)
    mapping = json.loads(docker("inspect", fourth))[0]["NetworkSettings"]["Ports"]["9090/tcp"][0]
    fourth_origin = "http://127.0.0.1:" + mapping["HostPort"]
    discovered = wait(lambda: request(origins[1], "/api/node/candidates"),
                      lambda result: len(result["candidates"]) == 4, "fourth candidate discovery")
    candidate = next(item for item in discovered["candidates"] if item["advertisement"]["discovery_id"] not in identities)
    assert request(fourth_origin, "/api/node/status")["phase"] == "unbound_draft"
    admission_body = {"discovery_id": candidate["advertisement"]["discovery_id"],
                      "rack_id": 1, "ssh_user": "crowdb", "ssh_port": 2222, "ssh_password": "wrong-password"}
    try:
        request(origins[1], "/api/node/admit", admission_body)
    except urllib.error.HTTPError as error:
        assert error.code == 500 and "ssh authentication failed" in " ".join(error.__notes__), error
    else:
        raise AssertionError("incorrect initial password must leave admission pending")
    pending = next(node for node in request(origins[1], "/api/node/admissions")
                   if node["discovery_id"] == admission_body["discovery_id"])
    binding = json.loads(docker("exec", names[1], "cat", "/opt/crowdb/data/node-binding.json"))
    grant = {"operation_id": pending["operation_id"], "management_seeds": binding["management_seeds"]}
    # Interrupt after KV preparation but before hardware confirmation/binding.
    public_key = docker("exec", names[1], "cat", "/opt/crowdb/data/ssh/id_ed25519.pub")
    install = {"command": "install_key", "operation_id": pending["operation_id"], "public_key": public_key}
    docker("exec", "--user", "crowdb", fourth, "crowdb-monitor", "control", "--json", json.dumps(install))
    prepared = {"command": "start_kv", "node_id": pending["node_id"], "bootstrap": binding["bootstrap"],
                "manifest": None, "credentials": None, "admission": grant}
    docker("exec", "--user", "crowdb", fourth, "crowdb-monitor", "control", "--json", json.dumps(prepared))
    cancelled = request(origins[1], "/api/node/cancel", {"discovery_id": pending["discovery_id"],
                         "operation_id": pending["operation_id"]})
    assert cancelled["pending"] == [], cancelled
    assert docker("exec", fourth, "sh", "-c", "test ! -e /opt/crowdb/data/accepted-node.json") == ""
    try:
        docker("exec", "--user", "crowdb", fourth, "crowdb-monitor", "control", "--json", json.dumps(prepared))
    except RuntimeError as error:
        assert "cancelled" in str(error), error
    else:
        raise AssertionError("delayed cancelled preparation restarted KV")
    print("Pending KV admission cancellation and delayed-command fencing passed", flush=True)
    joined = request(origins[1], "/api/node/admit", {"discovery_id": candidate["advertisement"]["discovery_id"],
                     "rack_id":1, "ssh_user":"crowdb", "ssh_port":2222, "ssh_password":"crowdb"})
    assert joined["confirmed"] and joined["node_id"] not in {record["node_id"] for record in records}
    assert joined["node_id"] == pending["node_id"] and joined["operation_id"] != pending["operation_id"]
    origins.append(fourth_origin)
    wait(lambda: request(fourth_origin, "/api/node/status"), lambda result: result["phase"] == "active", "new member binds")
    assert len(request(origins[0], "/api/node/admissions")) == 4
    print("Post-bootstrap admission through another UI passed", flush=True)
    return fourth, fourth_origin, joined


def services(docker, request, wait, origins, fourth, joined):
    service = request(origins[0], f"/api/nodes/{joined['node_id']}/services/deploy",
                      {"kind":"diskio", "instance_id":71, "rpc_port":19071})
    assert service["pid"]
    wait(lambda: json.loads(docker("exec", fourth, "cat", "/opt/crowdb/run/status/monitor.json")),
         lambda status: status["services"].get("diskio-71", {}).get("healthy") is True, "remote DiskIO application readiness")
    wait(lambda: request(origins[2], "/api/servers"), lambda entries: any(entry["id"] == "diskio-71" for entry in entries), "shared service projection")
    stale = json.loads(docker("exec", fourth, "cat", "/opt/crowdb/data/services/diskio-71/execution.json"))["intent"]
    stopped = request(origins[2], "/api/services/diskio-71/stop", {})
    assert stopped["pid"] is None
    try:
        docker("exec", "--user", "crowdb", fourth, "crowdb-monitor", "control", "--json", json.dumps({"command":"service", "intent":stale}))
    except RuntimeError as error:
        assert "superseded" in str(error), error
    else:
        raise AssertionError("stale service intent restarted a stopped service")
    restarted = request(origins[1], "/api/services/diskio-71/restart", {})
    assert restarted["pid"]
    print("Remote monitor service lifecycle from multiple UIs passed", flush=True)
