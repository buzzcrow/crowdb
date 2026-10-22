# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Retained CLI metadata and data semantics against normal admitted nodes."""

import json


def observation(cluster):
    obsolete = cluster.root / "obsolete-console.toml"
    obsolete.write_text("invalid legacy configuration")
    cluster.extra_environment = {"CROWDB_CLI_STATE": str(obsolete)}
    for index in range(3):
        endpoint = cluster.endpoint(index)
        status = json.loads(cluster.invoke("cluster", "status", "--json", endpoint=endpoint))
        assert status["phase"] == "ready" and status["observation"]["cluster_id"] == cluster.cluster_id
        for domain in ("rack", "node"):
            assert "1" in cluster.invoke("cluster", domain, "list", endpoint=endpoint)
        assert "store 0" in cluster.invoke("cluster", "topology", "--node", "1", endpoint=endpoint)
    assert obsolete.read_text() == "invalid legacy configuration"


def hardware(cluster):
    cluster.invoke("cluster", "rack", "add", "--id", "71", "--name", "cli-rack")
    assert "cli-rack" in cluster.invoke("cluster", "rack", "list", endpoint=cluster.endpoint(1))
    cluster.invoke("cluster", "node", "add", "--id", "701", "--rack", "71",
                   "--host", "192.0.2.71", "--ssh-credential-ref", "fixture-reference")
    assert "192.0.2.71" in cluster.invoke("cluster", "node", "list", endpoint=cluster.endpoint(2))
    cluster.invoke("cluster", "node", "remove", "--id", "701", endpoint=cluster.endpoint(1))
    assert "192.0.2.71" not in cluster.invoke("cluster", "node", "list")
    cluster.invoke("cluster", "rack", "remove", "--id", "71")
    assert "cli-rack" not in cluster.invoke("cluster", "rack", "list")


def logical(cluster):
    cluster.invoke("kv", "store", "add", "--id", "9", "--nodes", "1")
    stores = cluster.invoke("kv", "store", "list", endpoint=cluster.endpoint(1))
    assert any(line.split()[0] == "9" for line in stores.splitlines() if line.split()), stores
    for group in (90, 91):
        cluster.invoke("kv", "group", "add", "--store", "9", "--group", str(group),
                       "--replica-id", str(group * 10), "--nodes", "1")
    groups = cluster.invoke("kv", "group", "list", "--store", "9", endpoint=cluster.endpoint(2))
    assert "90" in groups and "91" in groups, groups
    cluster.invoke("kv", "data", "put", "--store", "9", "--group", "90", "--key", "witness", "--value", "owned")
    assert "owned" in cluster.invoke("kv", "data", "get", "--store", "9", "--group", "90", "--key", "witness")
    cluster.invoke("kv", "group", "remove", "--store", "9", "--group", "91")
    groups = cluster.invoke("kv", "group", "list", "--store", "9")
    assert "90" in groups and "91" not in groups, groups
    cluster.invoke("kv", "store", "remove", "--id", "9")
    stores = cluster.invoke("kv", "store", "list")
    assert not any(line.split()[0] == "9" for line in stores.splitlines() if line.split()), stores


def data(cluster):
    prefix = "cli-data/" + cluster.run_id
    for suffix in ("a", "b"):
        cluster.invoke("kv", "data", "put", "--store", "0", "--group", "0",
                       "--key", prefix + suffix, "--value", suffix)
    scanned = cluster.invoke("kv", "data", "scan", "--store", "0", "--group", "0",
                             "--prefix", prefix, endpoint=cluster.endpoint(2))
    assert prefix + "a" in scanned and prefix + "b" in scanned, scanned
    for suffix in ("a", "b"):
        cluster.invoke("kv", "data", "delete", "--store", "0", "--group", "0", "--key", prefix + suffix)
        assert "not found" in cluster.invoke("kv", "data", "get", "--store", "0", "--group", "0",
                                             "--key", prefix + suffix, endpoint=cluster.endpoint(1))
