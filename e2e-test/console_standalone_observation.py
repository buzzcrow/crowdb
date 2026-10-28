# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Observe release Web's local children only inside its owned namespace."""
import json
from fixture import HTTP
from console_native_observation import owned, process


class StandaloneBrowserObservations:
    def __init__(self, cluster):
        self.cluster = cluster
        self.artifacts = cluster.artifacts

    def control(self, body):
        node = body.get("node")
        if type(node) is not int or node != 0:
            raise ValueError("standalone observation requires its only owned node")
        if body.get("action") == "observe-native-process":
            return process(self.cluster, node, body.get("pid"))
        if body != {"action": "observe-native-services", "node": 0}:
            raise ValueError("unsupported standalone observation")
        identity = owned(self.cluster, node)
        with HTTP.open(f"http://{self.cluster.addresses[0]}:6135/api/servers", timeout=5) as response:
            rows = json.load(response)
        observed = {}
        known = getattr(self.cluster, "native_processes", set())
        for row in rows:
            pid = row.get("pid")
            if type(pid) is int and 1 < pid <= 0xffffffff:
                known.add((identity, node, pid))
                observed[row["id"]] = {"pid": pid}
        self.cluster.native_processes = known
        return {"services": observed}
