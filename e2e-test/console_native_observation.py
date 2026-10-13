# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Read ordinary monitor/process observations only in the recorded node namespace."""

import json
from allocation import LABEL


def owned(cluster, node):
    if type(node) is not int or not 0 <= node < len(cluster.servers):
        raise ValueError("node is outside the owned fixture")
    info = json.loads(cluster.command("inspect", cluster.servers[node]))[0]
    mounted = any(mount.get("Source") == str(cluster.data_paths[node])
                  and mount.get("Destination") == "/opt/crowdb/data"
                  for mount in info.get("Mounts", []))
    if (info["Config"]["Labels"].get(LABEL) != cluster.run_id
            or info["Image"] != cluster.image or not mounted):
        raise ValueError("native observation ownership or image/mount changed")
    return info["Id"]


def services(cluster, node):
    identity = owned(cluster, node)
    status = json.loads(cluster.command("exec", identity, "cat",
                                      "/opt/crowdb/run/status/monitor.json"))
    known = getattr(cluster, "native_processes", set())
    for service in status["services"].values():
        pid = service.get("pid")
        if type(pid) is int and 1 < pid <= 0xffffffff:
            known.add((identity, node, pid))
    cluster.native_processes = known
    return status


def process(cluster, node, pid):
    if type(pid) is not int or not 1 < pid <= 0xffffffff:
        raise ValueError("invalid observed native PID")
    identity = owned(cluster, node)
    if (identity, node, pid) not in getattr(cluster, "native_processes", set()):
        raise ValueError("PID was not observed in this owned node")
    state = cluster.command("exec", identity, "sh", "-c",
        f"if test -f /proc/{pid}/stat; then awk '{{print $3}}' /proc/{pid}/stat; else echo dead; fi").strip()
    return {"alive": state not in ("dead", "Z", "X")}


class BrowserObservations:
    """A separate read-only queue while the lifecycle worker waits on the browser."""
    def __init__(self, cluster):
        self.cluster = cluster
        self.artifacts = cluster.artifacts / "browser-observation"
        self.artifacts.mkdir(exist_ok=True)

    def control(self, body):
        if body.get("action") == "observe-native-services":
            return services(self.cluster, body.get("node"))
        if body.get("action") == "observe-native-process":
            return process(self.cluster, body.get("node"), body.get("pid"))
        raise ValueError("browser observation control is read-only")
