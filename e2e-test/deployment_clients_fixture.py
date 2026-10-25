# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Formal single deployment with an explicitly reachable ChunkDB inspector."""

import json
from managed_fixture import ManagedCluster


class DeploymentClientsCluster(ManagedCluster):
    def ready(self):
        credentials = self.artifacts / "client.env"
        before = credentials.read_text() if credentials.exists() else None
        super().ready()
        if before is not None and before != credentials.read_text():
            raise AssertionError("durable client credentials changed after restart")
        name = self.servers[0]
        status = self.monitor_status()
        modes = {file: self.command("exec", name, "stat", "-c", "%a",
                                   "/opt/crowdb/data/" + file).strip()
                 for file in ("secrets/server.env", "console/secrets/client.env", "crash")}
        cwd = {role: self.command("exec", "--user", "crowdb", name, "readlink",
                                 f"/proc/{pid}/cwd").strip()
               for role, pid in (("monitor", status["monitor_pid"]),
                                 ("kv", status["services"]["kv"]["pid"]))}
        info = json.loads(self.command("inspect", name))[0]
        (self.artifacts / "deployment-runtime.json").write_text(json.dumps({
            "modes": modes, "cwd": cwd, "ports": info["HostConfig"]["PortBindings"]}, indent=2))
