# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Formal single deployment with an explicitly reachable ChunkDB inspector."""

import json
from allocation import start_node
from managed_fixture import ManagedCluster, SOURCE


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
                 for file in ("secrets/server.env", "secrets/client.env", "crash")}
        cwd = {role: self.command("exec", "--user", "crowdb", name, "readlink",
                                 f"/proc/{pid}/cwd").strip()
               for role, pid in (("monitor", 1), ("kv", status["services"]["kv"]["pid"]))}
        info = json.loads(self.command("inspect", name))[0]
        (self.artifacts / "deployment-runtime.json").write_text(json.dumps({
            "modes": modes, "cwd": cwd, "ports": info["HostConfig"]["PortBindings"]}, indent=2))

    def launch(self, data, profile):
        template = self.artifacts / "chunkdb-template.toml"
        template.write_text((SOURCE / "templates/chunkdb.toml").read_text().replace(
            'rpc_listen_addr = "127.0.0.1:12200"', 'rpc_listen_addr = "0.0.0.0:12200"'))
        return start_node(self, 0, data, (), address=self.address, memory="2048m",
            environment=("CROWDB_STARTUP_MODE=single", "CROWDB_PROFILE_PATH=/opt/crowdb/etc/e2e-profile.toml"),
            mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-profile.toml,readonly",
                    f"type=bind,source={template},target=/opt/crowdb/etc/templates/chunkdb.toml,readonly"))
