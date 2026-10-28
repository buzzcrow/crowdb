# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Uninitialized release KV and a bare-metal Web consuming sealed intent."""

import json
from allocation import node_data, start_node
from fixture import Cluster, HTTP, wait


class BootstrapWebCluster(Cluster):
    def __init__(self, image, artifacts, **kwargs):
        super().__init__(image, artifacts, node_count=1, store_ids=(0,), groups=(),
                         wire_replicas=False, advertise=True,
                         node_args=(("--node-id", "1"),), **kwargs)

    def start(self):
        super().start()
        data = node_data(self, 1)
        profile = self.artifacts / "web.toml"
        profile.write_text('version = 1\nmode = "bare-metal"\nbind = "0.0.0.0"\nport = 6135\n'
                           f'group0_management_seeds = {json.dumps(self.origins)}\n'
                           'ui_root = "/opt/crowdb/ui"\nlog_dir = "/opt/crowdb/data/log/web"\n'
                           'log_max_file_mb = 30\nlog_max_files = 5\nrequest_timeout_ms = 500\n')
        self.addresses.append(start_node(self, 1, data,
            ("server", "web", "--config", "/opt/crowdb/etc/e2e-web.toml"),
            environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",),
            mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-web.toml,readonly",)))
        wait(lambda: HTTP.open(f"http://{self.addresses[1]}:6135/healthz", timeout=5).read(),
             lambda body: body == b"ok", "bootstrap Web listener")
        return self

    def control(self, body):
        if body.get("action") != "upload-bootstrap-source":
            return super().control(body)
        if type(body.get("node")) is not int or body["node"] != 1:
            raise ValueError("bootstrap source belongs to the owned Web node")
        super().control({"action": "status", "node": 1})
        source = self.artifacts / "bootstrap-source.toml"
        if source.is_symlink() or not source.is_file() or source.stat().st_size > 65536:
            raise ValueError("missing, symlinked or oversized bootstrap source")
        target = "/opt/crowdb/data/bootstrap-source.toml"
        name = self.servers[1]
        self.command("cp", str(source), f"{name}:{target}")
        self.command("exec", name, "chown", "crowdb:crowdb", target)
        self.command("exec", name, "chmod", "600", target)
        return {"path": target}
