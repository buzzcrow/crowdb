# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal standalone recovery observes three independent release voters."""

from allocation import start_node
from console_standalone_fixture import prepare_config
from fixture import Cluster, HTTP, wait


class RecoveringWebCluster(Cluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, node_count=3, store_ids=(0, 0, 0),
                         groups=(0, 1), wire_replicas=True, advertise=True, **kwargs)
        self.bundle = bundle

    def start(self):
        super().start()
        self.client(self.bundle, "recovery-authority", "console::web_cluster_recovery::prepare")
        data = prepare_config(self, 3, {
            "rack": [{"id": 1, "name": "authority-rack"}],
            "node": [{"id": node, "rack_id": 1, "host": "127.0.0.1", "ssh_port": 22, "ssh_user": ""}
                     for node in (1, 2, 3)],
            "server": [{"id": f"kv-{index + 1}", "url": origin, "node_id": index + 1,
                        "auto_start": False} for index, origin in enumerate(self.origins)]})
        self.addresses.append(start_node(self, 3, data,
            ("server", "web", "--bind", "0.0.0.0", "--port", "6135",
             "--ui-root", "/opt/crowdb/ui", "--log-dir", "/opt/crowdb/data/log/web"),
            environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",)))
        self.web_ready()
        return self

    def web_ready(self):
        wait(lambda: HTTP.open(f"http://{self.addresses[3]}:6135/healthz", timeout=5).read(),
             lambda body: body == b"ok", "standalone release recovery readiness", seconds=20)

    def control(self, body):
        action = body.get("action")
        if action not in ("export-recovery-config", "install-recovery-cache"):
            result = super().control(body)
            if action == "start" and body.get("node") == 3:
                self.web_ready()
            return result
        if body.get("node") != 3:
            raise ValueError("recovery cache control requires its owned Web node")
        super().control({"action": "status", "node": 3})
        path = "/opt/crowdb/data/persistent/console/default/config.json"
        local = self.artifacts / "recovery-config.json"
        if action == "export-recovery-config":
            self.command("cp", f"{self.servers[3]}:{path}", str(local))
        else:
            if not local.is_file() or local.stat().st_size > 65536:
                raise ValueError("missing or oversized recovery cache")
            self.command("cp", str(local), f"{self.servers[3]}:{path}")
            self.command("exec", self.servers[3], "chown", "crowdb:crowdb", path)
            self.command("exec", self.servers[3], "chmod", "600", path)
        return {"cache": action}
