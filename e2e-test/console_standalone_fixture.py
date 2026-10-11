# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Standalone Web persistence through the supported release server entrypoint."""

import json
from allocation import create_network, node_data, start_node
from fixture import Cluster, HTTP, wait


class StandaloneWebCluster(Cluster):
    def __init__(self, image, artifacts, rejected=False, **kwargs):
        super().__init__(image, artifacts, node_count=1, wire_replicas=False,
                         advertise=True, **kwargs)
        self.rejected = rejected

    def start(self):
        subnet = create_network(self)
        data = node_data(self, 0)
        if self.rejected:
            config = data / "persistent/console/default/config.json"
            config.parent.mkdir(parents=True)
            config.write_text("broken config")
        address = str(subnet.network_address + 2)
        self.addresses.append(start_node(self, 0, data,
            ("server", "web", "--bind", "0.0.0.0", "--port", "9090",
             "--ui-root", "/opt/crowdb/ui", "--log-dir", "/opt/crowdb/data/log/web"),
            address=address, environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",)))
        self.ready()
        return self

    def ready(self):
        if self.rejected:
            state = wait(lambda: json.loads(self.command("inspect", self.servers[0]))[0]["State"],
                         lambda state: not state["Running"], "rejected standalone Web", seconds=20)
            if state["ExitCode"] == 0:
                raise AssertionError("malformed standalone config was accepted")
            (self.artifacts / "standalone-rejection.json").write_text(json.dumps({
                "state": state, "log": self.command("logs", self.servers[0])}, indent=2))
        else:
            wait(lambda: HTTP.open(f"http://{self.addresses[0]}:9090/healthz", timeout=5).read(),
                 lambda body: body == b"ok", "standalone Web listener", seconds=20)

    def control(self, body):
        result = super().control(body)
        if body.get("action") == "start":
            self.ready()
        return result
