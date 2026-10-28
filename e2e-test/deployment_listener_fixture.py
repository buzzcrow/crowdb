# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Verify fixed Access listeners recover through the monitor retry plan."""

import json
import tomllib
from managed_fixture import ManagedCluster


class FixedListenerAccessCluster(ManagedCluster):
    def __init__(self, image, artifacts, protocol, services, **kwargs):
        super().__init__(image, artifacts, services=services, **kwargs)
        self.protocol = protocol

    def start(self):
        super().start()
        config = self.command("exec", self.servers[0], "cat", "/opt/crowdb/data/services/access-1/service.toml")
        port = {"s3": 7135, "iceberg": 8135}[self.protocol]
        expected = f"{self.addresses[0]}:{port}"
        if tomllib.loads(config)[self.protocol]["listen"] != expected:
            raise AssertionError(f"{self.protocol} listener was not kept on its fixed port {expected}")
        before = self.service_status("access")
        result = self.control({"action": "recover-service", "node": 0,
                               "service": "access", "signal": "KILL"})
        after = result["after"]
        if after["generation"] <= before["generation"] or after["pid"] == before["pid"]:
            raise AssertionError("monitor did not schedule a new Access process")
        if json.loads(self.command("inspect", self.servers[0]))[0]["NetworkSettings"]["Networks"][self.run_id]["IPAddress"] != self.addresses[0]:
            raise AssertionError("Access recovery changed the owned node identity")
        recovered_config = self.command("exec", self.servers[0], "cat", "/opt/crowdb/data/services/access-1/service.toml")
        if tomllib.loads(recovered_config) != tomllib.loads(config):
            raise AssertionError(f"{self.protocol} listener changed after retry")
        (self.artifacts / "listener-recovery.json").write_text(json.dumps({
            "protocol": self.protocol, "before": before, "after": after,
            "fixed_listener": expected, "signal": result["signal"]}, indent=2))
        return self
