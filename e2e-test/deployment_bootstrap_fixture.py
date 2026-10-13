# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Interrupt the shipped monitor after its first durable completed boot step."""

import json
import time
from managed_fixture import ManagedCluster


class InterruptedBootstrapCluster(ManagedCluster):
    def before_ready(self, data, profile):
        deadline = time.monotonic() + 40
        before = None
        while time.monotonic() < deadline:
            state = json.loads(self.command("inspect", self.servers[0]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("release container exited before bootstrap interruption")
            try:
                before = json.loads(self.command("exec", self.servers[0], "cat",
                                                "/opt/crowdb/data/bootstrap/manifest.json"))
            except RuntimeError as error:
                if "No such file" not in str(error):
                    raise
            if before and before["state"] == "initializing" and any(
                    step["complete"] for step in before["steps"]):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("bootstrap did not expose a completed step before readiness")
        (self.artifacts / "interrupted-manifest.json").write_text(json.dumps(before, indent=2))
        self.command("kill", "--signal", "KILL", self.servers[0])
        exited = json.loads(self.command("inspect", self.servers[0]))[0]["State"]
        if exited["ExitCode"] == 0:
            raise AssertionError("interrupted monitor returned success")
        name = self.servers[0]
        self.command("rm", name)
        self.servers.clear()
        self.containers.remove(name)
        address = self.launch(data, profile)
        if address != self.addresses[0]:
            raise AssertionError("bootstrap recovery changed the owned container IP")

    def ready(self):
        super().ready()
        body = self.command("exec", self.servers[0], "cat",
                            "/opt/crowdb/data/bootstrap/manifest.json")
        (self.artifacts / "recovered-manifest.json").write_text(body)
