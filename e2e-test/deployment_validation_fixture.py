# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Require the shipped monitor to reject invalid durable inputs at startup."""

import json
import time
from allocation import start_node
from managed_fixture import ManagedCluster


class RejectedBootstrapCluster(ManagedCluster):
    def __init__(self, image, artifacts, kind, **kwargs):
        super().__init__(image, artifacts, services=("kv",), **kwargs)
        self.kind = kind

    def profile(self):
        return "invalid = true\n" if self.kind == "profile" else super().profile()

    def launch(self, data, profile):
        if self.kind == "manifest":
            preparer = self.run_id + "-manifest-input"
            self.containers.append(preparer)
            self.command("run", "--rm", "--name", preparer, "--label",
                f"org.crowdb.e2e.run={self.run_id}", "--network", "none", "--user", "root",
                "--cpus", "0.5", "--memory", "128m", "--pids-limit", "32",
                "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                "--entrypoint", "/bin/sh", self.image, "-ec",
                "mkdir /opt/crowdb/data/bootstrap; printf 'invalid manifest' > /opt/crowdb/data/bootstrap/manifest.json; chown -R crowdb:crowdb /opt/crowdb/data/bootstrap; chmod 0600 /opt/crowdb/data/bootstrap/manifest.json")
            self.containers.remove(preparer)
        address = start_node(self, 0, data, (), address=self.address, running=False,
            environment=("CROWDB_STARTUP_MODE=single", "CROWDB_PROFILE_PATH=/opt/crowdb/etc/e2e-profile.toml"),
            mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-profile.toml,readonly",))
        self.command("start", self.servers[0])
        return address

    def ready(self):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = json.loads(self.command("inspect", self.servers[0]))[0]["State"]
            if state["Status"] == "exited":
                body = self.command("logs", self.servers[0])
                (self.artifacts / "rejected-state.json").write_text(json.dumps(state, indent=2))
                (self.artifacts / "rejection.log").write_text(body)
                return
            time.sleep(0.1)
        raise AssertionError(f"release monitor did not reject invalid {self.kind} within 30 seconds")
