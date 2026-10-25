# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Owned monitor failures through ordinary service recovery and durable identity."""

import json
import gzip
import time
from pathlib import Path
from fixture import LABEL
from managed_fixture import ManagedCluster


class DeploymentFailureCluster(ManagedCluster):
    def control(self, body):
        action = body.get("action")
        if action not in ("prepare-lifecycle", "exhaust-restarts", "reject-changed-credentials"):
            return super().control(body)
        if body.get("node") != 0 or "web" not in self.services:
            raise ValueError("monitor failure requires the owned complete node")
        name = self.servers[0]
        info = json.loads(self.command("inspect", name))[0]
        if info["Config"]["Labels"].get(LABEL) != self.run_id or not info["State"]["Running"]:
            raise ValueError("monitor container ownership or running state changed")
        if action == "prepare-lifecycle":
            accepted = self.command("exec", name, "cat", "/opt/crowdb/data/accepted-node.json")
            self.command("stop", "--time", "15", name)
            self.command("rm", name)
            self.servers.clear()
            self.containers.remove(name)
            self.launch(self.data_paths[0], self.artifacts / "profile.toml")
            self.ready()
            retained = self.command("exec", self.servers[0], "cat", "/opt/crowdb/data/accepted-node.json")
            if json.loads(accepted) != json.loads(retained):
                raise AssertionError("graceful monitor replacement changed accepted bootstrap")
            return {"phase": "ready"}
        secrets = []
        for file in ("secrets/server.env", "console/secrets/client.env"):
            for line in self.command("exec", name, "cat", "/opt/crowdb/data/" + file).splitlines():
                key, _, value = line.partition("=")
                if value and any(word in key for word in ("TOKEN", "KEY")):
                    secrets.append(value)
        self.private_values = secrets
        if action == "exhaust-restarts":
            for _ in range(5):
                super().control({"action": "recover-service", "node": 0,
                                 "service": "web", "signal": "KILL"})
        before = self.monitor_status()
        if action == "reject-changed-credentials":
            self.command("exec", "--user", "root", name, "/bin/sh", "-ec",
                         "printf 'invalid credentials\\n' > /opt/crowdb/data/secrets/server.env")
            self.command("restart", name)
        else:
            pid = self.monitor_status()["services"]["web"]["pid"]
            if type(pid) is not int or pid <= 1:
                raise ValueError("web PID must identify the observed monitor child")
            executable = self.command("exec", name, "cat", f"/proc/{pid}/cmdline").split("\0")[0]
            if Path(executable).name != "crowdb-web":
                raise ValueError("observed web executable changed before monitor failure")
            self.command("exec", name, "kill", "-KILL", str(pid))
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            state = json.loads(self.command("inspect", name))[0]["State"]
            if state["Status"] == "exited":
                return self.failure_result(action, state, before)
            time.sleep(0.1)
        raise AssertionError("container stayed running after unrecoverable monitor failure")

    def failure_result(self, action, state, before):
        log = self.command("logs", self.servers[0])
        status_path = self.artifacts / "failed-monitor-status.json"
        self.command("cp", f"{self.servers[0]}:/opt/crowdb/run/status/monitor.json", str(status_path))
        after = json.loads(status_path.read_text())
        credential_rejection = "server credentials are incomplete" in log
        assert self.private_values, "credential files contain no secret values to check"
        for path in (self.data_paths[0] / "log").rglob("*"):
            if not path.is_file():
                continue
            body = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
            if any(value.encode() in body for value in self.private_values):
                raise AssertionError("secret leaked into diagnostic logs")
            credential_rejection |= b"server credentials are incomplete" in body
        if any(value in log for value in self.private_values):
            raise AssertionError("secret leaked into container logs")
        return {"action": action, "state": state, "before": before,
                "after": after, "log": log, "credential_rejection": credential_rejection,
                "secret_free": True}
