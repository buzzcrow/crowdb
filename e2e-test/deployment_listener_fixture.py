# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Require combined access startup to exit when one protocol listener is occupied."""

import json
import time
from allocation import node_data
from managed_fixture import ManagedCluster


class OccupiedAccessCluster(ManagedCluster):
    def __init__(self, image, artifacts, protocol, services, **kwargs):
        super().__init__(image, artifacts, services=services, **kwargs)
        self.protocol = protocol

    def start(self):
        super().start()
        config = self.command("exec", self.servers[0], "cat", "/opt/crowdb/run/config/access.toml")
        old, new = (("0.0.0.0:9092", "127.0.0.1:18080") if self.protocol == "s3"
                    else ("0.0.0.0:9091", "127.0.0.1:18181"))
        if old not in config:
            raise AssertionError("unoccupied protocol listener must be relocated")
        path = self.artifacts / "occupied-access.toml"
        path.write_text(config.replace(old, new))
        private = self.artifacts / "server.env"
        private.touch(mode=0o600)
        private.write_text(self.command("exec", self.servers[0], "cat",
                                       "/opt/crowdb/data/secrets/server.env"))
        data = node_data(self, 1, prepare_logs=False)
        name = self.run_id + "-occupied-access"
        self.containers.append(name)
        self.command("run", "-d", "--name", name, "--label", f"org.crowdb.e2e.run={self.run_id}",
            "--network", "container:" + self.servers[0], "--cpus", "0.5", "--memory", "512m",
            "--pids-limit", "256", "--ulimit", "core=-1", "--env-file", str(private),
            "--env", "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access",
            "--env", "CROWDB_ACCESS_HEALTH_LISTEN=127.0.0.1:19094",
            "--env", "CROWDB_ICEBERG_PUBLIC_URI=http://127.0.0.1:9092",
            "--env", "CROWDB_S3_PUBLIC_URI=http://127.0.0.1:9091",
            "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
            "--mount", f"type=bind,source={path},target=/opt/crowdb/etc/occupied-access.toml,readonly",
            self.image, "server", "access", "--config", "/opt/crowdb/etc/occupied-access.toml")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = json.loads(self.command("inspect", name))[0]["State"]
            if state["Status"] == "exited":
                self.command("exec", self.servers[0], "/opt/crowdb/bin/crowdb-monitor", "readiness")
                (self.artifacts / "listener-failure.json").write_text(json.dumps({
                    "state": state, "log": self.command("logs", name),
                    "protocol": self.protocol}, indent=2))
                return self
            time.sleep(0.1)
        raise AssertionError("combined access listener failure did not terminate within 30 seconds")
