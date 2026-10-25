# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Default shipped anonymous-volume policy; preserve diagnostics before removal."""

import json
import time
from allocation import create_network, start_node
from fixture import LABEL, HTTP, request
from managed_fixture import ManagedCluster


class AnonymousReleaseCluster(ManagedCluster):
    def start(self):
        subnet = create_network(self)
        address = str(subnet.network_address + 2)
        self.addresses.append(start_node(self, 0, None, (), address=address,
            memory="2048m", environment=("CROWDB_DEPLOYMENT_MODE=local", "CROWDB_INITIAL_VOTERS=1",
                                         f"CROWDB_CLUSTER={self.run_id}")))
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            state = json.loads(self.command("inspect", self.servers[0]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("anonymous-volume startup exited: " + self.command("logs", self.servers[0]))
            try:
                self.command("exec", self.servers[0], "/opt/crowdb/bin/crowdb-monitor", "readiness")
                if HTTP.open(f"http://{address}:9090/healthz", timeout=5).read() != b"ok":
                    raise RuntimeError("default release Web health response is invalid")
                if request(f"http://{address}:9090", "/api/cluster/status")["phase"] != "ready":
                    raise RuntimeError("default node application is not ready")
                break
            except (RuntimeError, OSError):
                time.sleep(0.1)
        else:
            raise AssertionError("default anonymous-volume startup never became ready")
        info = json.loads(self.command("inspect", self.servers[0]))[0]
        accepted = json.loads(self.command("exec", self.servers[0], "cat",
                                          "/opt/crowdb/data/accepted-node.json"))
        status = request(f"http://{address}:9090", "/api/cluster/status")
        (self.artifacts / "anonymous-runtime.json").write_text(json.dumps({
            "mounts": info["Mounts"], "accepted": accepted, "status": status,
            "log": self.command("logs", self.servers[0])}, indent=2))
        return self

    def close(self):
        for name in self.servers if not self.closed else ():
            found = self.command("container", "ls", "-a", "--filter", f"label={LABEL}={self.run_id}",
                                 "--filter", f"name={name}", "--format", "{{.Names}}")
            if name not in found.splitlines():
                continue
            info = json.loads(self.command("inspect", name))[0]
            if info["Config"]["Labels"].get(LABEL) != self.run_id:
                raise ValueError("anonymous volume container ownership changed")
            for directory in ("log", "crash"):
                self.command("cp", f"{name}:/opt/crowdb/data/{directory}",
                             str(self.artifacts / directory))
            self.command("container", "rm", "-fv", name)
            self.containers.remove(name)
        super().close()
