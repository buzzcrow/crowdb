# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal release monitor startup with a mounted service-selection profile."""

import json
import time
from pathlib import Path
from allocation import create_network, node_data, start_node
from fixture import Cluster, request, wait

SOURCE = Path(__file__).resolve().parents[1] / "container/single-node-container"


def selected_profile(services):
    parts = (SOURCE / "profile.toml").read_text().split("[[services]]")
    selected = [part for part in parts[1:] if any(
        part.splitlines()[1] == f'id = "{service}"' for service in services)]
    if len(selected) != len(services):
        raise ValueError("unknown or duplicate release service selection")
    profile = parts[0] + "".join("[[services]]" + part for part in selected)
    return profile.replace('"--management-port", "10000"',
                           '"--management-addr", "0.0.0.0", "--management-port", "10000"')


class ManagedCluster(Cluster):
    def __init__(self, image, artifacts, services=("kv",), **kwargs):
        super().__init__(image, artifacts, node_count=1, wire_replicas=False, advertise=True, **kwargs)
        self.services = services

    def start(self):
        subnet = create_network(self)
        self.address = str(subnet.network_address + 2)
        data = node_data(self, 0, prepare_logs=False)
        preparer = self.run_id + "-mount-owner"
        self.containers.append(preparer)
        self.command("run", "--rm", "--name", preparer, "--label",
                     f"org.crowdb.e2e.run={self.run_id}", "--network", "none",
                     "--cpus", "0.5", "--memory", "128m", "--pids-limit", "32",
                     "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                     "--entrypoint", "/bin/chown", self.image, "crowdb:crowdb",
                     "/opt/crowdb/data")
        self.containers.remove(preparer)
        profile = self.artifacts / "profile.toml"
        profile.write_text(self.profile())
        self.addresses.append(self.launch(data, profile))
        self.origins = [f"http://{self.addresses[0]}:10000"]
        self.ready()
        return self

    def profile(self):
        return selected_profile(self.services)

    def launch(self, data, profile):
        return start_node(self, 0, data, (), address=self.address,
            cpus="2" if len(self.services) > 2 else "0.5",
            memory="2048m" if len(self.services) > 2 else "512m",
            environment=("CROWDB_STARTUP_MODE=single", "CROWDB_PROFILE_PATH=/opt/crowdb/etc/e2e-profile.toml"),
            mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-profile.toml,readonly",))

    def ready(self):
        deadline = time.monotonic() + 60
        last = None
        while time.monotonic() < deadline:
            state = json.loads(self.command("inspect", self.servers[0]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("normal release monitor exited: " + self.command("logs", self.servers[0]))
            try:
                last = json.loads(self.command("exec", self.servers[0], "cat",
                                              "/opt/crowdb/run/status/monitor.json"))
                if last["phase"] == "ready":
                    if set(last["services"]) != set(self.services):
                        raise AssertionError(f"unexpected running services: {last}")
                    self.command("exec", self.servers[0], "/opt/crowdb/bin/crowdb-monitor", "readiness")
                    (self.artifacts / "managed-status.json").write_text(json.dumps(last, indent=2))
                    wait(lambda: request(self.origins[0], "/health"), lambda _: True,
                         "host management reachability")
                    return
            except RuntimeError as error:
                last = str(error)
            time.sleep(0.1)
        raise AssertionError(f"normal selected release services never became ready: {last}")

    def crash_restart(self):
        started = time.monotonic()
        self.command("kill", "--signal", "KILL", self.servers[0])
        self.command("rm", self.servers[0])
        self.servers.clear()
        self.containers.remove(self.run_id + "-node0")
        self.launch(self.data_paths[0], self.artifacts / "profile.toml")
        address = json.loads(self.command("inspect", self.servers[0]))[0]["NetworkSettings"]["Networks"][self.run_id]["IPAddress"]
        if address != self.addresses[0]:
            raise AssertionError("managed node IP changed across restart")
        self.ready()
        self.timings["restart"] = time.monotonic() - started
