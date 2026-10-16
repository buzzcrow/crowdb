# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal release monitor startup with a mounted service-selection profile."""

import json
import time
from pathlib import Path
from allocation import create_network, node_data, start_node
from fixture import Cluster, LABEL, request, wait

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
                     "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "128m", "--pids-limit", "32",
                     "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                     "--entrypoint", "/bin/chown", self.image, "crowdb:crowdb",
                     "/opt/crowdb/data")
        self.containers.remove(preparer)
        profile = self.artifacts / "profile.toml"
        profile.write_text(self.profile())
        self.addresses.append(self.launch(data, profile))
        self.origins = [f"http://{self.addresses[0]}:10000"]
        self.before_ready(data, profile)
        self.ready()
        return self

    def before_ready(self, _data, _profile):
        """Allow owned bootstrap interruption before the normal readiness gate."""

    def profile(self):
        return selected_profile(self.services)

    def launch(self, data, profile):
        return start_node(self, 0, data, (), address=self.address,
            memory="2048m" if len(self.services) > 2 else "512m",
            environment=("CROWDB_STARTUP_MODE=single", "CROWDB_PROFILE_PATH=/opt/crowdb/etc/e2e-profile.toml"),
            mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-profile.toml,readonly",))

    def ready(self):
        deadline = time.monotonic() + 60
        last = None
        while time.monotonic() < deadline:
            cancelled = getattr(self, "control_cancelled", None)
            if cancelled is not None and cancelled.is_set():
                raise RuntimeError("managed readiness cancelled during fixture teardown")
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
                    self.client_configuration()
                    wait(lambda: request(self.origins[0], "/health"), lambda _: True,
                         "host management reachability")
                    return
            except RuntimeError as error:
                last = str(error)
            time.sleep(0.1)
        raise AssertionError(f"normal selected release services never became ready: {last}")

    def client_configuration(self):
        manifest = self.command("exec", self.servers[0], "cat",
                                "/opt/crowdb/data/bootstrap/manifest.json")
        (self.artifacts / "managed-manifest.json").write_text(manifest)
        config_root = self.artifacts / "runtime-config" / "config"
        config_root.mkdir(parents=True, exist_ok=True)
        for service in ("chunkdb", "chunk-kv"):
            if service in self.services:
                body = self.command("exec", self.servers[0], "cat",
                                    f"/opt/crowdb/run/config/{service}.toml")
                (config_root / f"{service}.toml").write_text(body)
        if "access" in self.services:
            credentials = self.artifacts / "client.env"
            credentials.touch(mode=0o600, exist_ok=True)
            credentials.write_text(self.command("exec", self.servers[0],
                "/opt/crowdb/bin/crowdb-monitor", "credentials", "show", "--format", "env"))

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

    def control(self, body):
        if body.get("action") != "recover-service":
            return super().control(body)
        service, signal = body.get("service"), body.get("signal")
        if body.get("node") != 0 or service not in self.services or signal not in ("KILL", "STOP"):
            raise ValueError("invalid owned service recovery request")
        name = self.servers[0]
        info = json.loads(self.command("inspect", name))[0]
        if info["Config"]["Labels"].get(LABEL) != self.run_id or not info["State"]["Running"]:
            raise ValueError("service container ownership or running state changed")
        before = self.monitor_status()["services"][service]
        pid = before["pid"]
        if type(pid) is not int or pid <= 1:
            raise ValueError("service PID must identify a child of the release monitor")
        executable = self.command("exec", name, "cat", f"/proc/{pid}/cmdline").split("\0")[0]
        expected = {"kv": "crowdb-kv-server", "diskdb": "crowdb-diskdb",
                    "diskio": "crowdb-diskio", "chunkdb": "crowdb-chunkdb",
                    "chunk-kv": "crowdb-chunk-kv-server", "access": "crowdb-access-server",
                    "web": "crowdb-web"}[service]
        if Path(executable).name != expected:
            raise ValueError("observed service PID executable changed")
        self.command("exec", name, "kill", "-" + signal, str(pid))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            cancelled = getattr(self, "control_cancelled", None)
            if cancelled is not None and cancelled.is_set():
                raise RuntimeError("owned service recovery cancelled during fixture teardown")
            if not json.loads(self.command("inspect", name))[0]["State"]["Running"]:
                raise AssertionError("release container exited during service recovery")
            state = self.monitor_status()
            after = state["services"][service]
            if (state["phase"] == "ready" and after.get("pid")
                    and after["pid"] != pid and after["generation"] > before["generation"]):
                expected_event = "child_exited" if signal == "KILL" else "probe_failed"
                events = [json.loads(line) for line in self.command(
                    "exec", name, "cat", "/opt/crowdb/data/log/monitor/monitor.log").splitlines()]
                if not any(event.get("service") == service and event.get("kind") == expected_event
                           for event in events):
                    raise AssertionError("service recovery did not record its original fault event")
                self.command("exec", name, "/opt/crowdb/bin/crowdb-monitor", "readiness")
                return {"before": before, "after": after, "event": expected_event}
            time.sleep(0.1)
        raise AssertionError(f"{service} did not recover after {signal} within 90 seconds")

    def monitor_status(self):
        return json.loads(self.command("exec", self.servers[0], "cat",
                                       "/opt/crowdb/run/status/monitor.json"))
