# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal node startup with a sealed service-selection policy."""

import json
import time
from pathlib import Path
from allocation import create_network, node_data, start_node
from fixture import Cluster, LABEL, request, wait

SOURCE = Path(__file__).resolve().parents[1] / "container/oci-image"


def selected_profile(services):
    known = {"kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"}
    if len(set(services)) != len(services) or not set(services) <= known:
        raise ValueError("unknown or duplicate release service selection")
    return (SOURCE / "node-profile.toml").read_text()


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
        listeners = {
            "paxos-kv": ("kv", {"http_port": 10000, "rpc_port": 10100}),
            "diskdb": ("diskdb", {"rpc_port": 7100}),
            "diskio": ("diskio", {"rpc_port": 7200}),
            "chunkdb": ("chunkdb", {"http_port": 7300, "rpc_port": 7301}),
            "chunk-kv": ("chunk-kv", {"http_port": 7400, "rpc_port": 7401}),
            "access-server": ("access", {"http_port": 9092, "s3_port": 9091,
                                         "dataset_port": 9093, "health_port": 9094}),
        }
        policy = self.artifacts / "node-policy.toml"
        choices = []
        for kind, (role, ports) in listeners.items():
            ports = ", ".join(f"{key} = {value}" for key, value in ports.items())
            enabled = str(role in self.services).lower()
            choices.append(f'{{ kind = "{kind}", enabled = {enabled}, listeners = {{ {ports} }} }}')
        choices = ", ".join(choices)
        storage = 'mode = "automatic"\ncapacity_bytes = 17179869184\nreserve_bytes = 1073741824\nsparse = true' \
            if "diskio" in self.services else 'mode = "diskless"'
        policy.write_text(f'services = [{choices}]\n[storage]\n{storage}\n')
        return start_node(self, 0, data, (), address=self.address,
            memory="2048m" if len(self.services) > 2 else "512m",
            environment=("CROWDB_DEPLOYMENT_MODE=local", "CROWDB_INITIAL_VOTERS=1",
                f"CROWDB_CLUSTER={self.run_id}", "CROWDB_PROFILE_PATH=/opt/crowdb/etc/e2e-profile.toml",
                "CROWDB_NODE_POLICY_FILE=/run/e2e-node-policy.toml"),
            mounts=(f"type=bind,source={profile},target=/opt/crowdb/etc/e2e-profile.toml,readonly",
                    f"type=bind,source={policy},target=/run/e2e-node-policy.toml,readonly"))

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
                report = request(f"http://{self.addresses[0]}:9090", "/api/cluster/status")
                last = report
                if report["phase"] == "failed":
                    raise AssertionError(f"normal selected release services failed: {report}")
                if report["phase"] == "ready":
                    status = self.monitor_status()
                    expected = {"web", "kv"} | {role + "-1" for role in self.services
                                                   if role not in ("web", "kv")}
                    if set(status["services"]) != expected:
                        raise AssertionError(f"unexpected running services: {status}")
                    self.command("exec", self.servers[0], "/opt/crowdb/bin/crowdb-monitor", "readiness")
                    (self.artifacts / "managed-status.json").write_text(json.dumps(status, indent=2))
                    self.client_configuration()
                    wait(lambda: request(self.origins[0], "/health"), lambda _: True,
                         "host management reachability")
                    return
            except (RuntimeError, OSError) as error:
                last = str(error)
            time.sleep(0.1)
        raise AssertionError(f"normal selected release services never became ready: {last}")

    def client_configuration(self):
        accepted = self.command("exec", self.servers[0], "cat",
                                "/opt/crowdb/data/accepted-node.json")
        (self.artifacts / "managed-node.json").write_text(accepted)
        config_root = self.artifacts / "runtime-config" / "config"
        config_root.mkdir(parents=True, exist_ok=True)
        for role in ("chunkdb", "chunk-kv"):
            if role in self.services:
                body = self.command("exec", self.servers[0], "cat",
                                    f"/opt/crowdb/data/services/{role}-1/service.toml")
                (config_root / f"{role}.toml").write_text(body)
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

    def service_status(self, service, status=None):
        key = service if service in ("kv", "web") else service + "-1"
        return (self.monitor_status() if status is None else status)["services"][key]

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
        before = self.service_status(service)
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
        hung = None
        if signal == "STOP":
            try:
                hung = self.observe_hang(name, service, before)
            finally:
                if self.service_status(service).get("pid") == pid:
                    self.command("exec", name, "kill", "-CONT", str(pid))
            # Readiness observes a live process; explicit termination triggers
            # the monitor's eligible-exit recovery policy.
            self.command("exec", name, "kill", "-KILL", str(pid))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            cancelled = getattr(self, "control_cancelled", None)
            if cancelled is not None and cancelled.is_set():
                raise RuntimeError("owned service recovery cancelled during fixture teardown")
            if not json.loads(self.command("inspect", name))[0]["State"]["Running"]:
                raise AssertionError("release container exited during service recovery")
            state = self.monitor_status()
            after = self.service_status(service, state)
            if (state["phase"] == "ready" and after.get("pid")
                    and after["pid"] != pid and after["generation"] > before["generation"]):
                if service not in ("kv", "web"):
                    execution = json.loads(self.command("exec", name, "cat",
                        f"/opt/crowdb/data/services/{service}-1/execution.json"))
                    if (not execution["complete"] or execution["pid"] != after["pid"]
                            or execution["intent"]["operation_id"] != before["operation_id"]):
                        raise AssertionError("service recovery changed its accepted operation or durable process result")
                self.command("exec", name, "/opt/crowdb/bin/crowdb-monitor", "readiness")
                return {"before": before, "after": after, "signal": signal, "hung": hung,
                        "recovery_trigger": "explicit-termination" if hung else "exit"}
            time.sleep(0.1)
        raise AssertionError(f"{service} did not recover after {signal} within 90 seconds")

    def observe_hang(self, name, service, before):
        state = self.command("exec", name, "cat", f"/proc/{before['pid']}/status")
        if "State:\tT" not in state:
            raise AssertionError("owned process was not stopped by the injected fault")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            current = self.service_status(service)
            if (current.get("pid") != before["pid"]
                    or current["generation"] != before["generation"]):
                raise AssertionError("health observation restarted a live process")
            if not current["healthy"]:
                return current
            time.sleep(0.1)
        raise AssertionError(f"{service} hang was not reported by the independent monitor probe")

    def monitor_status(self):
        return json.loads(self.command("exec", self.servers[0], "cat",
                                       "/opt/crowdb/run/status/monitor.json"))
