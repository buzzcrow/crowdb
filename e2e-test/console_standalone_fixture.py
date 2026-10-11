# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Standalone Web persistence through the supported release server entrypoint."""

import json
from pathlib import PurePosixPath
from allocation import create_network, node_data, start_node
from fixture import Cluster, HTTP, wait


def prepare_config(cluster, index, config):
    data = node_data(cluster, index)
    path = data / "persistent/console/default/config.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    preparation = cluster.run_id + "-web-config-owner"
    cluster.containers.append(preparation)
    cluster.command("run", "--rm", "--name", preparation, "--label",
        f"org.crowdb.e2e.run={cluster.run_id}", "--network", "none", "--user", "0",
        "--entrypoint", "/bin/chown", "--cpus", str(cluster.node_cpus), "--memory", "64m",
        "--mount", f"type=bind,source={data},target=/opt/crowdb/data", cluster.image,
        "-R", "crowdb:crowdb", "/opt/crowdb/data/persistent/console/default")
    return data


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

    def chunkdb_launch(self, action, node):
        super().control({"action": "status", "node": node})
        name = self.servers[node]
        if action == "export-chunkdb-launch":
            self.control({"action": "export-standalone-config", "node": node})
            config = json.loads((self.artifacts / "standalone-config.json").read_text())
            launch = config["local_launches"]["chunkdb-901"]
            path = PurePosixPath(launch["workdir"])
            if ".." in path.parts or not path.is_relative_to("/opt/crowdb/data"):
                raise ValueError("Chunkdb launch workdir outside owned data")
            server = next(server for server in config["server"] if server["id"] == "chunkdb-901")
            pid = server["pid"]
            if type(pid) is not int or pid <= 1:
                raise ValueError("invalid Chunkdb launch PID")
            self.retained_chunkdb = (pid, str(path))
            self.command("cp", f"{name}:{path}/conf/crowdb_chunkdb_config.toml",
                         str(self.artifacts / "chunkdb-launch.toml"))
            return {"pid": pid}
        pid, path = self.retained_chunkdb
        processes = self.command("exec", name, "sh", "-c",
                                 f"if test -f /proc/{pid}/stat; then "
                                 f"awk '{{ print $3 == \"Z\" ? \"dead\" : \"alive\" }}' /proc/{pid}/stat; "
                                 "else echo dead; fi").strip()
        workdir = self.command("exec", name, "find", path, "-maxdepth", "0", "-type", "d").strip()
        return {"alive": processes == "alive", "workdir_exists": workdir == path}

    def control(self, body):
        if body.get("action") in ("export-chunkdb-launch", "inspect-retained-chunkdb-launch"):
            return self.chunkdb_launch(body["action"], body.get("node"))
        if body.get("action") in ("export-standalone-config", "seal-bootstrap-intent"):
            # Reuse the normal lifecycle ownership/index check before file transfer.
            super().control({"action": "status", "node": body.get("node")})
            name = self.servers[body["node"]]
            directory = "/opt/crowdb/data/persistent/console/default"
            if body["action"] == "export-standalone-config":
                self.command("cp", f"{name}:{directory}/config.json",
                             str(self.artifacts / "standalone-config.json"))
                present = self.command("exec", name, "find", directory, "-maxdepth", "1",
                                       "-type", "f", "-name", "bootstrap-intent.toml")
                return {"intent_exists": bool(present)}
            intent = self.artifacts / "bootstrap-intent.toml"
            if not intent.is_file() or intent.stat().st_size > 65536:
                raise ValueError("missing or oversized bootstrap intent")
            self.command("cp", str(intent), f"{name}:{directory}/bootstrap-intent.toml")
            self.command("exec", name, "chown", "crowdb:crowdb", f"{directory}/bootstrap-intent.toml")
            self.command("exec", name, "chmod", "600", f"{directory}/bootstrap-intent.toml")
            return {"sealed": True}
        result = super().control(body)
        if body.get("action") == "start":
            self.ready()
        return result
