# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Writable Web deploys packaged Access over the isolated production storage chain."""

import json

from allocation import LABEL, node_data, start_node
from chunk_kv_fixture import ChunkKvCluster
from fixture import HTTP, wait


class AccessDeploymentCluster(ChunkKvCluster):
    def start(self):
        super().start()
        self.client(self.bundle, "catalog-initialize", "access_iceberg::initialize")
        self.client(self.bundle, "deployment-authority", "console::web_access::prepare")
        data = node_data(self, 12)
        config = data / "persistent/console/default/config.json"
        config.parent.mkdir(parents=True)
        config.write_text(json.dumps({"rack": [{"id": 1, "name": "native fixture"}],
            "node": [{"id": node, "rack_id": 1, "host": "127.0.0.1", "ssh_port": 22, "ssh_user": ""}
                     for node in (1, 2, 3)],
            "server": [{"id": f"kv-{index + 1}", "url": origin, "node_id": index + 1,
                        "auto_start": False} for index, origin in enumerate(self.origins[:3])]}))
        config.chmod(0o600)
        preparation = self.run_id + "-web-config-owner"
        self.containers.append(preparation)
        self.command("run", "--rm", "--name", preparation, "--label", f"{LABEL}={self.run_id}",
                     "--network", "none", "--user", "0", "--entrypoint", "/bin/chown",
                     "--cpus", str(self.node_cpus), "--memory", "64m",
                     "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                     self.image, "-R", "crowdb:crowdb", "/opt/crowdb/data/persistent/console/default")
        address = start_node(self, 12, data,
            ("server", "web", "--bind", "0.0.0.0", "--port", "9090",
             "--ui-root", "/opt/crowdb/ui", "--log-dir", "/opt/crowdb/data/log/web"),
            environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",))
        self.addresses.append(address)
        wait(lambda: HTTP.open(f"http://{address}:9090/healthz", timeout=5).read(),
             lambda body: body == b"ok", "release Access deployment Web listener", seconds=20)
        return self

    def control(self, body):
        action = body.get("action")
        if action not in ("access-credentials", "access-stopped"):
            return super().control(body)
        if body.get("node") != 12:
            raise ValueError("Access deployment control requires its owned Web node")
        super().control({"action": "status", "node": 12})
        container = self.servers[12]
        if action == "access-stopped":
            pid = self.access_pid
            alive = self.command("exec", container, "sh", "-c",
                f"if test -f /proc/{pid}/stat; then "
                f"awk '{{ print $3 == \"Z\" ? \"dead\" : \"alive\" }}' /proc/{pid}/stat; "
                "else echo dead; fi").strip()
            return {"alive": alive == "alive", "pid": pid}
        path = "/opt/crowdb/data/persistent/console/default/config.json"
        config = json.loads(self.command("exec", container, "cat", path))
        entry = next(server for server in config["server"] if server["id"] == "access-server-902")
        pid = entry["pid"]
        if type(pid) is not int or pid <= 1:
            raise ValueError("invalid owned Access PID")
        self.access_pid = pid
        credentials = "/opt/crowdb/data/persistent/console/default/secrets/client.env"
        mode = self.command("exec", container, "stat", "-c", "%a", credentials).strip()
        size = int(self.command("exec", container, "stat", "-c", "%s", credentials))
        digest = self.command("exec", container, "sha256sum", credentials).split()[0]
        return {"mode": mode, "size": size, "sha256": digest, "pid": pid}
