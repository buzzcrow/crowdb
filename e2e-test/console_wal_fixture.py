# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Writable Web over separately owned KV nodes using normal file WALs."""

import json
from allocation import LABEL, create_network, node_data, start_node
from fixture import Cluster, HTTP, wait
from kv_node import arguments


class WalWebCluster(Cluster):
    def __init__(self, image, artifacts, nodes, **kwargs):
        super().__init__(image, artifacts, node_count=nodes, store_ids=(0,) * nodes,
                         groups=(), wire_replicas=False, advertise=True,
                         node_args=tuple(("--node-id", str(index + 1)) for index in range(nodes)),
                         **kwargs)

    def start(self):
        subnet = create_network(self)
        for index in range(self.node_count):
            data = node_data(self, index)
            address = str(subnet.network_address + index + 2)
            args = list(arguments(self, index, address))
            args[args.index("--wal-backend") + 1] = "file"
            self.addresses.append(start_node(self, index, data, tuple(args), address,
                                             preserve_network=self.preserve_network))
            self.origins.append(f"http://{address}:7000")
        Cluster.ready(self)
        index = self.node_count
        data = node_data(self, index)
        config = data / "persistent/console/default/config.json"
        config.parent.mkdir(parents=True)
        config.write_text(json.dumps({
            "rack": [], "node": [],
            "server": [{"id": f"kv-{node + 1}", "url": origin,
                        "node_id": node + 1, "auto_start": False}
                       for node, origin in enumerate(self.origins)]}))
        config.chmod(0o600)
        preparation = self.run_id + "-web-config-owner"
        self.containers.append(preparation)
        self.command("run", "--rm", "--name", preparation, "--label", f"{LABEL}={self.run_id}",
                     "--network", "none", "--user", "0", "--entrypoint", "/bin/chown",
                     "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "64m",
                     "--mount", f"type=bind,source={data},target=/opt/crowdb/data", self.image,
                     "-R", "crowdb:crowdb", "/opt/crowdb/data/persistent/console/default")
        self.addresses.append(start_node(self, index, data,
            ("server", "web", "--skip-startup-restore", "--bind", "0.0.0.0", "--port", "6135",
             "--ui-root", "/opt/crowdb/ui", "--log-dir", "/opt/crowdb/data/log/web"),
            environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",)))
        wait(lambda: HTTP.open(f"http://{self.addresses[-1]}:6135/healthz", timeout=5).read(),
             lambda body: body == b"ok", "WAL recovery release Web listener", seconds=20)
        return self

    def control(self, body):
        action = body.get("action")
        if action not in ("stop-voters", "start-voters"):
            return super().control(body)
        if type(body.get("node")) is not int or body["node"] != 0:
            raise ValueError("voter-group action requires its owned first node")
        for index in range(self.node_count):
            super().control({"action": "status", "node": index})
        names = self.servers[:self.node_count]
        args = ("stop", "--time", "10") if action == "stop-voters" else ("start",)
        self.command(*args, *names)
        return {"nodes": self.node_count}
