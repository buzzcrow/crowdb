# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three production KV voters and a normal writable Web deployment node."""

import json
from allocation import LABEL, node_data, start_node
from console_standalone_fixture import StandaloneWebCluster
from fixture import Cluster, HTTP, wait


class TypedWebCluster(StandaloneWebCluster):
    def __init__(self, image, artifacts, **kwargs):
        Cluster.__init__(self, image, artifacts, node_count=3, store_ids=(0, 0, 0),
                         groups=(0, 1), wire_replicas=True, advertise=True, **kwargs)
        self.rejected = False

    def start(self):
        Cluster.start(self)
        data = node_data(self, 3)
        config = data / "persistent/console/default/config.json"
        config.parent.mkdir(parents=True)
        config.write_text(json.dumps({"rack": [{"id": 1, "name": "test rack"}],
            "node": [{"id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_port": 22, "ssh_user": ""}],
            "server": [
            {"id": f"kv-{index + 1}", "url": origin, "node_id": index + 1, "auto_start": False}
            for index, origin in enumerate(self.origins)]}))
        config.chmod(0o600)
        preparation = self.run_id + "-web-config-owner"
        self.containers.append(preparation)
        self.command("run", "--rm", "--name", preparation, "--label", f"{LABEL}={self.run_id}",
                     "--network", "none", "--user", "0", "--entrypoint", "/bin/chown",
                     "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "64m",
                     "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                     self.image, "-R", "crowdb:crowdb", "/opt/crowdb/data/persistent/console/default")
        address = start_node(self, 3, data,
            ("server", "web", "--skip-startup-restore", "--bind", "0.0.0.0", "--port", "9090",
             "--ui-root", "/opt/crowdb/ui", "--log-dir", "/opt/crowdb/data/log/web"),
            environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",))
        self.addresses.append(address)
        self.ready()
        return self

    def ready(self):
        if len(self.servers) < 4:
            return Cluster.ready(self)
        wait(lambda: HTTP.open(f"http://{self.addresses[3]}:9090/healthz", timeout=5).read(),
             lambda body: body == b"ok", "typed release Web listener", seconds=20)
