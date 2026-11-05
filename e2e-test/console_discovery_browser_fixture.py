# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""A release Web client keeps its real monitor observations after provider loss."""

import json
from allocation import LABEL, create_network, node_data, start_node
from console_native_observation import owned
from console_shell_fixture import ShellBrowserCluster
from control import Control
from fixture import Cluster, HTTP, wait


class DiscoveryBrowserCluster(ShellBrowserCluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        Cluster.__init__(self, image, artifacts, node_count=2,
                         wire_replicas=False, advertise=True, **kwargs)
        self.bundle = bundle
        self.rejected = False

    def start(self):
        subnet = create_network(self)
        provider = str(subnet.network_address + 2)
        monitor = "http://127.0.0.1:26135"
        host = json.loads(self.command("info", "--format", "{{json .}}"))["ID"]
        self.addresses.append(start_node(self, 0, node_data(self, 0),
            ("server", "web", "--bind", "0.0.0.0", "--port", "7135",
             "--ui-root", "/opt/crowdb/ui", "--log-dir", "/opt/crowdb/data/log/web",
             "--node-monitor", monitor), address=provider,
            environment=("CROWDB_RUNTIME_ROOT=/opt/crowdb/data",)))
        data = node_data(self, 1, prepare_logs=False)
        preparation = self.run_id + "-provider-owner"
        self.containers.append(preparation)
        self.command("run", "--rm", "--name", preparation, "--label", f"{LABEL}={self.run_id}",
            "--network", "none", "--user", "0", "--entrypoint", "/bin/chown",
            "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "64m",
            "--mount", f"type=bind,source={data},target=/opt/crowdb/data", self.image,
            "-R", "crowdb:crowdb", "/opt/crowdb/data")
        self.addresses.append(start_node(self, 1, data, (), address=provider,
            environment=("CROWDB_DEPLOYMENT_MODE=production", "CROWDB_INITIAL_VOTERS=3",
                         "CROWDB_SSH_PASSWORD=crowdb", f"CROWDB_CLUSTER={self.run_id}",
                         f"CROWDB_PHYSICAL_HOST_ID={host}"),
            network_container=self.servers[0]))
        wait(lambda: HTTP.open(f"http://{self.addresses[0]}:7135/healthz", timeout=5).read(),
             lambda body: body == b"ok", "independent release Web", seconds=20)
        wait(lambda: HTTP.open(f"http://{self.addresses[0]}:7135/api/node/candidates", timeout=5).read(),
             lambda body: json.loads(body)["local"]["advertisement"]["monitor_endpoints"],
             "release monitor observations", seconds=10)
        return self

    def browser(self, name, *, config="shell.config.ts", **kwargs):
        with Control(self) as control:
            super().browser(name, config=config, extra_env={
                "CROWDB_E2E_CONTROL": str(control.directory),
                "CROWDB_WEB_E2E_BASE_URL": f"http://{self.addresses[0]}:7135"})

    def control(self, body):
        if body != {"action": "stop-discovery-provider"}:
            raise ValueError("unsupported discovery browser control")
        identity = owned(self, 1)
        self.command("kill", "--signal", "TERM", identity)
        return {"signalled": True}
