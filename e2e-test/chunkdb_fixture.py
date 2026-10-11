# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Protected Chunkdb with separately packaged KV and Diskdb dependencies."""

import ipaddress
import json
import time
from allocation import node_data, start_node
from diskdb_fixture import DiskdbCluster
from fixture import HTTP, wait


class ChunkdbCluster(DiskdbCluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, bundle,
                         seed_case="chunkdb::hardware::seed", restart_metadata=True, kv_nodes=3, **kwargs)
        self.disk_group_ids = list(range(1000, 1004))
        self.chunkdb_index = self.diskdb_index + 1

    def start(self):
        super().start()
        address = str(ipaddress.ip_address(self.addresses[0]) + self.chunkdb_index)
        config = self.artifacts / "chunkdb.toml"
        config.write_text(f'''[slot_bootstrap]
service_instances = [888]
storage_groups = [{{ store_id = 0, group_id = 1 }}]
[server]
rpc_workers = 2
http_listen_addr = "0.0.0.0:7301"
rpc_listen_addr = "{address}:7302"
instance_id = "888"
kv_server_mgmt_seeds = ["{self.origins[0]}"]
keepalive_interval_secs = 1
[topology]
refresh_interval_secs = 1
''')
        observed = start_node(self, self.chunkdb_index, node_data(self, self.chunkdb_index),
            ("server", "chunkdb", "--config", "/opt/crowdb/etc/e2e-chunkdb.toml",
             "--log-dir", "/opt/crowdb/data/log/chunkdb", "--log"),
            address=address, mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-chunkdb.toml,readonly",))
        if observed != address:
            raise AssertionError("Chunkdb inspected IP differs from advertised endpoint")
        self.addresses.append(address)
        self.ready_chunkdb()
        (self.artifacts / "chunkdb-endpoints.json").write_text(json.dumps({
            "ip": address, "rpc_port": 7302, "http_port": 7301, "instance_id": 888}, indent=2))
        return self

    def crash_restart(self):
        started = time.monotonic()
        self.command("kill", "--signal", "KILL", self.servers[self.chunkdb_index])
        super().crash_restart()
        self.command("start", self.servers[self.chunkdb_index])
        self.ready_chunkdb()
        self.timings["restart"] = time.monotonic() - started

    def ready_chunkdb(self):
        def probe():
            state = json.loads(self.command("inspect", self.servers[self.chunkdb_index]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Chunkdb exited: " + self.command("logs", self.servers[self.chunkdb_index]))
            with HTTP.open(f"http://{self.addresses[self.chunkdb_index]}:7301/ready", timeout=5) as response:
                return response.status == 200 and response.read() == b"ok"
        wait(probe, bool, "protected Chunkdb readiness")
