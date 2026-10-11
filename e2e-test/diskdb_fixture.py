# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Packaged Diskdb with its own packaged KV dependency and host RPC client."""

import ipaddress
import json
import time
from allocation import node_data, start_node
from fixture import Cluster, request, wait


class DiskdbCluster(Cluster):
    def __init__(self, image, artifacts, bundle, restart_metadata=False, **kwargs):
        super().__init__(image, artifacts, node_count=1, groups=(0, 1), advertise=True, **kwargs)
        self.bundle = bundle
        self.restart_metadata = restart_metadata

    def start(self):
        super().start()
        self.client(self.bundle, "seed", "diskdb::hardware::seed")
        data = node_data(self, 1)
        address = str(ipaddress.ip_address(self.addresses[0]) + 1)
        config = self.artifacts / "diskdb.toml"
        config.write_text(f'''[server]
rpc_workers = 2
listen_addr = "0.0.0.0:7100"
http_listen_addr = "0.0.0.0:7101"
rpc_listen_addr = "{address}:7102"
instance_id = "999"
kv_server_mgmt_seeds = ["{self.origins[0]}"]
[storage]
zone_size_bytes = 134217728
block_size_bytes = 1048576
allocate_granularity = 1048576
zone_rotate_count = 4
cas_retry_limit = 100
[sync]
group0_store_id = 0
group0_group_id = 0
sync_interval_secs = 2
[heartbeat]
interval_secs = 2
miss_threshold = 3
temp_failure_timeout_secs = 900
[reporting]
interval_secs = 2
''')
        observed = start_node(self, 1, data, ("server", "diskdb", "--config",
            "/opt/crowdb/etc/e2e-diskdb.toml", "--log-dir", "/opt/crowdb/data/log/diskdb", "--log"),
            address=address, mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-diskdb.toml,readonly",))
        self.addresses.append(address)
        wait(lambda: request(f"http://{address}:7101", "/ready"), lambda _: True,
             "Diskdb packaged server readiness")
        (self.artifacts / "diskdb-endpoints.json").write_text(json.dumps({
            "ip": address, "rpc_port": 7102, "http_port": 7101, "instance_id": 999,
            "disk_group_id": 100, "data": str(data)}, indent=2))
        if observed != address:
            raise AssertionError("Diskdb inspected IP differs from its advertised endpoint")
        return self

    def crash_restart(self):
        started = time.monotonic()
        self.command("kill", "--signal", "KILL", self.servers[1])
        if self.restart_metadata:
            self.command("kill", "--signal", "KILL", self.servers[0])
            self.command("start", self.servers[0])
            Cluster.ready(self)
        self.command("start", self.servers[1])
        for index, name in enumerate(self.servers):
            inspected = json.loads(self.command("inspect", name))[0]
            if inspected["NetworkSettings"]["Networks"][self.run_id]["IPAddress"] != self.addresses[index]:
                raise AssertionError("Diskdb dependency IP changed during persisted restart")
        wait(lambda: request(f"http://{self.addresses[1]}:7101", "/ready"), lambda _: True,
             "recovered Diskdb readiness")
        self.timings["restart"] = time.monotonic() - started
