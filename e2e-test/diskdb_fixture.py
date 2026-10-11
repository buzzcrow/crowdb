# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Packaged Diskdb with its own packaged KV dependency and host RPC client."""

import ipaddress
import json
import time
from allocation import node_data, start_node
from fixture import Cluster, request, wait


class DiskdbCluster(Cluster):
    def __init__(self, image, artifacts, bundle, restart_metadata=False, seed_case="diskdb::hardware::seed", kv_nodes=1, kv_groups=(0, 1), **kwargs):
        super().__init__(image, artifacts, node_count=kv_nodes, stores_per_node=((0,),) * kv_nodes,
                         groups=kv_groups, advertise=True, **kwargs)
        self.diskdb_index = kv_nodes
        self.bundle = bundle
        self.seed_case = seed_case
        self.disk_group_ids = [100]
        self.restart_metadata = restart_metadata

    def start(self):
        super().start()
        self.client(self.bundle, "seed", self.seed_case)
        data = node_data(self, self.diskdb_index)
        address = str(ipaddress.ip_address(self.addresses[0]) + self.diskdb_index)
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
''' + self.diskdb_extra_config())
        observed = start_node(self, self.diskdb_index, data, ("server", "diskdb", "--config",
            "/opt/crowdb/etc/e2e-diskdb.toml", "--log-dir", "/opt/crowdb/data/log/diskdb", "--log"),
            address=address, mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-diskdb.toml,readonly",))
        self.addresses.append(address)
        wait(lambda: request(f"http://{address}:7101", "/ready"), lambda _: True,
             "Diskdb packaged server readiness")
        (self.artifacts / "diskdb-endpoints.json").write_text(json.dumps({
            "ip": address, "rpc_port": 7102, "http_port": 7101, "instance_id": 999,
            "disk_group_id": self.disk_group_ids[0],
            "disk_group_ids": self.disk_group_ids, "data": str(data)}, indent=2))
        if observed != address:
            raise AssertionError("Diskdb inspected IP differs from its advertised endpoint")
        return self

    def diskdb_extra_config(self):
        return ""

    def crash_restart(self):
        started = time.monotonic()
        self.command("kill", "--signal", "KILL", self.servers[self.diskdb_index])
        if self.restart_metadata:
            for name in self.servers[:self.diskdb_index]:
                self.command("kill", "--signal", "KILL", name)
            for name in self.servers[:self.diskdb_index]:
                self.command("start", name)
            Cluster.ready(self)
        self.command("start", self.servers[self.diskdb_index])
        for index, name in enumerate(self.servers[:self.diskdb_index + 1]):
            inspected = json.loads(self.command("inspect", name))[0]
            if inspected["NetworkSettings"]["Networks"][self.run_id]["IPAddress"] != self.addresses[index]:
                raise AssertionError("Diskdb dependency IP changed during persisted restart")
        wait(lambda: request(f"http://{self.addresses[self.diskdb_index]}:7101", "/ready"), lambda _: True,
             "recovered Diskdb readiness")
        self.timings["restart"] = time.monotonic() - started
