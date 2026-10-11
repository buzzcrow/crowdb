# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Protected Chunkdb with separately packaged KV and Diskdb dependencies."""

import ipaddress
import json
import socket
import time
from allocation import node_data, start_node
from diskdb_fixture import DiskdbCluster
from fixture import HTTP, wait


class ChunkdbCluster(DiskdbCluster):
    def __init__(self, image, artifacts, bundle, service_instances=(888,), storage_groups=(1,), **kwargs):
        self.service_instances = service_instances
        self.storage_groups = storage_groups
        super().__init__(image, artifacts, bundle,
                         seed_case="chunkdb::hardware::seed", restart_metadata=True, kv_nodes=3, kv_groups=(0, *storage_groups), **kwargs)
        self.disk_group_ids = list(range(1000, 1004))
        self.chunkdb_index = self.diskdb_index + 1

    def start(self):
        super().start()
        endpoints = []
        for offset, instance in enumerate(self.service_instances):
            index = self.chunkdb_index + offset
            endpoints.append(self.start_chunkdb(index, instance))
        (self.artifacts / "chunkdb-endpoints.json").write_text(json.dumps(
            endpoints[0] if len(endpoints) == 1 else {"nodes": endpoints}, indent=2))
        return self

    def start_chunkdb(self, index, instance):
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        config = self.artifacts / f"chunkdb-{instance}.toml"
        groups = ", ".join(f"{{ store_id = 0, group_id = {group} }}" for group in self.storage_groups)
        config.write_text(f'''[slot_bootstrap]
service_instances = {list(self.service_instances)}
storage_groups = [{groups}]
[server]
rpc_workers = 2
http_listen_addr = "0.0.0.0:7301"
rpc_listen_addr = "{address}:7302"
instance_id = "{instance}"
kv_server_mgmt_seeds = ["{self.origins[0]}"]
keepalive_interval_secs = 1
[topology]
refresh_interval_secs = 1
''' + self.extra_config())
        observed = start_node(self, index, node_data(self, index),
            ("server", "chunkdb", "--config", "/opt/crowdb/etc/e2e-chunkdb.toml",
             "--log-dir", "/opt/crowdb/data/log/chunkdb", "--log"),
            address=address, mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-chunkdb.toml,readonly",))
        if observed != address:
            raise AssertionError("Chunkdb inspected IP differs from advertised endpoint")
        self.addresses.append(address)
        self.ready_chunkdb(index)
        return {"ip": address, "rpc_port": 7302, "http_port": 7301, "instance_id": instance}

    def extra_config(self):
        return ""

    def crash_restart(self):
        started = time.monotonic()
        indices = range(self.chunkdb_index, self.chunkdb_index + len(self.service_instances))
        for index in indices:
            self.command("kill", "--signal", "KILL", self.servers[index])
        super().crash_restart()
        for index in indices:
            self.command("start", self.servers[index])
            self.ready_chunkdb(index)
        self.timings["restart"] = time.monotonic() - started

    def ready_chunkdb(self, index=None):
        index = self.chunkdb_index if index is None else index
        def probe():
            state = json.loads(self.command("inspect", self.servers[index]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Chunkdb exited: " + self.command("logs", self.servers[index]))
            with HTTP.open(f"http://{self.addresses[index]}:7301/ready", timeout=5) as response:
                return response.status == 200 and response.read() == b"ok"
        wait(probe, bool, "protected Chunkdb readiness")


class ChunkdbStorageCluster(ChunkdbCluster):
    """Six storage failure domains backed by independently owned Diskio nodes."""

    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, bundle, **kwargs)
        self.seed_case = "chunkdb::hardware::seed_storage"
        self.disk_group_ids = list(range(1000, 1006))

    def diskdb_extra_config(self):
        return "[rebalance]\nenabled = false\nplan_interval_secs = 1\n"

    def extra_config(self):
        return '''[lifecycle]
cache_capacity = 10000
sweep_chunk_lock_interval_secs = 60
lock_hold_warn_threshold_ms = 1000
layout_validity_ms = 1000
[conversion]
enabled = false
data_num = 8
code_num = 4
min_seal_age_secs = 0
min_mirror_strips = 8
max_concurrency = 4
max_bandwidth_mbps = 50
scan_interval_secs = 1
task_lease_secs = 30
'''

    def start(self):
        super().start()
        base = ipaddress.ip_address(self.addresses[0])
        endpoints = []
        self.client(self.bundle, "storage-unavailable", "chunkdb::io::unavailable_before_services_start")
        for offset in range(6):
            index = self.chunkdb_index + len(self.service_instances) + offset
            address = str(base + index)
            data = node_data(self, index)
            self.addresses.append(start_node(self, index, data, (
                "server", "diskio", "--port", "7200", "--bind", address,
                "--dummy-disk", "mem", "--kv-seeds", self.origins[0],
                "--instance-id", str(8000 + offset), "--rack-id", str(100 + offset),
                "--node-id", str(10 + offset), "--dg-id", str(1000 + offset),
                "--sync-interval-ms", "200", "--auto-discover-disks",
                "--log-dir", "/opt/crowdb/data/log/diskio"), address))
            endpoints.append({"node": index, "ip": address, "rpc_port": 7200,
                "instance_id": 8000 + offset, "disk_group_id": 1000 + offset,
                "data": str(data)})
        for endpoint in endpoints:
            def reachable(endpoint=endpoint):
                state = json.loads(self.command("inspect", self.servers[endpoint["node"]]))[0]["State"]
                if not state["Running"]:
                    raise AssertionError("Diskio exited: " + self.command("logs", self.servers[endpoint["node"]]))
                with socket.create_connection((endpoint["ip"], 7200), timeout=1):
                    return True
            wait(reachable, bool, "packaged storage Diskio RPC listener")
        (self.artifacts / "storage-endpoints.json").write_text(json.dumps(endpoints, indent=2))
        self.client(self.bundle, "storage-ready", "chunkdb::io::ready")
        return self
