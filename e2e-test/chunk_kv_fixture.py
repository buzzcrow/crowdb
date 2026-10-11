# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Production Chunk-KV over the protected, persistent storage profile."""

import ipaddress
import json
import time

from allocation import node_data, start_node
from chunkdb_fixture import ChunkdbStorageCluster
from fixture import HTTP, wait


class ChunkKvCluster(ChunkdbStorageCluster):
    def __init__(self, image, artifacts, bundle, empty_catalog=False, **kwargs):
        self.empty_catalog = empty_catalog
        super().__init__(image, artifacts, bundle, storage_backend="file", **kwargs)

    def start(self):
        super().start()
        index = len(self.servers)
        self.chunk_kv_index = index
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        config = self.artifacts / "chunk-kv.toml"
        seeds = json.dumps(self.origins[:3])
        config.write_text(f'''instance_id = 900
rpc_listen_addr = "{address}:7402"
rpc_advertise_addr = "{address}:7402"
http_listen_addr = "0.0.0.0:7401"
http_advertise_addr = "{address}:7401"
group0_mgmt_seeds = {seeds}
catalog_refresh_interval_ms = 200
rpc_workers = 1
[balance]
enabled = false
target_partitions_per_owner = 1
target_partition_bytes = 9223372036854775807
minimum_weighted_improvement_percent = 100
cooldown_ms = 9223372036854775807
max_owner_request_rate = 0
[storage]
metadata_store_id = 0
stream_mirror_copies = 2
tree_chunk_capacity_bytes = 16777216
stream_chunk_capacity_bytes = 16777216
diskio_connections_per_endpoint = 1
diskio_rpc_workers = 1
[bootstrap_partition]
partition_id = {{ high = 1, low = 1 }}
tree_id = 1
stream_name = {{ high = 2, low = 1 }}
owner_epoch = 1
metadata_group_id = 1
''')
        if self.empty_catalog:
            config.write_text(config.read_text().split("[bootstrap_partition]", 1)[0])
        observed = start_node(self, index, node_data(self, index), (
            "server", "chunk-kv", "--config", "/opt/crowdb/etc/e2e-chunk-kv.toml",
            "--log-dir", "/opt/crowdb/data/log/chunk-kv", "--log"), address,
            mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-chunk-kv.toml,readonly",))
        if observed != address:
            raise AssertionError("Chunk-KV inspected IP differs from its advertised endpoint")
        self.addresses.append(observed)
        self.ready_chunk_kv(index)
        (self.artifacts / "chunk-kv-endpoints.json").write_text(json.dumps({
            "ip": address, "rpc_port": 7402, "http_port": 7401, "node": index}, indent=2))
        return self

    def ready_chunk_kv(self, index=None):
        index = self.chunk_kv_index if index is None else index
        address = self.addresses[index]
        def probe():
            state = json.loads(self.command("inspect", self.servers[index]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Chunk-KV exited: " + self.command("logs", self.servers[index]))
            path = "health" if self.empty_catalog else "ready"
            with HTTP.open(f"http://{address}:7401/{path}", timeout=5) as response:
                if self.empty_catalog:
                    health = json.loads(response.read())
                    if health["ready"] or health["hosted_partitions"] != 0 or health["catalog_generation"] != 0:
                        raise AssertionError(f"unpublished Chunk-KV catalog unexpectedly serves data: {health}")
                return response.status == 200
        wait(probe, bool, "production Chunk-KV catalog and partition readiness")

    def crash_restart(self):
        started = time.monotonic()
        self.command("kill", "--signal", "KILL", self.servers[self.chunk_kv_index])
        super().crash_restart()
        self.command("start", self.servers[self.chunk_kv_index])
        self.ready_chunk_kv()
        self.timings["restart"] = time.monotonic() - started
