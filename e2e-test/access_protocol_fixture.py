# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal combined S3/Iceberg listeners with independent protection policies."""

import ipaddress
import json

from allocation import node_data, start_node
from chunk_kv_fixture import ChunkKvCluster
from fixture import HTTP, wait


class AccessProtocolCluster(ChunkKvCluster):
    def start(self):
        super().start()
        self.client(self.bundle, "iceberg-initialize", "access_iceberg::initialize")
        index = len(self.servers)
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        config = self.artifacts / "access.toml"
        config.write_text('''[s3]
ec_data = 2
ec_code = 1
large_prefetch_strips_per_chunk = 2
large_memory_budget_bytes = 67108864
[s3.small_write]
conversion_enabled = false
mirror_copies = 2
[iceberg]
ec_data = 4
ec_code = 2
large_prefetch_strips_per_chunk = 3
large_memory_budget_bytes = 100663296
[iceberg.small_write]
conversion_enabled = false
mirror_copies = 2
''')
        environment = (
            "CROWDB_MANAGEMENT_SEEDS=" + ",".join(self.origins[:3]),
            f"CROWDB_S3_LISTEN={address}:7500", f"CROWDB_ICEBERG_LISTEN={address}:7502",
            "CROWDB_ACCESS_HEALTH_LISTEN=0.0.0.0:7501",
            "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access",
            "CROWDB_S3_TENANT=local", "CROWDB_S3_MASTER_KEY=" + "11" * 32,
            "CROWDB_S3_REGION=us-east-1", "CROWDB_S3_TRUSTED_NETWORK=true",
            "CROWDB_ICEBERG_READ_TOKEN=" + "r" * 32, "CROWDB_ICEBERG_WRITE_TOKEN=" + "w" * 32,
            "CROWDB_ICEBERG_MANAGE_TOKEN=" + "m" * 32, "CROWDB_ICEBERG_CLEAR_TOKEN=" + "c" * 32,
            "CROWDB_ICEBERG_GC_ENABLED=0",
        )
        observed = start_node(self, index, node_data(self, index),
            ("server", "access", "--config", "/opt/crowdb/etc/e2e-access.toml"),
            address, environment=environment,
            mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-access.toml,readonly",))
        if observed != address:
            raise AssertionError("combined Access inspected IP differs from its listener address")
        self.addresses.append(observed)
        def probe():
            state = json.loads(self.command("inspect", self.servers[index]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("combined Access exited: " + self.command("logs", self.servers[index]))
            with HTTP.open(f"http://{address}:7501/_crowdb/health/ready", timeout=5) as response:
                if response.status != 200:
                    return False
            import urllib.request
            request = urllib.request.Request(f"http://{address}:7502/v1/config",
                                             headers={"Authorization": "Bearer " + "r" * 32})
            with HTTP.open(request, timeout=5) as response:
                return response.status == 200
        wait(probe, bool, "combined Access S3 and Iceberg readiness")
        return self
