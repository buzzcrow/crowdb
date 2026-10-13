# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three logical storage nodes with independent production voter containers."""

import ipaddress
import json
import socket

from allocation import node_data, start_node
from fixture import Cluster, HTTP, wait
from timeout_budget import scaled_timeout


class S3NodeCluster(Cluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, node_count=3, stores_per_node=((0,),) * 3,
                         groups=(0, 1, 2, 3), advertise=True, preserve_network=True, **kwargs)
        self.bundle = bundle

    def start(self):
        super().start()
        self.client(self.bundle, "seed", "access_s3::node_failure::seed")
        for logical in range(3):
            self.diskdb(logical)
        for logical in range(3):
            self.chunkdb(logical)
        for logical in range(3):
            self.diskio(logical)
        self.client(self.bundle, "storage-ready", "access_s3::node_failure::seed")
        self.chunk_kv()
        self.access()
        (self.artifacts / "s3-node-endpoints.json").write_text(json.dumps({
            "endpoint": f"http://{self.addresses[13]}:7500",
            "nodes": [{"rack": n + 1, "node": n + 1, "services": [n, n + 3, n + 6, n + 9]}
                      for n in range(3)]}, indent=2))
        return self

    def launch_service(self, index, arguments, config=None, environment=(), preserve_network=False, data=None):
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        mounts = ()
        if config:
            path = self.artifacts / f"service-{index}.toml"
            path.write_text(config)
            mounts = (f"type=bind,source={path},target=/opt/crowdb/etc/e2e-service.toml,readonly",)
        observed = start_node(self, index, data or node_data(self, index), arguments, address,
                              mounts=mounts, environment=environment, preserve_network=preserve_network)
        if observed != address:
            raise AssertionError("inspected service endpoint changed")
        self.addresses.append(observed)
        return address

    def diskdb(self, logical):
        index = logical + 3
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        config = f'''[server]
rpc_workers = 2
listen_addr = "0.0.0.0:7100"
http_listen_addr = "0.0.0.0:7101"
rpc_listen_addr = "{address}:7102"
instance_id = "{10001 + logical}"
kv_server_mgmt_seeds = {json.dumps(self.origins)}
[heartbeat]
interval_secs = 1
'''
        self.launch_service(index, ("server", "diskdb", "--config", "/opt/crowdb/etc/e2e-service.toml",
                                   "--log-dir", "/opt/crowdb/data/log/diskdb", "--log"), config,
                            preserve_network=True)
        wait(lambda: HTTP.open(f"http://{address}:7101/ready", timeout=5).status, lambda code: code == 200,
             "three-node Diskdb readiness", seconds=scaled_timeout(90))

    def chunkdb(self, logical):
        index = logical + 6
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        config = f'''service_ownership = "dynamic"
[deployment]
mode = "production"
[server]
rpc_workers = 2
http_listen_addr = "0.0.0.0:7301"
rpc_listen_addr = "{address}:7302"
instance_id = "{20000 + logical}"
kv_server_mgmt_seeds = {json.dumps(self.origins)}
keepalive_interval_secs = 1
[topology]
refresh_interval_secs = 1
[range_guard]
allow_all_when_empty = false
[placement]
mode = "protected"
allow_unsafe_ec = false
allow_degraded_failure_domains = false
'''
        self.launch_service(index, ("server", "chunkdb", "--config", "/opt/crowdb/etc/e2e-service.toml",
                                   "--log-dir", "/opt/crowdb/data/log/chunkdb", "--log"), config,
                            preserve_network=True)
        wait(lambda: HTTP.open(f"http://{address}:7301/ready", timeout=5).status, lambda code: code == 200,
             "three-node dynamic Chunkdb readiness", seconds=scaled_timeout(90))

    def diskio(self, logical):
        index = logical + 9
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        data = node_data(self, index)
        capacity = 5120 * 1024 * 1024
        path = data / "disk.dat"
        with path.open("wb") as output:
            output.truncate(capacity)
        path.chmod(0o666)
        self.launch_service(index, (
            "server", "diskio", "--port", "7200", "--bind", address, "--no-o-direct",
            "--disk", f"{logical + 1:x}:/opt/crowdb/data/disk.dat:{capacity}",
            "--kv-seeds", ",".join(self.origins), "--instance-id", str(11001 + logical),
            "--rack-id", str(logical + 1), "--node-id", str(logical + 1),
            "--dg-id", str(logical + 1), "--sync-interval-ms", "200", "--auto-discover-disks",
            "--log-dir", "/opt/crowdb/data/log/diskio"), preserve_network=True, data=data)
        wait(lambda: socket.create_connection((address, 7200), timeout=1).close() or True,
             bool, "three-node DiskIO endpoint", seconds=scaled_timeout(90))

    def chunk_kv(self):
        address = str(ipaddress.ip_address(self.addresses[0]) + 12)
        config = f'''instance_id = 900
rpc_listen_addr = "{address}:7402"
rpc_advertise_addr = "{address}:7402"
http_listen_addr = "0.0.0.0:7401"
http_advertise_addr = "{address}:7401"
group0_mgmt_seeds = {json.dumps(self.origins)}
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
'''
        self.launch_service(12, ("server", "chunk-kv", "--config", "/opt/crowdb/etc/e2e-service.toml",
                                "--log-dir", "/opt/crowdb/data/log/chunk-kv", "--log"), config)
        wait(lambda: HTTP.open(f"http://{address}:7401/ready", timeout=5).status, lambda code: code == 200,
             "three-node catalog readiness", seconds=scaled_timeout(90))

    def access(self):
        address = str(ipaddress.ip_address(self.addresses[0]) + 13)
        environment = (f"CROWDB_S3_LISTEN={address}:7500", "CROWDB_ACCESS_HEALTH_LISTEN=0.0.0.0:7501",
                       "CROWDB_MANAGEMENT_SEEDS=" + ",".join(self.origins), "CROWDB_S3_TENANT=local",
                       "CROWDB_S3_MASTER_KEY=" + "11" * 32, "CROWDB_S3_TRUSTED_NETWORK=true", "CROWDB_S3_EC_DATA=2", "CROWDB_S3_EC_CODE=1",
                       "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access")
        self.launch_service(13, ("server", "access", "s3"), environment=environment)
        wait(lambda: HTTP.open(f"http://{address}:7501/_crowdb/health/ready", timeout=5).status,
             lambda code: code == 200, "three-node S3 readiness", seconds=scaled_timeout(90))
