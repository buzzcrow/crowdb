# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Normal Dataset listener over persistent protected Chunk-KV storage."""

import ipaddress
import urllib.error
import urllib.request

from allocation import node_data, start_node
from chunk_kv_fixture import ChunkKvCluster
from fixture import HTTP, wait


class AccessDatasetCluster(ChunkKvCluster):
    def start(self):
        super().start()
        index = 12
        address = str(ipaddress.ip_address(self.addresses[0]) + index)
        observed = start_node(self, index, node_data(self, index),
            ("server", "access", "dataset", "serve"), address,
            environment=(
                "CROWDB_MANAGEMENT_SEEDS=" + ",".join(self.origins[:3]),
                f"CROWDB_DATASET_LISTEN={address}:7503",
                "CROWDB_DATASET_READ_TOKEN=token",
                "CROWDB_DATASET_NAME=e2e",
                "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access",
            ))
        if observed != address:
            raise AssertionError("Dataset inspected address differs from its listener")
        self.addresses.append(observed)
        self.ready_dataset()
        return self

    def ready_dataset(self):
        def probe():
            try:
                HTTP.open(urllib.request.Request(
                    f"http://{self.addresses[12]}:7503/v1/dataset/open"), timeout=2)
            except urllib.error.HTTPError as error:
                return error.code == 401
            return False
        wait(probe, bool, "Dataset authenticated listener readiness")

    def control(self, body):
        result = super().control(body)
        if body.get("action") == "start":
            if body["node"] == 12:
                self.ready_dataset()
            elif body["node"] == self.chunk_kv_index:
                self.ready_chunk_kv()
        return result
