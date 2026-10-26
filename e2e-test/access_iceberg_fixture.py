# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Two normal Iceberg frontends over protected persistent catalog storage."""

import ipaddress
import json
import urllib.request

from allocation import node_data, start_node
from chunk_kv_fixture import ChunkKvCluster
from fixture import HTTP, wait


class AccessIcebergCluster(ChunkKvCluster):
    def __init__(self, image, artifacts, bundle, native_files=False, catalog_lifecycle=False, gc_worker=False, gc_page_items=None, gc_interval_ms=100, gc_catalogs=(), deferred_access=False, **kwargs):
        self.native_files = native_files
        self.catalog_lifecycle = catalog_lifecycle or gc_worker
        self.deferred_access = deferred_access
        self.gc_worker = gc_worker
        self.gc_page_items = gc_page_items
        self.gc_interval_ms = gc_interval_ms
        self.gc_catalogs = tuple(gc_catalogs)
        super().__init__(image, artifacts, bundle, **kwargs)

    def start(self):
        super().start()
        if not (self.native_files or self.catalog_lifecycle or self.deferred_access):
            self.client(self.bundle, "iceberg-initialize", "access_iceberg::initialize")
        config = self.artifacts / "iceberg.toml"
        config.write_text("[iceberg]\nlarge_mirror_copies = 2\n\n[iceberg.small_write]\nconversion_enabled = false\nmirror_copies = 2\n")
        for index in (12, 13):
            address = str(ipaddress.ip_address(self.addresses[0]) + index)
            environment = (
                "CROWDB_MANAGEMENT_SEEDS=" + ",".join(self.origins[:3]),
                f"CROWDB_ICEBERG_LISTEN={address}:7502",
                "CROWDB_ACCESS_HEALTH_LISTEN=0.0.0.0:7501",
                "CROWDB_ACCESS_LOG_DIR=/opt/crowdb/data/log/access",
                "CROWDB_ICEBERG_READ_TOKEN=" + "r" * 32,
                "CROWDB_ICEBERG_WRITE_TOKEN=" + "w" * 32,
                "CROWDB_ICEBERG_MANAGE_TOKEN=" + "m" * 32,
                "CROWDB_ICEBERG_CLEAR_TOKEN=" + "c" * 32,
                "CROWDB_ICEBERG_GC_ENABLED=" + ("1" if (self.native_files and index == 13) or self.gc_worker else "0"),
            )
            if self.gc_worker:
                if self.gc_catalogs:
                    environment += ("CROWDB_ICEBERG_GC_CATALOGS=" + ",".join(self.gc_catalogs),)
                if self.gc_interval_ms is not None:
                    environment += (f"CROWDB_ICEBERG_GC_INTERVAL_MS={self.gc_interval_ms}",)
                if self.gc_page_items is not None:
                    environment += (f"CROWDB_ICEBERG_GC_PAGE_ITEMS={self.gc_page_items}",)
            observed = start_node(self, index, node_data(self, index),
                ("server", "access", "iceberg", "--config", "/opt/crowdb/etc/e2e-iceberg.toml", "serve"),
                address, environment=environment,
                mounts=(f"type=bind,source={config},target=/opt/crowdb/etc/e2e-iceberg.toml,readonly",),
                running=not (self.native_files or self.catalog_lifecycle or self.deferred_access))
            if observed != address:
                raise AssertionError("Iceberg inspected IP differs from its listener address")
            self.addresses.append(observed)
            if not (self.native_files or self.catalog_lifecycle or self.deferred_access):
                self.ready_iceberg(index)
        if self.catalog_lifecycle:
            self.command("cp", self.servers[12] + ":/opt/crowdb/bin/crowdb-access-server",
                         str(self.artifacts / "release-access-cli"))
            self.command("cp", self.servers[12] + ":/opt/crowdb/lib",
                         str(self.artifacts / "release-cli-lib"))
        return self

    def ready_iceberg(self, index):
        address = self.addresses[index]
        def probe():
            state = json.loads(self.command("inspect", self.servers[index]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Iceberg exited: " + self.command("logs", self.servers[index]))
            path = "/_crowdb/metrics" if self.gc_worker else "/v1/config"
            token = "m" if self.gc_worker else "r"
            request = urllib.request.Request(f"http://{address}:7502{path}",
                headers={"Authorization": "Bearer " + token * 32})
            with HTTP.open(request, timeout=5) as response:
                return response.status == 200
        wait(probe, bool, "Iceberg authenticated catalog readiness")

    def control(self, body):
        result = super().control(body)
        if body.get("action") == "start":
            if body["node"] in (12, 13):
                self.ready_iceberg(body["node"])
            elif body["node"] == self.chunk_kv_index:
                self.ready_chunk_kv(recovering=True)
        return result
