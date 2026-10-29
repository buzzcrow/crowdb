# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Readiness must distinguish retained registrations from the current boot."""

import tempfile
import unittest
from unittest.mock import Mock, call, patch

from chunkdb_fixture import ChunkdbCluster, ChunkdbStorageCluster
from chunk_kv_fixture import ChunkKvCluster
from access_iceberg_fixture import AccessIcebergCluster
from access_dataset_fixture import AccessDatasetCluster
from s3_node_fixture import S3NodeCluster
from s3_recovery import run_recovery


class StorageReadinessTest(unittest.TestCase):
    def test_initial_storage_launch_publishes_its_freshness_boundary_first(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = ChunkdbStorageCluster("image", directory, "bundle")
            cluster.addresses = ["10.20.30.2"] * 5
            cluster.origins = ["http://10.20.30.2:7000"]

            def launch(*args, **_kwargs):
                self.assertEqual((cluster.artifacts / "storage-started-at-ms").read_text(), "2000")
                return args[4]

            with patch.object(ChunkdbCluster, "start"), patch.object(cluster, "client"), \
                 patch("chunkdb_fixture.wait"), patch("chunkdb_fixture.start_node", side_effect=launch), \
                 patch("chunkdb_fixture.time.time_ns", return_value=2_000_000_000):
                cluster.start()

    def test_restart_replaces_the_boundary_after_metadata_recovery_before_diskio_start(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = ChunkdbStorageCluster("image", directory, "bundle")
            cluster.servers = [f"node{index}" for index in range(11)]
            marker = cluster.artifacts / "storage-started-at-ms"
            marker.write_text("1000")
            starts = []

            def command(*args):
                if args[0] == "start":
                    self.assertEqual(marker.read_text(), "2000")
                    starts.append(args[1])

            def metadata_restart():
                self.assertEqual(marker.read_text(), "1000")

            cluster.command = command
            with patch.object(ChunkdbCluster, "crash_restart", side_effect=metadata_restart), \
                 patch.object(cluster, "client") as ready, \
                 patch("chunkdb_fixture.time.time_ns", return_value=2_000_000_000):
                cluster.crash_restart()
            self.assertEqual(starts, cluster.servers[5:11])
            ready.assert_called_once_with("bundle", "storage-ready-recovered", "chunkdb::io::ready")

    def test_three_node_consumers_start_after_storage_ownership_is_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = S3NodeCluster("image", directory, "bundle")
            cluster.addresses = [f"10.20.30.{index + 2}" for index in range(14)]
            events = []
            cluster.client = lambda _bundle, phase, _case: events.append(phase)
            cluster.diskdb = lambda node: events.append(("diskdb", node))
            cluster.diskio = lambda node: events.append(("diskio", node))
            cluster.chunkdb = lambda node: events.append(("chunkdb", node))
            cluster.chunk_kv = lambda: events.append("chunk-kv")
            cluster.access = lambda: events.append("access")
            with patch("s3_node_fixture.Cluster.start"):
                cluster.start()
            self.assertEqual(events, ["seed",
                *(("diskdb", node) for node in range(3)),
                "storage-seed",
                *(("diskio", node) for node in range(3)),
                "storage-ready",
                *(("chunkdb", node) for node in range(3)),
                "chunk-kv", "access"])

    def test_three_node_service_indexes_survive_dependency_ordered_launch(self):
        from allocation import node_data
        with tempfile.TemporaryDirectory() as directory:
            cluster = S3NodeCluster("image", directory, "bundle")
            base = "10.20.30."
            def start_metadata():
                cluster.addresses = [base + str(index + 2) for index in range(3)]
                for index in range(3):
                    node_data(cluster, index)
                    cluster.servers.append(f"{cluster.run_id}-node{index}")
            def launch(_cluster, index, data, arguments, address, **_kwargs):
                cluster.servers.append(f"{cluster.run_id}-node{index}")
                return address
            def ready(_bundle, phase, _case):
                if phase == "seed":
                    self.assertEqual(cluster.addresses, [base + str(i + 2) for i in range(3)])
                if phase == "storage-seed":
                    self.assertEqual(cluster.servers, [f"{cluster.run_id}-node{i}" for i in range(6)])
                if phase == "storage-ready":
                    self.assertEqual(len(cluster.addresses), 14)
                    self.assertEqual(cluster.addresses[9:12], [base + str(i + 2) for i in range(9, 12)])
            cluster.client = ready
            cluster.diskdb = lambda node: cluster.launch_service(node + 3, ())
            cluster.diskio = lambda node: cluster.launch_service(node + 9, ())
            cluster.chunkdb = lambda node: cluster.launch_service(node + 6, ())
            cluster.chunk_kv = lambda: cluster.launch_service(12, ())
            cluster.access = lambda: cluster.launch_service(13, ())
            with patch("s3_node_fixture.Cluster.start", side_effect=start_metadata),                  patch("s3_node_fixture.start_node", side_effect=launch):
                cluster.start()
            self.assertEqual(cluster.servers, [f"{cluster.run_id}-node{i}" for i in range(14)])
            self.assertEqual([path.parent.name for path in cluster.data_paths], [f"node{i}" for i in range(14)])
            mounts = __import__("json").loads((cluster.artifacts / "mounts.json").read_text())
            self.assertEqual(mounts["host_data"], [str(path) for path in cluster.data_paths])

    def test_s3_chunk_kv_restart_selects_fenced_recovery_readiness(self):
        cluster = Mock()
        cluster.timings = {}
        run_recovery(cluster, "chunk-kv")
        cluster.ready_chunk_kv.assert_called_once_with(recovering=True)
        self.assertEqual(cluster.control.call_args_list, [
            call({"action": "kill", "node": 11}),
            call({"action": "start", "node": 11})])

    def test_protocol_restart_selects_fenced_recovery_readiness(self):
        for kind in (AccessIcebergCluster, AccessDatasetCluster):
            with self.subTest(kind=kind.__name__):
                cluster = object.__new__(kind)
                cluster.chunk_kv_index = 11
                cluster.ready_chunk_kv = Mock()
                with patch.object(ChunkKvCluster, "control", return_value={"ok": True}):
                    cluster.control({"action": "start", "node": 11})
                cluster.ready_chunk_kv.assert_called_once_with(recovering=True)
