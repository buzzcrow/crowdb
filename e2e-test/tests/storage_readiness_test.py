# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Readiness must distinguish retained registrations from the current boot."""

import tempfile
import unittest
from unittest.mock import patch

from chunkdb_fixture import ChunkdbCluster, ChunkdbStorageCluster


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
