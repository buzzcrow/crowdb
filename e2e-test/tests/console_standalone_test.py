# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from allocation import LABEL
from console_standalone_fixture import StandaloneWebCluster


class StandaloneControlTest(unittest.TestCase):
    def cluster(self, directory, owner="owned"):
        cluster = object.__new__(StandaloneWebCluster)
        cluster.run_id = "owned"
        cluster.servers = ["owned-node0"]
        cluster.artifacts = Path(directory)
        cluster.command = Mock(return_value=json.dumps([{
            "Config": {"Labels": {LABEL: owner}}, "State": {"Running": True}}]))
        return cluster

    def test_file_transfer_rejects_foreign_ownership_before_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.cluster(directory, owner="foreign")
            for action in ("export-standalone-config", "seal-bootstrap-intent", "export-chunkdb-launch", "inspect-retained-chunkdb-launch"):
                with self.assertRaisesRegex(ValueError, "ownership"):
                    cluster.control({"action": action, "node": 0})
            self.assertEqual(cluster.command.call_args_list, [
                unittest.mock.call("inspect", "owned-node0"),
                unittest.mock.call("inspect", "owned-node0"),
                unittest.mock.call("inspect", "owned-node0"),
                unittest.mock.call("inspect", "owned-node0")])

    def test_file_transfer_rejects_invalid_node_before_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.cluster(directory)
            for node in (-1, 1, True, "0"):
                with self.assertRaisesRegex(ValueError, "outside"):
                    cluster.control({"action": "export-standalone-config", "node": node})
            cluster.command.assert_not_called()

    def test_seal_rejects_missing_or_oversized_intent_before_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = self.cluster(directory)
            with self.assertRaisesRegex(ValueError, "missing or oversized"):
                cluster.control({"action": "seal-bootstrap-intent", "node": 0})
            (Path(directory) / "bootstrap-intent.toml").write_bytes(b"x" * 65537)
            with self.assertRaisesRegex(ValueError, "missing or oversized"):
                cluster.control({"action": "seal-bootstrap-intent", "node": 0})
            self.assertTrue(all(call.args[0] == "inspect" for call in cluster.command.call_args_list))

    def test_chunkdb_export_rejects_unowned_paths_and_invalid_pids(self):
        with tempfile.TemporaryDirectory() as directory:
            for workdir, pid in (("/etc", 22), ("/opt/crowdb/data/../etc", 22),
                                 ("/opt/crowdb/data/service", 1), ("/opt/crowdb/data/service", True)):
                cluster = self.cluster(directory)
                (Path(directory) / "standalone-config.json").write_text(json.dumps({
                    "local_launches": {"chunkdb-901": {"workdir": workdir}},
                    "server": [{"id": "chunkdb-901", "pid": pid}]}))
                with self.assertRaisesRegex(ValueError, "workdir outside|invalid Chunkdb"):
                    cluster.control({"action": "export-chunkdb-launch", "node": 0})
                self.assertFalse(any("chunkdb-launch.toml" in str(call)
                                     for call in cluster.command.call_args_list))
