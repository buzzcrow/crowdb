# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import tempfile
import unittest
from unittest.mock import Mock

from allocation import LABEL
from console_bootstrap_fixture import BootstrapWebCluster


class BootstrapSourceTest(unittest.TestCase):
    def test_upload_rejects_wrong_node_and_foreign_owner_before_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = BootstrapWebCluster("image", directory)
            cluster.servers = ["kv", "web"]
            cluster.command = Mock(return_value=json.dumps([{
                "Config": {"Labels": {LABEL: "foreign"}}, "State": {"Running": True}}]))
            for node in (True, -1, 0, 2, "1"):
                with self.assertRaises(ValueError):
                    cluster.control({"action": "upload-bootstrap-source", "node": node})
            cluster.command.assert_not_called()
            with self.assertRaisesRegex(ValueError, "ownership"):
                cluster.control({"action": "upload-bootstrap-source", "node": 1})
            cluster.command.assert_called_once_with("inspect", "web")

    def test_upload_rejects_missing_symlinked_or_oversized_source_before_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = BootstrapWebCluster("image", directory)
            cluster.servers = ["kv", "web"]
            cluster.command = Mock(return_value=json.dumps([{
                "Config": {"Labels": {LABEL: cluster.run_id}}, "State": {"Running": True}}]))
            source = cluster.artifacts / "bootstrap-source.toml"
            for kind in ("missing", "symlink", "oversized"):
                if kind == "symlink":
                    source.symlink_to(cluster.artifacts / "foreign")
                elif kind == "oversized":
                    source.unlink()
                    source.write_bytes(b"x" * 65537)
                cluster.command.reset_mock()
                with self.assertRaises(ValueError):
                    cluster.control({"action": "upload-bootstrap-source", "node": 1})
                cluster.command.assert_called_once_with("inspect", "web")
