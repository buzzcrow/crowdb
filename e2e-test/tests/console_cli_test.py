# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from console_cli_fixture import CliContainerCluster, LABEL, ROOT_LABEL


class CliOwnershipTest(unittest.TestCase):
    def test_recovery_observation_rejects_foreign_owner_before_file_access(self):
        cluster = object.__new__(CliContainerCluster)
        cluster.servers = ["owned"]
        cluster.root = Path("/owned/root")
        cluster.image = "immutable-image"
        cluster.state = lambda: {"id": "expected-owner"}
        cluster.command = Mock(return_value=json.dumps([{
            "Image": cluster.image,
            "Config": {"Labels": {LABEL: "foreign", ROOT_LABEL: str(cluster.root)}}}]))
        with self.assertRaisesRegex(ValueError, "ownership"):
            cluster.read_owned("node-binding.json")
        cluster.command.assert_called_once_with("inspect", "owned")

    def test_recovery_observation_rejects_paths_before_runtime_access(self):
        cluster = object.__new__(CliContainerCluster)
        cluster.command = Mock()
        for path in ("../credentials", "secrets/server.env", "/etc/passwd"):
            with self.assertRaisesRegex(ValueError, "unsupported"):
                cluster.read_owned(path)
        cluster.command.assert_not_called()

    def test_cleanup_rechecks_listed_ownership_before_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = CliContainerCluster("image", Path(directory), Path(directory))
            cluster.start()
            (cluster.root / "container-cluster.json").write_text(json.dumps({"id": "owner"}))
            cluster.command = Mock(side_effect=["unexpected-member", json.dumps([{
                "Config": {"Labels": {LABEL: "foreign", ROOT_LABEL: str(cluster.root)}}}])])
            with self.assertRaisesRegex(AssertionError, "ownership"):
                cluster.close()
            self.assertFalse(any("rm" in call.args for call in cluster.command.call_args_list))

    def test_recovery_observation_rejects_invalid_node_before_inspection(self):
        cluster = object.__new__(CliContainerCluster)
        cluster.servers = ["owned"]
        cluster.command = Mock()
        for index in (-1, 1, True, "0"):
            with self.assertRaisesRegex(ValueError, "outside"):
                cluster.read_owned("node-binding.json", index)
        cluster.command.assert_not_called()
