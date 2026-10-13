# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from control import Control
from fixture import Cluster, LABEL
from chunkdb_fixture import ChunkdbStorageCluster
from console_fixture import ConsoleCluster


class ControlTest(unittest.TestCase):
    def test_actions_validate_identity_and_never_target_other_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.servers = ["owned"]
            cluster.command = Mock(return_value=json.dumps([{
                "Config": {"Labels": {LABEL: "other-run"}}, "State": {"Running": True}}]))
            for body in ({"action": "delete", "node": 0}, {"action": "stop", "node": -1},
                         {"action": "stop", "node": True}, {"action": "stop", "node": 1}):
                with self.assertRaises(ValueError):
                    cluster.control(body)
            cluster.command.assert_not_called()
            with self.assertRaisesRegex(ValueError, "ownership"):
                cluster.control({"action": "stop", "node": 0})
            cluster.command.assert_called_once_with("inspect", "owned")

    def test_status_is_read_only_and_requires_owned_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.servers = ["owned"]
            cluster.command = Mock(return_value=json.dumps([{
                "Config": {"Labels": {LABEL: cluster.run_id}}, "State": {"Running": True}}]))
            self.assertEqual(cluster.control({"action": "status", "node": 0}), {"Running": True})
            cluster.command.assert_called_once_with("inspect", "owned")

    def test_storage_reconfiguration_rejects_invalid_options_before_lifecycle_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = ChunkdbStorageCluster("image", directory, "client-bundle")
            cluster.command = Mock()
            for options in ({}, {**cluster.options, "repair_enabled": 1},
                            {**cluster.options, "conversion_task_lease_secs": 0},
                            {**cluster.options, "repair_allow_unsafe_placement": True}):
                with self.assertRaises(ValueError):
                    cluster.control({"action": "configure-chunkdb", "node": 4, "options": options})
            cluster.command.assert_not_called()

    def test_failed_actions_are_reported_and_completed_requests_are_not_replayed(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            cluster.control = Mock(side_effect=ValueError("invalid target"))
            control = Control(cluster)
            request = control.directory / "one.request.json"
            request.write_text(json.dumps({"action": "stop", "node": 99}))
            response = control.directory / "one.response.json"
            with control:
                deadline = time.monotonic() + 2
                while not response.exists():
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
            outcome = json.loads(response.read_text())
            self.assertFalse(outcome["ok"])
            self.assertEqual(outcome["error"], "invalid target")
            self.assertGreaterEqual(outcome["seconds"], 0)
            with Control(cluster):
                time.sleep(0.05)
            cluster.control.assert_called_once()


class ConsoleControlTest(unittest.TestCase):
    def test_signals_require_owned_node_observed_pid_and_supported_service(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = ConsoleCluster("image", directory, Path(directory))
            cluster.servers = ["owned"]
            inspected = [{"Config": {"Labels": {LABEL: cluster.run_id}}, "State": {"Running": True}}]
            cluster.command = Mock(return_value=json.dumps(inspected))
            cluster.observation = Mock(return_value={"services": {"kv": {"pid": 42}}})
            body = {"action": "signal-service", "node": 0, "service": "kv", "pid": 42, "signal": "STOP"}
            for field, value in (("service", "web"), ("pid", 43), ("pid", True), ("signal", "KILL"), ("node", 1)):
                cluster.command.reset_mock()
                with self.assertRaises(ValueError):
                    cluster.control(dict(body, **{field: value}))
                self.assertTrue(all(call.args[0] == "inspect" for call in cluster.command.call_args_list))
            cluster.command.reset_mock()
            self.assertEqual(cluster.control(body), {"pid": 42, "signal": "STOP"})
            cluster.command.assert_any_call("exec", "owned", "/bin/kill", "-STOP", "42")
            inspected[0]["Config"]["Labels"][LABEL] = "foreign"
            cluster.command.return_value = json.dumps(inspected)
            cluster.command.reset_mock()
            with self.assertRaisesRegex(ValueError, "ownership"):
                cluster.control(body)
            cluster.command.assert_called_once_with("inspect", "owned")
