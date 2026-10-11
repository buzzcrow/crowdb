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
