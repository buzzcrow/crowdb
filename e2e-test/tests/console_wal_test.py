# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import tempfile
import unittest
from unittest.mock import Mock

from allocation import LABEL
from console_wal_fixture import WalWebCluster


class WalControlTest(unittest.TestCase):
    def test_batch_lifecycle_validates_every_voter_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = WalWebCluster("image", directory, 3)
            cluster.servers = ["kv1", "kv2", "kv3", "web"]
            def inspect(operation, name):
                self.assertEqual(operation, "inspect")
                return json.dumps([{"Config": {"Labels": {
                    LABEL: "foreign" if name == "kv3" else cluster.run_id}},
                    "State": {"Running": True}}])
            cluster.command = Mock(side_effect=inspect)
            for action in ("stop-voters", "start-voters"):
                with self.assertRaisesRegex(ValueError, "ownership"):
                    cluster.control({"action": action, "node": 0})
            self.assertTrue(all(call.args[0] == "inspect" for call in cluster.command.call_args_list))
            for node in (True, -1, 1, 3):
                cluster.command.reset_mock()
                with self.assertRaises(ValueError):
                    cluster.control({"action": "stop-voters", "node": node})
                cluster.command.assert_not_called()
