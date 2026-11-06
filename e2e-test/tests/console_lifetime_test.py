# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import tempfile
import unittest
from unittest.mock import Mock
from console_lifetime_fixture import DisposableWebCluster
from allocation import LABEL


class DisposableOwnershipTest(unittest.TestCase):
    def test_cleanup_refuses_reused_pid_or_lost_owner_before_signaling(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = DisposableWebCluster("image", directory)
            cluster.servers = ["owned"]
            cluster.child_pid, cluster.child_start = 42, "original-start"
            cluster.command = Mock(return_value=json.dumps([{
                "Config": {"Labels": {LABEL: cluster.run_id}}, "State": {"Running": True}}]))
            cluster.identity = Mock(return_value="reused-start")
            with self.assertRaisesRegex(ValueError, "identity changed"):
                cluster.control({"action":"cleanup-disposable-runtime", "node":0})
            cluster.command.assert_called_once_with("inspect", "owned")
            cluster.command.reset_mock()
            cluster.identity.return_value = "original-start"
            cluster.manifests = Mock(return_value=[{"processes":[{"pid":42,"start":"stale"}]}])
            with self.assertRaisesRegex(ValueError, "namespace record"):
                cluster.control({"action":"cleanup-disposable-runtime", "node":0})
            cluster.command.assert_called_once_with("inspect", "owned")

    def test_record_rejects_non_process_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = DisposableWebCluster("image", directory)
            cluster.command = Mock()
            for pid in (True, None, 0, 1, -1, "42"):
                with self.assertRaises(ValueError):
                    cluster.identity(pid, "crowdb-paxos-kv-server")
            cluster.command.assert_not_called()
