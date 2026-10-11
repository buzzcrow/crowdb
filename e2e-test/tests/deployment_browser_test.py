# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import unittest
from unittest.mock import Mock

from deployment_browser_fixture import DeploymentBrowserCluster
from fixture import LABEL


class DeploymentBrowserControlTest(unittest.TestCase):
    def fixture(self, label="owned", pid=27):
        cluster = object.__new__(DeploymentBrowserCluster)
        cluster.servers, cluster.run_id = ["node"], "owned"
        cluster.command = Mock(return_value=json.dumps([{
            "Config": {"Labels": {LABEL: label}}, "State": {"Running": True}}]))
        cluster.monitor_status = Mock(return_value={"services": {"kv": {"pid": pid}}})
        return cluster

    def test_foreign_container_and_stale_pid_never_receive_browser_signals(self):
        for cluster in (self.fixture(label="foreign"), self.fixture(pid=28), self.fixture(pid=1)):
            with self.assertRaises(ValueError):
                cluster.control({"action": "signal-kv", "node": 0, "signal": "STOP", "pid": 27})
            self.assertFalse(any("kill" in call.args for call in cluster.command.call_args_list))
