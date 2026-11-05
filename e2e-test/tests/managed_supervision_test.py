# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import unittest
from unittest.mock import Mock

from fixture import LABEL
from managed_fixture import ManagedCluster


class ManagedSupervisionTest(unittest.TestCase):
    def fixture(self, label="owned", pid=27, executable="/opt/crowdb/bin/crowdb-paxos-kv-server"):
        cluster = object.__new__(ManagedCluster)
        cluster.services, cluster.servers, cluster.run_id = ("kv",), ["node"], "owned"
        cluster.command = Mock(side_effect=[json.dumps([{
            "Config": {"Labels": {LABEL: label}}, "State": {"Running": True}}]), executable])
        cluster.monitor_status = Mock(return_value={"services": {
            "kv": {"pid": pid, "generation": 1}}})
        return cluster

    def request(self, **changes):
        return {"action": "recover-service", "node": 0, "service": "kv", "signal": "KILL", **changes}

    def test_invalid_service_node_and_signal_never_touch_docker(self):
        for changes in ({"service": "other"}, {"node": 1}, {"signal": "TERM"}):
            cluster = self.fixture()
            with self.assertRaises(ValueError):
                cluster.control(self.request(**changes))
            cluster.command.assert_not_called()

    def test_changed_ownership_cannot_signal_a_service(self):
        cluster = self.fixture(label="another-run")
        with self.assertRaisesRegex(ValueError, "ownership"):
            cluster.control(self.request())
        cluster.command.assert_called_once_with("inspect", "node")

    def test_monitor_pid_and_reused_pid_executable_are_never_signalled(self):
        for cluster in (self.fixture(pid=1), self.fixture(executable="/usr/bin/sleep")):
            with self.assertRaises(ValueError):
                cluster.control(self.request())
            self.assertFalse(any(call.args[0] == "exec" and "kill" in call.args
                                 for call in cluster.command.call_args_list))
