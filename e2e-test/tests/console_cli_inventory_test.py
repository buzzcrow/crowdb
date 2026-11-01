# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import copy
import json
import unittest
from unittest.mock import Mock, patch

from console_cli_target import CliAttachedCluster
from fixture import LABEL


class AttachedInventoryTest(unittest.TestCase):
    def setUp(self):
        self.cluster = object.__new__(CliAttachedCluster)
        self.cluster.servers = ["owned-1", "owned-2", "owned-3"]
        self.cluster.addresses = ["192.0.2.1", "192.0.2.2", "192.0.2.3"]
        self.cluster.run_id = "fixture"
        self.cluster.cluster_id = "cluster"
        self.cluster.image = "image"
        self.exited = set()
        self.containers = {name: {"Id": name, "Image": "image", "State": {"Running": True},
            "Config": {"Labels": {LABEL: "fixture"}}} for name in self.cluster.servers}
        self.status, self.services = {}, []
        kinds = ("paxos-kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access-server")
        for node, name in enumerate(self.cluster.servers, 1):
            processes = {"web": {"pid": 20, "generation": 1, "operation_id": None}}
            for number, kind in enumerate(kinds, 1):
                identity = f"{kind}-{node}"
                processes["kv" if kind == "paxos-kv" else identity] = {
                    "pid": 100 + number, "generation": 1, "operation_id": f"operation-{node}-{number}"}
                self.services.append({"id": identity, "node_id": node, "service_type": kind,
                    "endpoint": f"192.0.2.{node}:{10000 + number}", "health": "unknown", "start_pending": False})
            self.status[name] = {"services": processes}
        self.cluster.command = Mock(side_effect=self.command)
        self.request = patch("console_cli_target.request", side_effect=lambda *_: copy.deepcopy(self.services)).start()
        self.addCleanup(patch.stopall)

    def command(self, action, name, *arguments):
        if action == "inspect":
            return json.dumps([self.containers[name]])
        if action == "exec" and arguments[0] == "sh":
            self.assertEqual(arguments[3], "owned-service-pids")
            if any((name, int(pid)) in self.exited for pid in arguments[4:]):
                raise RuntimeError("owned service PID is not live")
            return ""
        self.assertEqual((action, arguments[0]), ("exec", "cat"))
        if arguments[1] == "/opt/crowdb/data/node-binding.json":
            return json.dumps({"node_id": self.cluster.servers.index(name) + 1,
                               "bootstrap": {"cluster_id": "cluster"}})
        self.assertEqual(arguments[1], "/opt/crowdb/run/status/monitor.json")
        return json.dumps(self.status[name])

    def test_cold_cache_does_not_hide_process_or_registration_changes(self):
        before = self.cluster.inventory()
        self.assertEqual(len(before["processes"]), 21)
        self.services[0].update(pid=101, pid_node_id=1, health="up")
        self.assertEqual(self.cluster.inventory(), before)
        self.status["owned-1"]["services"]["chunk-kv-1"]["pid"] += 1
        self.assertNotEqual(self.cluster.inventory(), before)
        self.status["owned-1"]["services"]["chunk-kv-1"]["pid"] -= 1
        self.services[0]["endpoint"] = "192.0.2.1:29999"
        self.assertNotEqual(self.cluster.inventory(), before)

    def test_foreign_container_is_rejected_before_monitor_access(self):
        self.containers["owned-1"]["Config"]["Labels"][LABEL] = "foreign"
        with self.assertRaisesRegex(AssertionError, "ownership"):
            self.cluster.inventory()
        self.assertFalse(any(call.args[0] == "exec" for call in self.cluster.command.call_args_list))

    def test_invalid_real_pid_and_wrong_cached_namespace_are_rejected(self):
        self.status["owned-1"]["services"]["web"]["pid"] = None
        with self.assertRaisesRegex(AssertionError, "PID"):
            self.cluster.inventory()
        self.status["owned-1"]["services"]["web"]["pid"] = 20
        self.services[0].update(pid=101, pid_node_id=2)
        with self.assertRaisesRegex(AssertionError, "namespace"):
            self.cluster.inventory()

    def test_old_positive_status_does_not_hide_process_exit_or_missing_web(self):
        self.exited.add(("owned-1", 20))
        with self.assertRaisesRegex(RuntimeError, "not live"):
            self.cluster.inventory()
        self.exited.clear()
        del self.status["owned-1"]["services"]["web"]
        with self.assertRaisesRegex(AssertionError, "service is absent"):
            self.cluster.inventory()


if __name__ == "__main__":
    unittest.main()
