# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from allocation import LABEL
from console_native_observation import services, process, BrowserObservations
from console_native_fixture import NativeConsoleCluster


class NativeObservationTest(unittest.TestCase):
    def test_invalid_capacity_profile_rejects_before_allocating(self):
        for sizes in ((80, 8), (80, 8, 0), (80, 8, True), (80, 8, 4097), (8,) * 9):
            with self.assertRaisesRegex(ValueError, "three to eight bounded"):
                NativeConsoleCluster("image", Path("/unused"), Path("/unused"), disk_gib=sizes)
        for zones in ((1, 1), (1, 1, 0), (1, 1, True), (1, 1, 9)):
            with self.assertRaisesRegex(ValueError, "zone sizes"):
                NativeConsoleCluster("image", Path("/unused"), Path("/unused"), zone_gib=zones)
        for spare in (-1, True, 4097):
            with self.assertRaisesRegex(ValueError, "spare disk"):
                NativeConsoleCluster("image", Path("/unused"), Path("/unused"), spare_disk_gib=spare)

    def cluster(self, owner="owned", image="image", mount="/owned/data"):
        info = {"Id": "immutable", "Image": image, "Config": {"Labels": {LABEL: owner}},
                "Mounts": [{"Source": mount, "Destination": "/opt/crowdb/data"}]}
        return SimpleNamespace(run_id="owned", image="image", servers=["node"],
            data_paths=[Path("/owned/data")], command=Mock(return_value=json.dumps([info])))

    def test_foreign_owner_image_or_mount_rejects_before_namespace_read(self):
        for kwargs in ({"owner": "foreign"}, {"image": "foreign"}, {"mount": "/foreign"}):
            cluster = self.cluster(**kwargs)
            with self.assertRaisesRegex(ValueError, "ownership"):
                services(cluster, 0)
            self.assertEqual(len(cluster.command.call_args_list), 1)
            self.assertEqual(cluster.command.call_args.args[0], "inspect")

    def test_invalid_node_rejects_before_runtime_access(self):
        for node in (-1, 1, True, "0"):
            cluster = self.cluster()
            with self.assertRaisesRegex(ValueError, "outside"):
                services(cluster, node)
            cluster.command.assert_not_called()

    def test_unobserved_pid_cannot_read_an_unrelated_process(self):
        cluster = self.cluster()
        with self.assertRaisesRegex(ValueError, "not observed"):
            process(cluster, 0, 999)
        self.assertEqual(len(cluster.command.call_args_list), 1)
        for pid in (-1, 1, True, "2", 0x100000000):
            cluster.command.reset_mock()
            with self.assertRaisesRegex(ValueError, "invalid"):
                process(cluster, 0, pid)
            cluster.command.assert_not_called()

    def test_browser_dispatch_requires_its_recorded_case(self):
        cluster = NativeConsoleCluster.__new__(NativeConsoleCluster)
        cluster.case_name = "console::native_binding::repair"
        cluster.browser = Mock()
        with self.assertRaisesRegex(ValueError, "not owned"):
            cluster.control({"action": "run-native-browser"})
        cluster.browser.assert_not_called()

    def test_browser_observation_queue_rejects_mutating_actions(self):
        proxy = BrowserObservations.__new__(BrowserObservations)
        proxy.cluster = self.cluster()
        for action in ("kill", "stop", "restart", "delete", "run-native-browser"):
            with self.assertRaisesRegex(ValueError, "read-only"):
                proxy.control({"action": action, "node": 0})
        proxy.cluster.command.assert_not_called()
