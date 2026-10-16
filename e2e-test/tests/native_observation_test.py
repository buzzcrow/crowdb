# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from allocation import LABEL
from console_native_observation import services, process, retained_marker, disk_backing, BrowserObservations
from console_native_fixture import NativeConsoleCluster
from console_native_storage import prepared_file_observation


class NativeObservationTest(unittest.TestCase):
    def test_file_preparation_failure_is_reported_without_poll_timeout(self):
        cluster = SimpleNamespace(servers=["owned-node"], command=Mock(return_value=json.dumps({
            "phase": "failed", "error": "insufficient backing space including host reserve"})))
        with self.assertRaisesRegex(AssertionError, "insufficient backing space"):
            prepared_file_observation(cluster, "operation")
        self.assertEqual(cluster.command.call_count, 1)

    def test_bootstrap_observation_reports_monitor_failure_without_waiting(self):
        cluster = NativeConsoleCluster.__new__(NativeConsoleCluster)
        cluster.api = Mock(return_value={"phase": "failed", "error": "peer host key rejected"})
        with self.assertRaisesRegex(AssertionError, "peer host key rejected"):
            cluster.initialization_status()
        cluster.api.assert_called_once_with("/api/cluster/initialization")

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

    def test_owned_native_backing_identity_requires_a_regular_file_with_original_capacity(self):
        cluster = self.cluster()
        cluster.disk_gib = (8,)
        inspection = cluster.command.return_value
        cluster.command.side_effect = [inspection, "81a4:20:30:8589934592\n"]
        self.assertEqual(disk_backing(cluster, 0), {"path": "/opt/crowdb/data/disks/disk-1.img",
                         "backing_identity": "file:20:30", "capacity_bytes": 8 * 1024 ** 3})
        cluster.command.assert_called_with("exec", "--user", "crowdb", "immutable", "stat",
                                           "--format=%f:%d:%i:%s", "--", "/opt/crowdb/data/disks/disk-1.img")
        for observation in ("a1ff:20:30:8589934592", "81a4:20:30:8589934593", "81a4:20:0:8589934592"):
            cluster.command.side_effect = [inspection, observation]
            with self.subTest(observation=observation), self.assertRaises(ValueError):
                disk_backing(cluster, 0)

    def test_foreign_native_backing_rejects_before_file_inspection(self):
        cluster = self.cluster(owner="foreign")
        with self.assertRaisesRegex(ValueError, "ownership"):
            disk_backing(cluster, 0)
        self.assertEqual(cluster.command.call_count, 1)

    def test_spare_backing_uses_only_the_owned_first_node_and_exact_size(self):
        cluster = self.cluster()
        cluster.disk_gib = (8,)
        cluster.spare_disk_gib = 4096
        inspection = cluster.command.return_value
        cluster.command.side_effect = [inspection, f"81a4:20:31:{4096 * 1024 ** 3}"]
        actual = disk_backing(cluster, 0, spare=True)
        self.assertEqual(actual["path"], "/opt/crowdb/data/disks/capacity-spare.img")
        self.assertEqual(actual["backing_identity"], "file:20:31")
        cluster.spare_disk_gib = 0
        cluster.command.side_effect = [inspection]
        with self.assertRaisesRegex(ValueError, "owned first-node"):
            disk_backing(cluster, 0, spare=True)

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

    def test_retained_marker_requires_owned_file_and_aligned_range(self):
        path = "/opt/crowdb/data/disks/managed-12345678-1234-4234-8234-123456789abc.img"
        cluster = self.cluster()
        inspect = cluster.command.return_value
        cluster.command.side_effect = [inspect, "Z" * 4096]
        self.assertEqual(retained_marker(cluster, 0, path, 4096), {"matches": True, "length": 4096})
        cluster.command.assert_called_with("exec", "immutable", "dd", f"if={path}", "iflag=nofollow",
                                           "bs=4096", "skip=1", "count=1", "status=none")
        for invalid in ("/etc/passwd", "/opt/crowdb/data/disks/../secret", None):
            cluster = self.cluster()
            with self.assertRaisesRegex(ValueError, "generated file"):
                retained_marker(cluster, 0, invalid, 0)
            cluster.command.assert_not_called()
        for invalid in (-1, 1, True, 1 << 63):
            cluster = self.cluster()
            with self.assertRaisesRegex(ValueError, "aligned bounded"):
                retained_marker(cluster, 0, path, invalid)
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
