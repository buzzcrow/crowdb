# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from deployment_clients_fixture import DeploymentClientsCluster
from managed_fixture import ManagedCluster


class DeploymentClientsTest(unittest.TestCase):
    def test_runtime_observation_uses_reported_monitor_pid_below_container_init(self):
        cluster = object.__new__(DeploymentClientsCluster)
        cluster.servers = ["node"]
        cluster.monitor_status = Mock(return_value={
            "monitor_pid": 27, "services": {"kv": {"pid": 42}}})

        def command(*args):
            if args[0] == "inspect":
                return json.dumps([{"HostConfig": {"PortBindings": {}}}])
            if "stat" in args:
                return "700" if args[-1].endswith("crash") else "600"
            if args[-1] in ("/proc/27/cwd", "/proc/42/cwd"):
                return "/opt/crowdb/data/crash"
            raise AssertionError(f"unexpected runtime observation: {args}")

        cluster.command = Mock(side_effect=command)
        with tempfile.TemporaryDirectory() as directory:
            cluster.artifacts = Path(directory)
            with patch.object(ManagedCluster, "ready"):
                cluster.ready()
            runtime = json.loads((cluster.artifacts / "deployment-runtime.json").read_text())
        self.assertEqual(runtime["cwd"], {
            "monitor": "/opt/crowdb/data/crash", "kv": "/opt/crowdb/data/crash"})
        cluster.command.assert_any_call(
            "exec", "--user", "crowdb", "node", "readlink", "/proc/27/cwd")
