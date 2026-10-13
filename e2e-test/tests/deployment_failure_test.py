# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import unittest
from unittest.mock import Mock

from deployment_failure_fixture import DeploymentFailureCluster
from fixture import LABEL


class DeploymentFailureTest(unittest.TestCase):
    def test_foreign_container_cannot_have_credentials_changed_or_restart_budget_consumed(self):
        for action in ("prepare-lifecycle", "exhaust-restarts", "reject-changed-credentials"):
            cluster = object.__new__(DeploymentFailureCluster)
            cluster.services, cluster.servers, cluster.run_id = ("web",), ["node"], "owned"
            cluster.command = Mock(return_value=json.dumps([{
                "Config": {"Labels": {LABEL: "foreign"}}, "State": {"Running": True}}]))
            with self.assertRaisesRegex(ValueError, "ownership"):
                cluster.control({"action": action, "node": 0})
            cluster.command.assert_called_once_with("inspect", "node")
