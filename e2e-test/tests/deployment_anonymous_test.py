# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import unittest
from unittest.mock import Mock

from deployment_anonymous_fixture import AnonymousReleaseCluster
from fixture import LABEL


class AnonymousCleanupTest(unittest.TestCase):
    def test_changed_ownership_prevents_volume_removal(self):
        cluster = object.__new__(AnonymousReleaseCluster)
        cluster.servers, cluster.run_id, cluster.closed = ["node"], "owned", False
        cluster.command = Mock(side_effect=["node", json.dumps([{
            "Config": {"Labels": {LABEL: "foreign"}}}])])
        with self.assertRaisesRegex(ValueError, "ownership"):
            cluster.close()
        self.assertFalse(any("rm" in call.args or "cp" in call.args
                             for call in cluster.command.call_args_list))
