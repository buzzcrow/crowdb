# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import tempfile
import unittest

from control import Control
from fixture import Cluster


class ControlLifetimeTest(unittest.TestCase):
    def test_completed_client_does_not_cancel_between_client_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster("image", directory)
            with Control(cluster) as first:
                self.assertFalse(cluster.control_cancelled.is_set())
            self.assertTrue(first.done.is_set())
            self.assertFalse(hasattr(cluster, "control_cancelled"))
            with Control(cluster) as second:
                self.assertIs(cluster.control_cancelled, second.done)
                self.assertFalse(cluster.control_cancelled.is_set())
            self.assertFalse(hasattr(cluster, "control_cancelled"))
