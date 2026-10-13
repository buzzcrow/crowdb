# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mixed_isolation import run_mixed


class MixedIsolationTest(unittest.TestCase):
    def test_failed_peer_wakes_survivor_and_both_owned_fixtures_close(self):
        closed = []

        class TestCluster:
            def __init__(self, image, artifacts, **kwargs):
                self.artifacts = Path(artifacts) / self.__class__.__name__
                self.artifacts.mkdir()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                closed.append(self.__class__.__name__)

            def client(self, bundle, phase, case):
                pass

            def crash_restart(self):
                raise RuntimeError("owned KV restart failed")

        class TestWeb(TestCluster):
            pass

        with tempfile.TemporaryDirectory() as directory:
            with patch("mixed_isolation.Cluster", TestCluster), patch("mixed_isolation.StandaloneWebCluster", TestWeb):
                with self.assertRaisesRegex(RuntimeError, "owned KV restart failed"):
                    run_mixed("release", directory, Path(directory))
        self.assertCountEqual(closed, ["TestCluster", "TestWeb"])
