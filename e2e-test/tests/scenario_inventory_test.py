# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Every migrated object/stream case must be reachable through the runner."""

import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scenarios import Case, CHUNK_CLIENT_CASES, CHUNK_STREAM_CASES, run_cases


class ScenarioInventoryTest(unittest.TestCase):
    def test_object_and_stream_cases_have_exactly_one_runner_owner(self):
        root = Path(__file__).resolve().parent
        paths = [root / "chunk_stream.rs", root / "chunk_client.rs"]
        paths.extend((root / "chunk_client").rglob("*.rs"))
        declared = set()
        for path in paths:
            module = "::".join(path.relative_to(root).with_suffix("").parts)
            for name in re.findall(
                    r'#\[ignore\s*=\s*"[^"]*"\]\s*(?:#\[[^\]]*\]\s*)*async fn (\w+)\(',
                    path.read_text()):
                declared.add(f"{module}::{name}")
        registered = [case.name for case in (*CHUNK_CLIENT_CASES, *CHUNK_STREAM_CASES)]
        self.assertEqual(len(registered), len(set(registered)), "duplicate fixture execution")
        self.assertEqual(declared, set(registered), "unregistered or stale migrated case")

    def test_failed_case_does_not_cancel_other_independent_fixtures(self):
        executed = []
        closed = []
        class TestCluster:
            def __init__(self, root, name):
                self.name = name
                self.artifacts = root / name
                self.artifacts.mkdir()

            def __enter__(self):
                return self

            def __exit__(self, *_):
                closed.append(self.name)

            def client(self, _bundle, _phase, name):
                executed.append(name)
                if name == "failed":
                    raise RuntimeError("original client assertion")

        cases = tuple(Case("owned.test", name) for name in ("failed", "second", "third"))
        with tempfile.TemporaryDirectory() as root:
            with patch("scenarios.ALL_CASES", cases), patch("scenarios.allocate_cluster",
                    side_effect=lambda case, *_: TestCluster(Path(root), case.name)):
                with self.assertRaisesRegex(RuntimeError, "original client assertion"):
                    run_cases("image", Path(root), "bundle", 1)
        self.assertEqual(executed, [case.name for case in cases])
        self.assertEqual(closed, executed)
