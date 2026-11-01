# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Control-protocol regressions only; no SDK, endpoint, process or container."""

import ast
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import tempfile
import threading
import types
import unittest
from unittest.mock import patch


def pressure_module():
    source = Path(os.environ.get("CROWDB_GC_PRESSURE_TEST_SOURCE", Path(__file__).parent /
                  "access_iceberg/gc/sdk_pressure.py"))
    tree = ast.parse(source.read_text(), filename=str(source))
    # Load the real control functions without importing or executing an SDK.
    declarations = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                    or isinstance(node, (ast.Import, ast.ImportFrom)) and not (
                        isinstance(node, ast.ImportFrom) and node.module.startswith("pyiceberg")
                        or isinstance(node, ast.Import) and any(alias.name == "requests" for alias in node.names))]
    module = types.ModuleType("gc_sdk_pressure")
    exec(compile(ast.Module(body=declarations, type_ignores=[]), str(source), "exec"), module.__dict__)
    module.control = None
    return module


class GcSdkPressureControlTest(unittest.TestCase):
    def setUp(self):
        self.pressure = pressure_module()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_all_four_workers_park_without_doing_post_ready_work_until_release(self):
        parked = threading.Barrier(5)
        release_check = threading.Event()
        completed = []

        def observed_sleep(_seconds):
            parked.wait(timeout=2)
            if not release_check.wait(timeout=2):
                raise TimeoutError("test did not release parked control workers")

        def worker(index):
            self.pressure.wait_for_release(self.root, index)
            completed.append(index)

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            try:
                with patch.object(self.pressure.time, "sleep", side_effect=observed_sleep):
                    futures = [executor.submit(worker, index) for index in range(4)]
                    parked.wait(timeout=2)
                    self.assertEqual(sorted(path.name for path in self.root.glob("ready-*")),
                                     ["ready-0", "ready-1", "ready-2", "ready-3"])
                    self.assertFalse((self.root / "run").exists())
                    self.assertEqual(completed, [])
                    (self.root / "run").touch()
                    release_check.set()
                    for future in futures:
                        future.result(timeout=2)
            finally:
                (self.root / "stop").touch()
                release_check.set()
        self.assertEqual(sorted(completed), [0, 1, 2, 3])

    def test_stop_prevents_release_even_if_run_marker_also_exists(self):
        (self.root / "stop").touch()
        (self.root / "run").touch()
        with self.assertRaisesRegex(RuntimeError, "cancelled before release"):
            self.pressure.wait_for_release(self.root, 1)

    def test_original_worker_error_is_published_and_unblocks_parked_peer(self):
        self.pressure.control = self.root
        with patch.object(self.pressure, "run", side_effect=ValueError("original catalog error")):
            with self.assertRaisesRegex(ValueError, "original catalog error"):
                self.pressure.guarded_run(2)
        failure = (self.root / "failed-2").read_text()
        self.assertIn("ValueError: original catalog error", failure)
        self.assertIn("Traceback", failure)
        with self.assertRaisesRegex(RuntimeError, "foreground peer failed:.*"):
            self.pressure.wait_for_release(self.root, 0)

    def test_failure_prevents_release_even_when_run_marker_arrives(self):
        (self.root / "failed-3").write_text("original SDK HTTP failure")
        (self.root / "run").touch()
        with self.assertRaisesRegex(RuntimeError, "original SDK HTTP failure"):
            self.pressure.wait_for_release(self.root, 2)

    def test_failure_is_not_visible_until_the_complete_traceback_is_published(self):
        self.pressure.control = self.root
        real_write = Path.write_text

        def interrupted_write(path, text):
            real_write(path, text[:len(text) // 2])
            self.assertEqual(list(self.root.glob("failed-*")), [])
            return real_write(path, text)

        with patch.object(Path, "write_text", new=interrupted_write), \
                patch.object(self.pressure, "run", side_effect=ValueError("whole original error")):
            with self.assertRaisesRegex(ValueError, "whole original error"):
                self.pressure.guarded_run(1)
        self.assertTrue((self.root / "failed-1").read_text().endswith("ValueError: whole original error\n"))

    def test_peer_cancellation_keeps_the_original_failure_record(self):
        self.pressure.control = self.root
        original = "ValueError: actual SDK request failed"
        (self.root / "failed-3").write_text(original)
        with patch.object(self.pressure, "run", side_effect=RuntimeError("foreground peer failed")):
            with self.assertRaisesRegex(RuntimeError, "foreground peer failed"):
                self.pressure.guarded_run(0)
        self.assertEqual(sorted(path.name for path in self.root.glob("failed-*")), ["failed-3"])
        self.assertEqual((self.root / "failed-3").read_text(), original)


if __name__ == "__main__":
    unittest.main()
