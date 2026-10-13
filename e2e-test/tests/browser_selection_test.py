# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import Mock, patch
import subprocess

from console_shell_fixture import ShellBrowserCluster


class BrowserSelectionTest(unittest.TestCase):
    def test_native_browser_outer_budget_leaves_time_for_report_and_exit(self):
        for scale, inner, outer in (("1", "600000", 660), ("2", "1200000", 1260)):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cluster = ShellBrowserCluster.__new__(ShellBrowserCluster)
                cluster.bundle = root / "bundle"
                (cluster.bundle / "clients/console-ui").mkdir(parents=True)
                cluster.artifacts = root / "artifacts"
                (cluster.artifacts / "browser").mkdir(parents=True)
                cluster.addresses = ["127.0.0.1"]
                cluster.browser_timeout = 600
                cluster.case_name = "six-node"
                (cluster.artifacts / "browser/results.json").write_text(json.dumps({
                    "stats": {"expected": 1, "unexpected": 0, "skipped": 0, "flaky": 0}}))
                cluster.invoke = Mock()
                with patch.dict(os.environ, {"CROWDB_E2E_TIMEOUT_SCALE": scale}):
                    cluster.browser("six-node", config="native.config.ts")
                self.assertEqual(cluster.invoke.call_args.kwargs["timeout"], outer)
                self.assertEqual(cluster.invoke.call_args.args[2]["CROWDB_WEB_E2E_TEST_TIMEOUT_MS"], inner)

    def test_failure_exception_retains_original_assertion_for_final_ci_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = ShellBrowserCluster.__new__(ShellBrowserCluster)
            cluster.artifacts = Path(directory)
            cluster.case_name = "six-node"
            failure = "Error: expected 201, received 500\nbackend refused deployment\n"
            result = subprocess.CompletedProcess(["browser"], 1, failure, "")
            with patch("console_shell_fixture.subprocess.run", return_value=result):
                with self.assertRaises(RuntimeError) as raised:
                    cluster.invoke(["browser"], Path(directory), {}, "browser")
            self.assertIn(failure, str(raised.exception))
            self.assertIn("six-node", str(raised.exception))
            self.assertEqual((cluster.artifacts / "client-browser.log").read_text(), failure)

    def test_literal_titles_match_playwright_full_names_without_regex_expansion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cluster = ShellBrowserCluster.__new__(ShellBrowserCluster)
            cluster.bundle = root / "bundle"
            (cluster.bundle / "clients/console-ui").mkdir(parents=True)
            cluster.artifacts = root / "artifacts"
            cluster.artifacts.mkdir()
            cluster.addresses = ["127.0.0.1"]
            cluster.case_name = "selection"
            (cluster.artifacts / "browser").mkdir()
            (cluster.artifacts / "browser/results.json").write_text(json.dumps({
                "stats": {"expected": 2, "unexpected": 0, "skipped": 0, "flaky": 0}}))
            cluster.invoke = Mock()
            cluster.browser(("history (back/forward)", "key a+b"), expected=2)
            self.assertEqual(cluster.invoke.call_args.kwargs["timeout"], 180)
            self.assertEqual(cluster.invoke.call_args.args[2]["CROWDB_WEB_E2E_TEST_TIMEOUT_MS"], "120000")
            arguments = cluster.invoke.call_args.args[0]
            pattern = arguments[arguments.index("--grep") + 1]
            for title in ("history (back/forward)", "key a+b"):
                self.assertIsNotNone(re.search(pattern, f"chromium spec.ts {title}"))
            self.assertIsNone(re.search(pattern, "chromium spec.ts history back/forward"))
            self.assertIsNone(re.search(pattern, "chromium spec.ts key aaab"))
