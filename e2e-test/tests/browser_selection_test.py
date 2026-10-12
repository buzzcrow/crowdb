# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import Mock

from console_shell_fixture import ShellBrowserCluster


class BrowserSelectionTest(unittest.TestCase):
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
            arguments = cluster.invoke.call_args.args[0]
            pattern = arguments[arguments.index("--grep") + 1]
            for title in ("history (back/forward)", "key a+b"):
                self.assertIsNotNone(re.search(pattern, f"chromium spec.ts {title}"))
            self.assertIsNone(re.search(pattern, "chromium spec.ts history back/forward"))
            self.assertIsNone(re.search(pattern, "chromium spec.ts key aaab"))
