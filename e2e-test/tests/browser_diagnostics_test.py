# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Exercise the browser deployment evidence collector with owned request events."""

from pathlib import Path
import subprocess
import unittest


class BrowserDiagnosticsTest(unittest.TestCase):
    def test_deployment_request_evidence_and_observation_failures(self):
        script = Path(__file__).with_name("browser_deployment_test.mjs")
        result = subprocess.run(["pixi", "run", "node", "--experimental-strip-types", "--test", str(script)],
                                capture_output=True, text=True, timeout=60)
        print(result.stdout + result.stderr, end="", flush=True)
        self.assertEqual(result.returncode, 0)
