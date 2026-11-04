# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Exercise exact journal request/response ownership without intercepting E2E APIs."""

from pathlib import Path
import subprocess
import unittest


class JournalRequestStabilityTest(unittest.TestCase):
    def test_journal_runtime_request_identity_and_refresh_order(self):
        script = Path(__file__).with_name("journal_runtime_request_test.mjs")
        result = subprocess.run(["pixi", "run", "node", "--experimental-strip-types", "--test", str(script)],
                                capture_output=True, text=True, timeout=60)
        print(result.stdout + result.stderr, end="", flush=True)
        self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
