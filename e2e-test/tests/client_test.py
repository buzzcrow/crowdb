# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host-client result and per-run environment contracts."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from client import run_client


class ClientTest(unittest.TestCase):
    def test_zero_matching_cases_cannot_pass(self):
        result = subprocess.CompletedProcess([], 0, "test result: ok. 0 passed; 0 failed;", "")
        with tempfile.TemporaryDirectory() as root, patch("client.subprocess.run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "exactly one"):
                run_client(root, ["172.18.0.2"], "owned", "write", root)
            self.assertTrue(Path(root, "client-write.log").is_file())

    def test_endpoints_are_per_invocation_without_mutating_host_environment(self):
        result = subprocess.CompletedProcess([], 0, "test result: ok. 1 passed; 0 failed;", "")
        original = os.environ.copy()
        with tempfile.TemporaryDirectory() as root, patch("client.subprocess.run", return_value=result) as run:
            run_client(root, ["172.18.0.2"], "first", "write", root)
            run_client(root, ["172.19.0.2"], "second", "verify", root)
            first, second = [call.kwargs["env"] for call in run.call_args_list]
            self.assertEqual(json.loads(first["CROWDB_E2E_RPC"]), ["172.18.0.2"])
            self.assertEqual(json.loads(second["CROWDB_E2E_RPC"]), ["172.19.0.2"])
            self.assertEqual(first["CROWDB_E2E_CLIENT_ROOT"], str(Path(root) / "clients"))
            self.assertEqual(first["CROWDB_E2E_TOKEN"], "first")
            self.assertEqual(second["CROWDB_E2E_TOKEN"], "second")
            self.assertIn("172.18.0.2", first["NO_PROXY"].split(","))
            self.assertNotIn("172.18.0.2", second["no_proxy"].split(","))
        self.assertEqual(dict(os.environ), original)

    def test_timeout_keeps_partial_output_and_failure(self):
        timeout = subprocess.TimeoutExpired("client", 120, output=b"last checkpoint", stderr=b"RPC stalled")
        with tempfile.TemporaryDirectory() as root, patch("client.subprocess.run", side_effect=timeout):
            with self.assertRaises(subprocess.TimeoutExpired):
                run_client(root, [], "owned", "verify", root)
            self.assertIn("last checkpoint", Path(root, "client-verify.log").read_text())
            self.assertEqual(json.loads(Path(root, "client-verify.json").read_text())["status"], "timeout")
