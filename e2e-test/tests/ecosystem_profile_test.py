# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Manual selection, host-client ownership and public credential boundaries."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import run
from clients.ecosystem.acceptance import execute


class EcosystemProfileTest(unittest.TestCase):
    def test_manual_resource_and_domain_validation_precedes_runtime(self):
        for arguments in (("--concurrency", "2"), ("--concurrency", "1", "--cpus", "3.9"),
                          ("--concurrency", "1", "--memory-mib", "10239"),
                          ("--concurrency", "1", "--layer", "iceberg")):
            with patch.object(sys, "argv", ["run.py", "--profile", "ecosystem", "--node-cpus", "1", *arguments]), \
                    patch("ecosystem_profile.docker") as docker, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    run.main()
                docker.assert_not_called()
        output = io.StringIO()
        with patch.object(sys, "argv", ["run.py", "--profile", "ecosystem-python", "--concurrency", "1",
                "--node-cpus", "1", "--cpus", "4", "--memory-mib", "6144", "--list"]), \
                patch("ecosystem_profile.docker") as docker, contextlib.redirect_stdout(output):
            run.main()
            docker.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())[0]["case"], "python_duckdb")

    def test_missing_locked_tools_fail_before_runtime_allocation(self):
        with tempfile.TemporaryDirectory() as directory, patch("ecosystem_profile.ROOT", Path(directory)), \
                patch.object(sys, "argv", ["run.py", "--profile", "ecosystem-python", "--concurrency", "1",
                    "--node-cpus", "1", "--cpus", "4", "--memory-mib", "6144"]), \
                patch("ecosystem_profile.docker") as docker, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                run.main()
            docker.assert_not_called()

    def test_public_logs_redact_credentials_and_request_signatures(self):
        script = Path(__file__).resolve().parents[1] / "clients/ecosystem/diagnostics.py"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "private").mkdir()
            (root / "results").mkdir()
            (root / "private/credentials.env").write_text('ICEBERG_TOKEN=known-token\nMASTER_KEY="known-master"\n')
            (root / "private/client.log").write_text(
                'known-token known-master Authorization: Bearer other-token\n'
                'X-Amz-Signature=signature&X-Amz-Credential=credential\n')
            subprocess.run([sys.executable, str(script), directory], check=True, capture_output=True)
            output = (root / "results/client.log").read_text()
            for secret in ("known-token", "known-master", "other-token", "signature&", "credential\n"):
                self.assertNotIn(secret, output)
            self.assertIn("[REDACTED]", output)
            self.assertFalse((root / "results/credentials.env").exists())

    def test_interrupted_host_client_terminates_its_process_group(self):
        with tempfile.TemporaryDirectory() as directory, patch("clients.ecosystem.acceptance.subprocess.Popen") as spawn, \
                patch("clients.ecosystem.acceptance.os.killpg") as kill:
            child = spawn.return_value
            child.pid = 12345
            child.poll.return_value = None
            child.wait.side_effect = [KeyboardInterrupt(), 0]
            with self.assertRaises(KeyboardInterrupt):
                execute(["host-client"], os.environ.copy(), Path(directory) / "client.log")
            self.assertTrue(spawn.call_args.kwargs["start_new_session"])
            kill.assert_called_once()
