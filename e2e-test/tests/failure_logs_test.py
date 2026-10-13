# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Failed fixtures surface service errors while preserving private diagnostics."""

import contextlib
import gzip
import io
import tempfile
import unittest
from pathlib import Path

from failure_logs import print_failure
from fixture import Cluster, redact
from unittest.mock import patch


class FailureLogsTest(unittest.TestCase):
    def test_service_errors_stacks_raw_stderr_and_archives_are_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = root / "artifacts"
            artifacts.mkdir()
            data = root / "node3"
            logs = data / "log" / "chunk-kv-3"
            logs.mkdir(parents=True)
            (logs / "server.log").write_text(
                "2026-10-13T07:00:00Z INFO started\n"
                "2026-10-13T07:00:01Z ERROR startup failed password=private-value\n"
                "stack frame: bootstrap_catalog\n"
                "2026-10-13T07:00:02Z INFO stopped\n")
            (logs / "service.log").write_text("Killed\n")
            with gzip.open(logs / "server-old.log.gz", "wt") as stream:
                stream.write("2026-10-13T06:00:00Z ERROR old crash\n")
            private = root / "private.log"
            private.write_text("ERROR unrelated secret\n")
            (logs / "foreign.log").symlink_to(private)
            (artifacts / "foreign.error").symlink_to(private)
            output = io.StringIO()
            with patch.dict("os.environ", {"CROWDB_E2E_PRINT_SERVICE_ERRORS": "1"}), \
                 contextlib.redirect_stdout(output):
                print_failure(artifacts, [data], "six-node", RuntimeError("deploy failed"), redact)
            text = output.getvalue()
            for expected in ("six-node", "node3", "chunk-kv-3", "startup failed", "bootstrap_catalog", "Killed", "old crash"):
                self.assertIn(expected, text)
            for excluded in ("private-value", "unrelated secret", "INFO started", "INFO stopped"):
                self.assertNotIn(excluded, text)
            self.assertIn("private-value", (logs / "server.log").read_text())

    def test_service_output_defaults_off_and_disabled_mode_does_not_read_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for environment in ({}, {"CROWDB_E2E_PRINT_SERVICE_ERRORS": "0"}):
                output = io.StringIO()
                with patch.dict("os.environ", environment, clear=True), \
                     patch.object(Path, "glob") as files, contextlib.redirect_stdout(output):
                    print_failure(root, [root], "case", RuntimeError("test failed"), redact)
                files.assert_not_called()
                self.assertIn("test failed", output.getvalue())
                self.assertNotIn("CONTAINER ERROR LOG", output.getvalue())

    def test_only_failed_fixture_prints_service_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            for error in (None, RuntimeError("client failed")):
                cluster = Cluster("image", directory)
                cluster.case_name = "owned-case"
                with patch.object(cluster, "diagnostics"), patch.object(cluster, "close"), \
                     patch("failure_logs.print_failure") as printed:
                    cluster.__exit__(type(error) if error else None, error, None)
                if error:
                    self.assertEqual(printed.call_args.args[2], "owned-case")
                else:
                    printed.assert_not_called()
