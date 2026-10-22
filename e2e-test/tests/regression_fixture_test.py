# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Measurement ownership guards; these tests do not certify OCI deployments."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "measurement_fixture", Path(__file__).resolve().parents[2] / "tools/benchmark/fixture.py")
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class RegressionFixtureTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "console.toml"
        self.descriptor = self.state.with_suffix(".fixture.json")
        self.value = {"pid": os.getpid(), "start": fixture.process_start(os.getpid()),
                      "endpoint": "http://127.0.0.1:1", "token": "owner", "root": str(self.root)}

    def write_descriptor(self):
        self.descriptor.write_text(json.dumps(self.value))
        self.descriptor.chmod(0o600)

    def test_current_process_identity_is_required(self):
        self.write_descriptor()
        self.assertEqual(fixture.descriptor(self.state), self.value)
        self.value["start"] = "foreign-start"
        self.write_descriptor()
        with self.assertRaises(fixture.FixtureStopped):
            fixture.descriptor(self.state)

    def test_descriptor_rejects_public_file_symlink_and_oversized_file(self):
        self.write_descriptor()
        self.descriptor.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "private regular"):
            fixture.descriptor(self.state)
        self.descriptor.unlink()
        self.descriptor.symlink_to(self.state)
        with self.assertRaisesRegex(ValueError, "private regular"):
            fixture.descriptor(self.state)
        self.descriptor.unlink()
        self.write_descriptor()
        self.descriptor.write_text(" " * 65537)
        with self.assertRaisesRegex(ValueError, "bounded private"):
            fixture.descriptor(self.state)

    def test_dead_owner_cannot_spawn_a_fixture(self):
        args = argparse.Namespace(state=self.state, owner_pid=None, arguments=["--kind", "kv"])
        with patch.object(fixture.subprocess, "Popen") as spawn:
            with self.assertRaisesRegex(ValueError, "live measurement owner"):
                fixture.setup(args)
            spawn.assert_not_called()

    def test_failed_client_drops_stale_report_and_does_not_inherit_cluster_target(self):
        self.write_descriptor()
        report = self.root / "client-runtime/artifacts/cli/bench-kv/report.json"
        report.parent.mkdir(parents=True)
        report.write_text('{"total_ops": 999}')
        args = argparse.Namespace(state=self.state, action="client", arguments=["bench", "kv", "bad"])
        with patch.dict(os.environ, {"CROWDB_CLUSTER": "foreign"}), \
                patch.object(fixture.subprocess, "call", return_value=2) as call:
            self.assertEqual(fixture.run(args), 2)
            self.assertNotIn("CROWDB_CLUSTER", call.call_args.kwargs["env"])
            self.assertEqual(call.call_args.args[0][1:3],
                             ["--management-endpoint", self.value["endpoint"]])
        self.assertFalse(report.exists())
        self.assertFalse((self.root / "measurements").exists(), "failed parsing retained stale logs")

    def test_invalid_benchmark_family_cannot_delete_arbitrary_directories(self):
        self.write_descriptor()
        retained = self.root / "client-runtime/artifacts/cli/keep"
        retained.mkdir(parents=True)
        witness = retained / "witness"
        witness.write_text("preserved")
        args = argparse.Namespace(state=self.state, action="client",
                                  arguments=["bench", "kv/../keep"])
        with patch.object(fixture.subprocess, "call", return_value=2):
            self.assertEqual(fixture.run(args), 2)
        self.assertEqual(witness.read_text(), "preserved")

    def test_stop_with_stale_identity_never_sends_a_control_request(self):
        self.value["start"] = "foreign-start"
        self.write_descriptor()
        args = argparse.Namespace(state=self.state, action="stop")
        with patch.object(fixture.HTTP, "open") as request:
            self.assertEqual(fixture.run(args), 0)
            request.assert_not_called()

    def check_service_logs(self):
        script = ("source \"$1\"\n"
                  "regression_fixture_log_root() { printf '%s\\n' \"$TEST_FIXTURE_ROOT\"; }\n"
                  "regression_require_no_service_errors\n")
        common = Path(__file__).resolve().parents[2] / "tools/benchmark/bench-regression-common.sh"
        return subprocess.run(["bash", "-c", script, "log-validation", str(common)],
            env=dict(os.environ, REGRESSION_LOG_ROOT=str(self.root), TEST_FIXTURE_ROOT=str(self.root)),
            capture_output=True, text=True, timeout=5)

    def test_service_log_scan_excludes_binary_error_strings(self):
        services = self.root / "services"
        services.mkdir()
        (services / "server").write_bytes(b"\0DiskNotExist\0")
        (services / "server.log").write_text("normal lifecycle\n")
        result = self.check_service_logs()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_service_log_errors_are_printed_and_fail_validation(self):
        services = self.root / "services"
        services.mkdir()
        (services / "server.log").write_text("DiskNotExist: lost registered disk\n")
        result = self.check_service_logs()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DiskNotExist: lost registered disk", result.stdout)

    def test_service_log_scan_failure_is_not_treated_as_no_errors(self):
        result = self.check_service_logs()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("grep status 2", result.stderr)
