# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

from console_cli_fixture import CliContainerCluster, LABEL, OWNER_LABEL
from console_cli_target import CliAttachedCluster
from fixture import Cluster


class CliOwnershipTest(unittest.TestCase):
    def test_diagnostics_export_bounded_access_metrics_before_teardown(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = CliAttachedCluster("image", Path(directory), Path(directory))
            cluster.addresses = ["192.0.2.1"]
            response = Mock()
            response.read.return_value = b'crowdb_s3_chunk_readiness_errors_total{phase="diskio"} 1\n'
            with patch("console_cli_client.urllib.request.build_opener") as build, \
                    patch.object(Cluster, "diagnostics") as collect:
                opener = build.return_value
                opener.open.return_value = MagicMock()
                opener.open.return_value.__enter__.return_value = response
                cluster.diagnostics()
            opener.open.assert_called_once_with("http://192.0.2.1:9091/_crowdb/metrics", timeout=2)
            response.read.assert_called_once_with(1024 * 1024 + 1)
            collect.assert_called_once()
            self.assertIn('phase="diskio"', (cluster.artifacts / "access-metrics-0.prom").read_text())

    def test_diagnostics_preserve_errors_and_continue_other_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = CliAttachedCluster("image", Path(directory), Path(directory))
            cluster.addresses = ["192.0.2.1"]
            with patch("console_cli_client.urllib.request.build_opener") as build, \
                    patch.object(Cluster, "diagnostics") as collect:
                build.return_value.open.side_effect = TimeoutError("metrics unavailable")
                cluster.diagnostics()
            collect.assert_called_once()
            self.assertEqual((cluster.artifacts / "access-metrics-0.error").read_text(), "metrics unavailable")

    def test_recovery_observation_rejects_foreign_owner_before_file_access(self):
        cluster = object.__new__(CliContainerCluster)
        cluster.servers = ["owned"]
        cluster.root = Path("/owned/root")
        cluster.image = "immutable-image"
        cluster.run_id = "expected-cluster"
        cluster.state = lambda: {"owner": "expected-owner", "image_id": cluster.image}
        cluster.command = Mock(return_value=json.dumps([{
            "Image": cluster.image,
            "Config": {"Labels": {LABEL: "foreign", OWNER_LABEL: "expected-owner"}}}]))
        with self.assertRaisesRegex(ValueError, "ownership"):
            cluster.read_owned("node-binding.json")
        cluster.command.assert_called_once_with("inspect", "owned")

    def test_recovery_observation_rejects_paths_before_runtime_access(self):
        cluster = object.__new__(CliContainerCluster)
        cluster.command = Mock()
        for path in ("../credentials", "secrets/server.env", "/etc/passwd"):
            with self.assertRaisesRegex(ValueError, "unsupported"):
                cluster.read_owned(path)
        cluster.command.assert_not_called()

    def test_cleanup_uses_independent_host_operation_with_case_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = CliContainerCluster("image", Path(directory), Path(directory))
            cluster.timings["status"] = "pass"
            with patch("console_cli_fixture.subprocess.run") as run:
                run.return_value.returncode = 0
                cluster.close()
            arguments = run.call_args.args[0]
            self.assertTrue(arguments[1].endswith("container/oci-image/clean.py"))
            self.assertTrue(Path(arguments[1]).is_file())
            self.assertEqual(arguments[2:], ["--cluster", cluster.run_id, "--delete-data"])
            self.assertEqual(run.call_args.kwargs["env"]["CROWDB_RUNTIME_ROOT"], str(cluster.root))
            self.assertTrue(cluster.closed)

    def test_failed_cleanup_preserves_retry_state(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = CliContainerCluster("image", Path(directory), Path(directory))
            with patch("console_cli_fixture.subprocess.run") as run:
                run.return_value.returncode = 1
                run.return_value.stdout = ""
                run.return_value.stderr = "daemon unavailable"
                with self.assertRaisesRegex(RuntimeError, "daemon unavailable"):
                    cluster.close()
            self.assertEqual(run.call_args.args[0][2:], ["--cluster", cluster.run_id, "--keep-data"])
            self.assertFalse(cluster.closed)

    def test_recovery_observation_rejects_invalid_node_before_inspection(self):
        cluster = object.__new__(CliContainerCluster)
        cluster.servers = ["owned"]
        cluster.command = Mock()
        for index in (-1, 1, True, "0"):
            with self.assertRaisesRegex(ValueError, "outside"):
                cluster.read_owned("node-binding.json", index)
        cluster.command.assert_not_called()
