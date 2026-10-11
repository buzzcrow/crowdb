# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host-client result and per-run environment contracts."""

import json
import io
import os
import subprocess
import tempfile
import tarfile
import unittest
from pathlib import Path
from unittest.mock import patch

from client import bundle_iceberg_rck, client_sources, run_client


class ClientTest(unittest.TestCase):
    def test_rck_archive_supplies_upstream_version_without_a_parent_git_repo(self):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tree:
            body = b"upstream build consumes version.txt"
            info = tarfile.TarInfo("build.gradle")
            info.size = len(body)
            tree.addfile(info, io.BytesIO(body))
        revision = "6976e020b894f6a6777704df2b8c4458cb291ae9"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "cache"
            source.mkdir()
            with patch.dict(os.environ, {"CROWDB_ICEBERG_RCK_ROOT": str(source)}), \
                    patch("client.subprocess.check_output", side_effect=[revision, "", archive.getvalue()]):
                bundle_iceberg_rck(root / "artifact")
            output = root / "artifact/clients/iceberg-rck"
            self.assertEqual((output / "source/version.txt").read_text(), "1.11.0\n")
            self.assertIn(f"git.commit.id={revision}\n",
                          (output / "source/iceberg-build.properties").read_text())
            self.assertEqual(json.loads((output / "source.json").read_text())["revision"], revision)
            self.assertFalse((output / "source/.git").exists())

    def test_client_identity_keeps_locks_and_excludes_mutable_environments(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            project = root / "tpc-loader"
            project.mkdir()
            for name in ("pixi.toml", "pixi.lock"):
                (project / name).write_text("locked client")
            for directory in (".pixi/envs/default", "target", "node_modules"):
                path = project / directory
                path.mkdir(parents=True)
                (path / "installed.py").write_text("mutable dependency")
            self.assertEqual(
                {path.relative_to(root).as_posix() for path in client_sources(root)},
                {"tpc-loader/pixi.toml", "tpc-loader/pixi.lock"},
            )

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
