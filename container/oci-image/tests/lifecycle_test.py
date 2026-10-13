# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Daemon cleanup on startup, build failure and cancellation; publication failures."""

import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from builder import running
from transfer import publish


class LifecycleTest(unittest.TestCase):
    def test_build_failure_and_interruption_stop_owned_daemon(self):
        for failure in (subprocess.CalledProcessError(1, "buildctl"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                process = MagicMock(pid=42)
                process.poll.return_value = None
                probe = MagicMock(returncode=0)
                with (patch("builder.preflight"), patch("builder.tool", side_effect=lambda name: name),
                      patch("builder.subprocess.Popen", return_value=process),
                      patch("builder.subprocess.run", return_value=probe), patch("builder.Path.exists", return_value=True),
                      patch("builder.os.killpg") as kill, self.assertRaises(type(failure)), running(Path(directory), privileged=True)):
                    raise failure
                kill.assert_called_once_with(42, signal.SIGTERM)
                process.wait.assert_called_once_with(timeout=10)

    def test_failed_start_and_readiness_deadline(self):
        for exited in (True, False):
            with self.subTest(exited=exited), tempfile.TemporaryDirectory() as directory:
                process = MagicMock(pid=42)
                process.poll.return_value = 1 if exited else None
                times = [0, 1] if exited else [0, 31]
                with (patch("builder.preflight"), patch("builder.tool", side_effect=lambda name: name),
                      patch("builder.subprocess.Popen", return_value=process), patch("builder.time.monotonic", side_effect=times),
                      patch("builder.os.killpg") as kill, self.assertRaises(RuntimeError if exited else TimeoutError), running(Path(directory), privileged=True)):
                    self.fail("unready daemon accepted a build")
                self.assertEqual(kill.call_count, 0 if exited else 1)


class PublicationTest(unittest.TestCase):
    def test_registry_copy_must_preserve_verified_digest(self):
        with (patch("transfer.receipt", return_value={"image_digest": "sha256:verified"}),
              patch("transfer.tool", return_value="skopeo"), patch("transfer.subprocess.run") as copy,
              patch("transfer.remote_digest", return_value="sha256:changed"),
              self.assertRaisesRegex(RuntimeError, "registry digest changed")):
            publish(Path("artifact.tar"), "registry/image:version", Path("private-auth.json"))
        self.assertIn("--preserve-digests", copy.call_args.args[0])
        self.assertIn("--all", copy.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
