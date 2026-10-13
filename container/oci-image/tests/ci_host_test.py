# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""A stopped socket inode must not be mistaken for a live Docker daemon."""

import importlib.util
import socket
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("prepare_ci_host", Path(__file__).resolve().parents[1] / "prepare-ci-host.py")
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


class CiHostTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "docker.sock"

    def test_stopped_socket_inode_is_removed(self):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(self.path))
            listener.listen()
        self.assertTrue(self.path.is_socket())
        host.remove_stale_socket(self.path)
        self.assertFalse(self.path.exists())

    def test_active_listener_is_preserved_and_rejected(self):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(self.path))
            listener.listen()
            with self.assertRaisesRegex(RuntimeError, "remains active"):
                host.remove_stale_socket(self.path)
            self.assertTrue(self.path.is_socket())

    def test_regular_file_is_preserved_and_rejected(self):
        self.path.write_text("unrelated data")
        with self.assertRaisesRegex(RuntimeError, "non-socket"):
            host.remove_stale_socket(self.path)
        self.assertEqual(self.path.read_text(), "unrelated data")

    def test_absent_socket_needs_no_cleanup(self):
        host.remove_stale_socket(self.path)
        self.assertFalse(self.path.exists())
