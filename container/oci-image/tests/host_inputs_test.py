# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host listeners are validated before creating a node container."""

import importlib.util
from pathlib import Path
import socket
import unittest

spec = importlib.util.spec_from_file_location("run_node", Path(__file__).resolve().parents[1] / "run-node.py")
run_node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_node)


class HostInputsTest(unittest.TestCase):
    def test_occupied_host_listener_rejects_before_runtime_launch(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            listener.listen()
            with self.assertRaisesRegex(ValueError, f"port {port} is already occupied"):
                run_node.check_host_ports([port])
            self.assertEqual(listener.getsockname()[1], port)
