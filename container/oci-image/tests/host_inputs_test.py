# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host listeners are validated before creating a node container."""

import importlib.util
from pathlib import Path
import socket
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from runtime import Runtime

spec = importlib.util.spec_from_file_location("run_node", Path(__file__).resolve().parents[1] / "run-node.py")
run_node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_node)


class HostInputsTest(unittest.TestCase):
    def test_port_offset_shifts_every_host_listener_for_additional_instances(self):
        shifted = run_node.shifted_ports(1)
        self.assertEqual(shifted[0], 2223)
        self.assertIn(10001, shifted)
        self.assertIn(10101, shifted)
        with self.assertRaisesRegex(ValueError, "beyond port 65535"):
            run_node.shifted_ports(65535)

    def test_runtime_logs_preserve_stderr_without_polluting_machine_readable_inspection(self):
        with patch('runtime.subprocess.run', return_value=SimpleNamespace(
                returncode=0,stdout='{"status":"running"}',stderr='startup failed on stderr')):
            runtime = Runtime('docker')
            self.assertEqual(runtime.invoke('inspect','owned'),'{"status":"running"}')
            self.assertEqual(runtime.invoke('logs','owned'),'{"status":"running"}startup failed on stderr')

    def test_closed_server_time_wait_does_not_prevent_immediate_recreation(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
            listener.listen()
            with socket.create_connection(('127.0.0.1', port)) as client:
                accepted, _ = listener.accept()
                accepted.close()
                self.assertEqual(client.recv(1), b'')
        with socket.socket() as without_reuse:
            with self.assertRaises(OSError):
                without_reuse.bind(('0.0.0.0', port))
        run_node.check_host_ports([port])

    def test_occupied_host_listener_rejects_before_runtime_launch(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            listener.listen()
            with self.assertRaisesRegex(ValueError, f"port {port} is already occupied"):
                run_node.check_host_ports([port])
            self.assertEqual(listener.getsockname()[1], port)
