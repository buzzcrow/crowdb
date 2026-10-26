# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import contextlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from client_process import run_client_process


class ClientProcessTest(unittest.TestCase):
    def test_reports_active_case_without_losing_stdout_stderr_or_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "client.log"
            release = Path(directory) / "release"
            console = io.StringIO()
            program = ("import pathlib,sys,time\nprint('ordinary output',flush=True)\n"
                       "while not pathlib.Path(sys.argv[1]).exists(): time.sleep(0.005)\n"
                       "print('service failed',file=sys.stderr)\nsys.exit(3)")

            def report(message, **kwargs):
                print(message, **kwargs)
                if message.startswith("[CLIENT RUNNING]"):
                    release.touch()

            with contextlib.redirect_stdout(console), \
                    patch("client_process.print", side_effect=report, create=True):
                result = run_client_process([sys.executable, "-c", program, str(release)], os.environ.copy(),
                                            log, 5, "case=measured phase=verify", heartbeat_seconds=0.02)
            self.assertEqual(result.returncode, 3)
            self.assertIn("ordinary output", result.stdout)
            self.assertIn("service failed", result.stdout)
            self.assertEqual(log.read_text(), result.stdout)
            self.assertIn("[CLIENT START] case=measured phase=verify", console.getvalue())
            self.assertIn("[CLIENT RUNNING] case=measured phase=verify", console.getvalue())
            self.assertIn("exit_code=3", console.getvalue())

    def test_timeout_reaps_client_and_retains_its_complete_output(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "client.log"
            program = "import time; print('before timeout',flush=True); time.sleep(60)"
            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(subprocess.TimeoutExpired) as raised:
                    run_client_process([sys.executable, "-c", program], os.environ.copy(),
                                       log, 0.3, "case=timeout", heartbeat_seconds=0.05)
            self.assertIn("before timeout", raised.exception.output)
            self.assertEqual(raised.exception.output, log.read_text())

    def test_interruption_kills_and_reaps_the_owned_client(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch("client_process.subprocess.Popen") as spawn:
            child = spawn.return_value.__enter__.return_value
            child.wait.side_effect = [KeyboardInterrupt, None]
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(KeyboardInterrupt):
                run_client_process(["client"], {}, Path(directory) / "client.log", 5, "case=interrupt")
            child.kill.assert_called_once()
            self.assertEqual(child.wait.call_count, 2)
