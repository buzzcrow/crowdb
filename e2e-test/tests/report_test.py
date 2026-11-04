# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""A passing client cannot hide an incomplete fixture lifecycle."""

import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from report import write_report


class ReportTest(unittest.TestCase):
    def test_expected_nonzero_exit_is_passed_only_when_fixture_lifecycle_also_passed(self):
        for fixture_status, actual, expected, outcome in (
            ("pass", 1, 1, "pass"), ("pass", 2, 1, "failed"), ("failed", 1, 1, "failed"),
            ("not_executed", 0, 0, "not_executed"),
        ):
            with self.subTest(fixture=fixture_status, actual=actual, expected=expected):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    case = root / "crowdb-e2e-owned"
                    case.mkdir()
                    for name, value in {
                        "case.json": {"case": "waiting", "scope": "deployment.reference"},
                        "client-check.json": {"phase": "check", "seconds": 1.0,
                            "exit_code": actual, "expected_exit_code": expected},
                        "timing.json": {"status": fixture_status, "total": 2.0},
                    }.items():
                        (case / name).write_text(json.dumps(value))
                    write_report(root, {"status": fixture_status})
                    results = json.loads((root / "results.json").read_text())
                    self.assertEqual(results["cases"][0]["status"], outcome)

    def test_failed_cleanup_is_a_failed_case_even_after_client_passed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case = root / "crowdb-e2e-owned"
            case.mkdir()
            for name, value in {
                "case.json": {"case": "case", "scope": "kv.management"},
                "client-write.json": {"phase": "write", "seconds": 1.0, "exit_code": 0},
                "timing.json": {"status": "failed", "total": 2.0},
            }.items():
                (case / name).write_text(json.dumps(value))
            write_report(root, {"status": "failed"})
            results = json.loads((root / "results.json").read_text())
            self.assertEqual(results["cases"][0]["status"], "failed")
            self.assertEqual(results["timings"]["status"], "failed")
            self.assertEqual(results["slowest_clients"][0]["seconds"], 1.0)

    def test_measurement_counts_not_executed_separately_and_retains_batch_failure(self):
        path = Path(__file__).resolve().parents[2] / 'tools/test-metrics/measure.py'
        spec = importlib.util.spec_from_file_location('fixture_report_measure', path)
        measure = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(measure)

        def fake_command(command, **kwargs):
            if command[:3] == ['pixi', 'run', 'clean']:
                return subprocess.CompletedProcess(command, 0)
            output = Path(command[command.index('--artifacts') + 1]) / 'run-test'
            output.mkdir(parents=True)
            (output / 'results.json').write_text(json.dumps({
                'timings': {'status': 'failed'},
                'cases': [{'status': status} for status in ('pass', 'failed', 'not_executed')]}))
            kwargs['stdout'].write('original startup rejected\n')
            return subprocess.CompletedProcess(command, 1)

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(measure, 'ROOT', Path(directory)), \
                patch.object(measure, 'TaskGraph') as graph, \
                patch.object(measure.subprocess, 'run', side_effect=fake_command), \
                patch.object(sys, 'argv', ['measure.py', 'test-e2e-console']), \
                contextlib.redirect_stdout(io.StringIO()) as console:
            graph.return_value.tasks = {('default', 'test-e2e-console'): 'unused'}
            self.assertEqual(measure.main(), 1)
            record = json.loads((Path(directory) /
                                 '.crowdb-runtime/artifacts/measure-tests/latest.json').read_text())[0]
            self.assertEqual(record['passed'], 1)
            self.assertEqual(record['failed'], 1)
            self.assertEqual(record['not_executed'], 1)
            self.assertEqual(record['exit_code'], 1)
            self.assertIn('original startup rejected', console.getvalue())
