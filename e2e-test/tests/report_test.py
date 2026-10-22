# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""A passing client cannot hide an incomplete fixture lifecycle."""

import json
import tempfile
import unittest
from pathlib import Path

from report import write_report


class ReportTest(unittest.TestCase):
    def test_expected_nonzero_exit_is_passed_only_when_fixture_lifecycle_also_passed(self):
        for fixture_status, actual, expected, outcome in (
            ("pass", 1, 1, "pass"), ("pass", 2, 1, "failed"), ("failed", 1, 1, "failed"),
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
