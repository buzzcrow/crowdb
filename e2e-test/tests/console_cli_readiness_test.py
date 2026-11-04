# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from console_cli_readiness import check_progress, check_unavailable, observe_unavailable


def failed(generation=24, checked=10000, age=2705, state="current"):
    return {"ready": False, "live": True,
            "chunks": "unavailable" if state == "current" else "unknown",
            "details": {"chunks": "DiskIO read exceeded the readiness deadline"},
            "reasons": ["chunks"] if state == "current" else ["observation-expired"],
            "observation": {"generation": generation, "checked_at_unix_ms": checked,
                            "age_ms": age, "max_age_ms": 3000, "state": state}}


def advancing():
    return [{"responses": [failed(24, 10000), failed(26, 10500), failed(25, 10100, 3071, "expired")]},
            {"responses": [failed(25, 12000), failed(27, 12500), failed(26, 12100)]},
            {"responses": [failed(26, 14000), failed(28, 14500), failed(27, 14100)]}]


class UnavailableProgressTest(unittest.TestCase):
    def test_expired_negative_evidence_keeps_truthful_age_and_continuous_progress(self):
        check_progress(advancing(), 3)

    def test_three_second_quantized_boundary_allows_both_states(self):
        for state in ("current", "expired"):
            check_unavailable([failed(age=3000, state=state)], 1)

    def test_stalled_sampler_fails_even_when_its_last_sample_is_current(self):
        history = [{"responses": [failed()]} for _ in range(4)]
        with self.assertRaisesRegex(AssertionError, "Sampler did not advance twice"):
            check_progress(history, 1)

    def test_one_stalled_endpoint_cannot_hide_behind_healthy_siblings(self):
        history = advancing()
        for sample in history:
            sample["responses"][1] = failed()
        with self.assertRaisesRegex(AssertionError, "Sampler did not advance twice"):
            check_progress(history, 3)

    def test_missing_error_ready_or_inconsistent_freshness_samples_are_rejected(self):
        for value in ({"error": "health request timed out"},
                      dict(failed(), ready=True),
                      failed(age=3071),
                      failed(age=2999, state="expired"),
                      dict(failed(), details={})):
            with self.subTest(value=value), self.assertRaises(AssertionError):
                check_unavailable([value], 1)

    def test_timestamp_refresh_without_a_new_generation_and_regression_are_rejected(self):
        for generation, checked in ((24, 11000), (23, 12000), (25, 9999)):
            history = copy.deepcopy(advancing())
            history[1]["responses"][0] = failed(generation, checked)
            with self.subTest(generation=generation, checked=checked), self.assertRaises(AssertionError):
                check_progress(history, 3)

    def test_missing_endpoint_is_rejected(self):
        history = advancing()
        history[1]["responses"].pop()
        with self.assertRaises(AssertionError):
            check_progress(history, 3)

    def test_observation_uses_the_last_eight_seconds_of_the_original_outage(self):
        clock = [0.0]
        calls = []

        def sleep(seconds):
            clock[0] += seconds

        def snapshot():
            calls.append(clock[0])
            generation = int(clock[0] // 2) + 1
            clock[0] += 0.1
            return [failed(generation, generation * 2000) for _ in range(3)]

        with tempfile.TemporaryDirectory() as directory, \
                patch("console_cli_readiness.time.monotonic", side_effect=lambda: clock[0]), \
                patch("console_cli_readiness.time.sleep", side_effect=sleep):
            observe_unavailable(directory, snapshot, 3)
            report = json.loads((Path(directory) / "access-health-paused-history.json").read_text())
        self.assertEqual(calls[0], 32)
        self.assertLessEqual(calls[-1], 40)
        self.assertAlmostEqual(clock[0], 40.1)
        self.assertIsNone(report["failure"])
        self.assertEqual(len(report["samples"]), len(calls))

    def test_failed_sample_is_retained_and_not_retried_until_it_passes(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch("console_cli_readiness.time.sleep"), \
                self.assertRaises(AssertionError):
            try:
                observe_unavailable(directory, lambda: [{"error": "unreachable"}], 1)
            finally:
                report = json.loads((Path(directory) / "access-health-paused-history.json").read_text())
                self.assertEqual(len(report["samples"]), 1)
                self.assertEqual(report["samples"][0]["responses"], [{"error": "unreachable"}])
                self.assertIsNotNone(report["failure"])


if __name__ == "__main__":
    unittest.main()
