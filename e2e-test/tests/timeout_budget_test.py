# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import unittest
from unittest.mock import patch

from timeout_budget import scaled_timeout


class TimeoutBudgetTest(unittest.TestCase):
    def test_local_and_ci_use_the_same_default(self):
        for environment in ({}, {"CI": "true"}):
            with patch.dict(os.environ, environment, clear=True):
                self.assertEqual(scaled_timeout(90), 90)

    def test_debug_override_and_invalid_values(self):
        with patch.dict(os.environ, {"CROWDB_E2E_TIMEOUT_SCALE": "2"}):
            self.assertEqual(scaled_timeout(90), 180)
        for value in ("0", "-1", "nan", "inf", "bad"):
            with patch.dict(os.environ, {"CROWDB_E2E_TIMEOUT_SCALE": value}):
                with self.assertRaises(ValueError):
                    scaled_timeout(90)
