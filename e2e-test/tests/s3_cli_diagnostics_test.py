# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import importlib.util
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('s3_cli_recipes',
    Path(__file__).resolve().parents[1] / 'clients/s3/clients.py')
recipes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recipes)


class S3CliDiagnosticsTest(unittest.TestCase):
    def test_failed_cli_keeps_both_outputs_and_command_without_credentials(self):
        result = subprocess.CompletedProcess([], 1, b'listing stdout', b'failure secret-value')
        with patch.object(recipes.subprocess, 'run', return_value=result):
            with self.assertRaises(AssertionError) as captured:
                recipes.run('aws', ['s3', 'ls', 's3://bucket/prefix?X-Amz-Signature=signed'],
                            {'AWS_SECRET_ACCESS_KEY': 'secret-value'})
        message = str(captured.exception)
        for expected in ['listing stdout', 'failure <redacted>', "'s3', 'ls'"]:
            self.assertIn(expected, message)
        self.assertNotIn('secret-value', message)
        self.assertNotIn('Signature=signed', message)
