# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import run
from special_profiles import cleanup_owned_hosts


class HostProfileTest(unittest.TestCase):
    def test_selection_and_resource_validation_precede_docker(self):
        for args in (('--concurrency','2'), ('--concurrency','1','--cpus','1'),
                     ('--concurrency','1','--memory-mib','2048'),
                     ('--concurrency','1','--layer','kv')):
            with patch.object(sys,'argv',['run.py','--profile','host',*args]), \
                    patch('special_profiles.docker') as docker, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit): run.main()
                docker.assert_not_called()
        output = io.StringIO()
        with patch.object(sys,'argv',['run.py','--profile','host','--concurrency','1','--list']), \
                patch('special_profiles.docker') as docker, contextlib.redirect_stdout(output):
            run.main()
            docker.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())[0]['scope'],'deployment.host')

    def test_cleanup_rejects_invalid_ownership_and_still_cleans_the_other_known_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for case in ('protocols','supplied'):
                path = root / case / 'private/runtime/persistent/oci-hosts/owned.json'
                path.parent.mkdir(parents=True)
                path.write_text('{}')
            for failure in ('changed image', 'foreign owner', 'different daemon', 'different mount'):
                with patch('special_profiles.read_private', return_value={}), \
                        patch('special_profiles.selected', side_effect=[ValueError(failure), {'verified':'receipt'}]), \
                        patch('special_profiles.clean') as clean:
                    with self.assertRaisesRegex(RuntimeError, failure): cleanup_owned_hosts(root)
                    clean.assert_called_once_with({'verified':'receipt'})

    def test_cleanup_does_not_search_the_runtime_when_no_known_receipt_exists(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch('special_profiles.selected') as selected, patch('special_profiles.clean') as clean:
            cleanup_owned_hosts(Path(temporary))
            selected.assert_not_called()
            clean.assert_not_called()
