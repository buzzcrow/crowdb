# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host cleanup stays independent of the CLI and cannot remove foreign state."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from host_cli_test import HostCliTest, container
from host_cleanup import clean, selected


class HostCleanupTest(unittest.TestCase):
    setUp = HostCliTest.setUp
    def test_exact_owner_is_stopped_then_removed_without_data_effects(self):
        self.runtime.current = container(self.record)
        with patch('host_cleanup.HostRuntime', return_value=self.runtime):
            clean(self.record)
        self.assertEqual(self.runtime.effects, [('stop', 'owned-container'), ('rm', 'owned-container')])

    def test_foreign_owner_or_daemon_never_receives_effects(self):
        for changed_daemon in (False, True):
            self.runtime.current = container(self.record)
            if changed_daemon: self.runtime.daemon['daemon'] = 'foreign'
            else: self.runtime.current['Config']['Labels']['org.crowdb.launcher'] = 'foreign'
            with patch('host_cleanup.HostRuntime', return_value=self.runtime), self.assertRaises(ValueError):
                clean(self.record)
        self.assertEqual(self.runtime.effects, [])

    def test_private_receipt_binding_and_symlink_fences(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root/'persistent/oci-hosts/test.json'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(self.record))
            path.chmod(0o600)
            self.assertEqual(selected('test', root), self.record)
            binding = root/'persistent/clusters/test/target.json'
            binding.parent.mkdir(parents=True)
            binding.write_text(json.dumps(dict(version=1, name='test', management_endpoints=self.record['origins'],
                runtime=dict(selection=self.record['runtime']['selection'], owner='foreign'))))
            binding.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'target conflicts'): selected('test', root)
            binding.unlink()
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'private regular'): selected('test', root)
            path.unlink()
            path.symlink_to('/missing')
            with self.assertRaisesRegex(ValueError, 'symlinks'): selected('test', root)
