# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host cleanup stays independent of the CLI and cannot remove foreign state."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import host_cli_test as fixture
from host_cleanup import check_data_users, clean, selected, selected_records


class HostCleanupTest(unittest.TestCase):
    setUp = fixture.HostCliTest.setUp
    def test_exact_owner_is_stopped_then_removed_without_data_effects(self):
        self.runtime.current = fixture.container(self.record)
        with patch('host_cleanup.HostRuntime', return_value=self.runtime):
            clean(self.record)
        self.assertEqual(self.runtime.effects, [('stop', 'owned-container'), ('rm', 'owned-container')])

    def test_foreign_owner_or_daemon_never_receives_effects(self):
        for changed_daemon in (False, True):
            self.runtime.current = fixture.container(self.record)
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

    def test_all_host_receipts_are_selected_and_validated_before_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / 'persistent/oci-hosts'
            directory.mkdir(parents=True)
            records = []
            for name in ('first', 'second'):
                record = dict(self.record, cluster=name, name='crowdb-' + name + '-host')
                path = directory / (name + '.json')
                path.write_text(json.dumps(record))
                path.chmod(0o600)
                records.append(record)
            self.assertEqual(selected_records(None, root), records)
            self.assertEqual(selected_records('second', root), records[1:])
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'private regular'):
                selected_records(None, root)

    def test_empty_host_inventory_and_symlinked_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(selected_records(None, root), [])
            self.assertEqual(selected_records('missing', root), [])
            (root / 'persistent').mkdir()
            (root / 'persistent/oci-hosts').symlink_to(root / 'missing')
            with self.assertRaisesRegex(ValueError, 'symlinks'):
                selected_records(None, root)

    def test_shared_data_bind_mounts_are_rejected_even_for_stopped_containers(self):
        runtime = MagicMock()
        runtime.invoke.return_value = 'foreign'
        for source in ('/data', '/data/disks', '/'):
            with self.subTest(source=source):
                runtime.inspect.return_value = dict(Name='/foreign', State=dict(Running=False),
                    Mounts=[dict(Type='bind', Source=source)])
                with self.assertRaisesRegex(ValueError, 'referenced by another container'):
                    check_data_users(runtime, self.record)
        runtime.inspect.return_value = dict(Name='/foreign', Mounts=[dict(Type='bind', Source='/unrelated')])
        check_data_users(runtime, self.record)
        runtime.check.assert_called_once_with(self.record)
