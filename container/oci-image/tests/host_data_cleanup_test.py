# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host data deletion retains supplied media and rejects changed path ownership."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import host_cli_test as fixture
from host_data_cleanup import HostDataCleanup


class HostDataCleanupTest(unittest.TestCase):
    def setUp(self):
        fixture.HostCliTest.setUp(self)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.runtime_root = self.root / 'runtime'
        self.data = self.root / 'data'
        self.data.mkdir()
        self.record['options']['data_root'] = str(self.data)
        self.receipt = self.runtime_root / 'persistent/oci-hosts/test.json'
        self.receipt.parent.mkdir(parents=True)
        self.binding = self.runtime_root / 'persistent/clusters/test/target.json'
        self.binding.parent.mkdir(parents=True)
        self.binding.write_text(json.dumps(dict(version=1, name='test',
            management_endpoints=self.record['origins'],
            runtime=dict(selection=self.record['runtime']['selection'], owner=self.record['owner']))))
        self.binding.chmod(0o600)
        self.seal()

    def seal(self):
        self.receipt.write_text(json.dumps(self.record))
        self.receipt.chmod(0o600)
        (self.data / 'container-storage.json').write_text(json.dumps(dict(version=1, disks=self.record['disks'])))

    def test_removes_node_data_and_retires_matching_receipts(self):
        (self.data / 'nested').mkdir()
        (self.data / 'nested/state').write_text('node data')
        HostDataCleanup(self.record, self.runtime_root).remove()
        self.assertFalse(self.data.exists())
        self.assertFalse(self.receipt.exists())
        self.assertFalse(self.binding.exists())

    def test_preserves_supplied_media_inside_and_outside_data_root_and_symlink_targets(self):
        external = self.root / 'external.img'
        external.write_text('supplied external disk')
        internal = self.data / 'disks/supplied.img'
        internal.parent.mkdir()
        internal.write_text('supplied internal disk')
        (self.data / 'state').write_text('node data')
        (self.data / 'link').symlink_to(external)
        self.record['disks'] = [dict(source=str(path)) for path in (external, internal, Path('/dev/not-opened'))]
        self.seal()
        HostDataCleanup(self.record, self.runtime_root).remove()
        self.assertEqual(external.read_text(), 'supplied external disk')
        self.assertEqual(internal.read_text(), 'supplied internal disk')
        self.assertEqual(list(self.data.iterdir()), [internal.parent])
        self.assertFalse(self.receipt.exists())

    def test_missing_data_directory_still_retires_ownership(self):
        (self.data / 'container-storage.json').unlink()
        self.data.rmdir()
        HostDataCleanup(self.record, self.runtime_root).remove()
        self.assertFalse(self.receipt.exists())
        self.assertFalse(self.binding.exists())

    def test_nonempty_unmarked_directory_is_rejected_without_effects(self):
        (self.data / 'container-storage.json').unlink()
        foreign = self.data / 'foreign'
        foreign.write_text('unowned')
        with self.assertRaisesRegex(ValueError, 'no retained storage declaration'):
            HostDataCleanup(self.record, self.runtime_root)
        self.assertEqual(foreign.read_text(), 'unowned')
        self.assertTrue(self.receipt.exists())

    def test_changed_storage_declaration_or_directory_identity_is_rejected(self):
        plan = HostDataCleanup(self.record, self.runtime_root)
        old = self.root / 'old'
        self.data.rename(old)
        self.data.mkdir()
        self.seal()
        with self.assertRaisesRegex(ValueError, 'identity changed'):
            plan.remove()
        (self.data / 'container-storage.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'differs from accepted media'):
            HostDataCleanup(self.record, self.runtime_root)
        self.assertTrue(self.receipt.exists())

    def test_symlinked_root_or_nested_mount_is_rejected(self):
        old = self.root / 'old'
        self.data.rename(old)
        self.data.symlink_to(old)
        with self.assertRaisesRegex(ValueError, 'without symlinks'):
            HostDataCleanup(self.record, self.runtime_root)
        self.data.unlink()
        old.rename(self.data)
        nested = self.data / 'mounted'
        nested.mkdir()
        with patch('host_data_cleanup.mount_paths', return_value={nested}):
            with self.assertRaisesRegex(ValueError, 'nested mount'):
                HostDataCleanup(self.record, self.runtime_root)
        self.assertTrue(self.receipt.exists())

    def test_runtime_root_or_shared_data_directory_is_rejected(self):
        original = self.record['options']['data_root']
        self.record['options']['data_root'] = str(self.root)
        with self.assertRaisesRegex(ValueError, 'overlaps runtime receipts'):
            HostDataCleanup(self.record, self.runtime_root)
        self.record['options']['data_root'] = original
        other = dict(self.record, cluster='other', name='crowdb-other-host')
        path = self.receipt.with_name('other.json')
        path.write_text(json.dumps(other))
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'overlap between recorded clusters'):
            HostDataCleanup(self.record, self.runtime_root)

    def test_failed_deletion_keeps_storage_marker_and_receipts_for_retry(self):
        (self.data / 'busy').write_text('data')
        plan = HostDataCleanup(self.record, self.runtime_root)
        unlink = os.unlink

        def fail(path, *args, **kwargs):
            if path == 'busy':
                raise PermissionError('busy file')
            return unlink(path, *args, **kwargs)

        with patch('host_data_cleanup.os.unlink', side_effect=fail):
            with self.assertRaisesRegex(PermissionError, 'busy file'):
                plan.remove()
        self.assertTrue((self.data / 'container-storage.json').exists())
        self.assertTrue(self.receipt.exists())
        self.assertTrue(self.binding.exists())
        HostDataCleanup(self.record, self.runtime_root).remove()
        self.assertFalse(self.data.exists())


if __name__ == '__main__':
    unittest.main()
