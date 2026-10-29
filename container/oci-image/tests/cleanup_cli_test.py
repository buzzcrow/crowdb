# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Clean command defaults select all receipts and remove owned data."""

import contextlib
import importlib.util
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, call, patch

OCI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OCI))


def entrypoint(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), OCI / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cleanup = entrypoint('clean')


class CleanupCliTest(unittest.TestCase):
    def setUp(self):
        self.docker = MagicMock()
        self.records = [dict(cluster=name, runtime=dict(daemon='owned')) for name in ('first', 'second')]
        self.hosts = [dict(cluster='host', name='crowdb-host-host', options=dict(runtime='containerd'))]
        self.patches = [patch.object(cleanup, 'Docker', return_value=self.docker),
                        patch.object(cleanup, 'selected', return_value=self.records),
                        patch.object(cleanup, 'check_daemon'), patch.object(cleanup, 'clean'),
                        patch.object(cleanup, 'selected_records', return_value=self.hosts),
                        patch.object(cleanup, 'image_garbage'), patch.object(cleanup, 'load'),
                        patch.object(cleanup, 'HostRuntime'), patch.object(cleanup, 'HostDataCleanup'),
                        patch.object(cleanup, 'clean_host'), patch.object(cleanup, 'require_owned'),
                        patch.object(cleanup.shutil, 'which', return_value='/docker'),
                        patch.object(cleanup, 'check_data_users')]
        (self.factory, self.select, self.check, self.clean, self.select_hosts, self.garbage,
         self.load, self.runtime, self.data, self.clean_host, self.require, self.which, self.users) = [
            item.start() for item in self.patches]
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])

    def test_bare_clean_selects_all_and_deletes_data_before_image_garbage(self):
        cleanup.main([])
        self.select.assert_called_once_with(None, True, cleanup.RECORDS)
        self.assertEqual(self.clean.call_args_list,
                         [call(self.docker, record, True, cleanup.RECORDS) for record in self.records])
        self.clean_host.assert_called_once_with(self.hosts[0])
        self.data.return_value.remove.assert_called_once_with()
        self.garbage.assert_called_once_with(self.docker, cleanup.RECORDS, runtime=self.records[0]['runtime'])

    def test_explicit_cluster_and_keep_data(self):
        self.load.return_value = self.records[0]
        self.select_hosts.return_value = []
        cleanup.main(['--cluster', 'first', '--keep-data'])
        self.select.assert_not_called()
        self.load.assert_called_once_with('first', cleanup.RECORDS)
        self.select_hosts.assert_called_once_with('first', cleanup.RECORDS.parents[1])
        self.clean.assert_called_once_with(self.docker, self.records[0], False, cleanup.RECORDS)
        self.data.assert_not_called()

    def test_explicit_all_and_delete_data(self):
        cleanup.main(['--all', '--delete-data'])
        self.select.assert_called_once_with(None, True, cleanup.RECORDS)
        self.assertEqual(self.clean.call_args_list,
                         [call(self.docker, record, True, cleanup.RECORDS) for record in self.records])

    def test_empty_inventory_still_checks_image_garbage(self):
        self.select.return_value = []
        self.select_hosts.return_value = []
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            cleanup.main([])
        self.assertIn('No recorded CROWDB clusters', output.getvalue())
        self.clean.assert_not_called()
        self.check.assert_not_called()
        self.garbage.assert_called_once_with(self.docker, cleanup.RECORDS, runtime=self.docker.identity.return_value)

    def test_containerd_only_cleanup_needs_no_docker_daemon(self):
        self.select.return_value = []
        cleanup.main([])
        self.factory.assert_not_called()
        self.garbage.assert_not_called()
        self.clean_host.assert_called_once_with(self.hosts[0])
        self.data.return_value.remove.assert_called_once_with()

    def test_host_keep_data_preserves_restart_receipts(self):
        self.load.return_value = None
        cleanup.main(['--cluster', 'host', '--keep-data'])
        self.clean_host.assert_called_once_with(self.hosts[0])
        self.data.assert_not_called()

    def test_host_docker_endpoint_does_not_trigger_image_cleanup_on_current_docker_context(self):
        self.select.return_value = []
        self.hosts[0]['options']['runtime'] = 'docker'
        cleanup.main([])
        self.factory.assert_not_called()
        self.clean_host.assert_called_once_with(self.hosts[0])
        self.garbage.assert_not_called()

    def test_missing_explicit_cluster_is_an_error_before_runtime_access(self):
        self.load.return_value = None
        self.select_hosts.return_value = []
        with self.assertRaisesRegex(ValueError, "cluster 'missing' has no ownership receipt"):
            cleanup.main(['--cluster', 'missing'])
        self.factory.assert_not_called()
        self.runtime.assert_not_called()

    def test_all_runtime_identities_are_checked_before_cleanup(self):
        self.check.side_effect = [None, ValueError('changed daemon')]
        with self.assertRaisesRegex(ValueError, 'changed daemon'):
            cleanup.main([])
        self.clean.assert_not_called()
        self.garbage.assert_not_called()

    def test_host_data_validation_fails_before_any_cluster_effects(self):
        self.data.side_effect = ValueError('changed data directory')
        with self.assertRaisesRegex(ValueError, 'changed data directory'):
            cleanup.main([])
        self.clean.assert_not_called()
        self.clean_host.assert_not_called()
        self.garbage.assert_not_called()

    def test_data_and_scope_options_reject_conflicts_before_runtime_access(self):
        cases = [['--all', '--cluster', 'first'], ['--keep-data', '--delete-data']]
        for argv in cases:
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                cleanup.main(argv)
        self.factory.assert_not_called()


if __name__ == '__main__':
    unittest.main()
