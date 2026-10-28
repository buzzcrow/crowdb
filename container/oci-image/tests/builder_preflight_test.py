# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Rootless startup restrictions fail before staging, with targeted host setup."""

import subprocess
import unittest
from unittest.mock import MagicMock, patch

from builder import preflight
from builder_host import apparmor_profile


class BuilderPreflightTest(unittest.TestCase):
    def setUp(self):
        self.patches = [patch('builder.platform.system', return_value='Linux'),
                        patch('builder.platform.machine', return_value='x86_64'),
                        patch('builder.tool', side_effect=lambda name: '/pixi/bin/' + name),
                        patch('builder.shutil.which', side_effect=lambda name: '/usr/bin/' + name),
                        patch('builder.subprocess.run', return_value=MagicMock(returncode=0)),
                        patch('builder.apparmor_denied', return_value=False),
                        patch('builder.setup_apparmor')]
        (self.system, self.machine, self.tool, self.which, self.run,
         self.denied, self.setup) = [item.start() for item in self.patches]
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])

    def test_rootless_preflight_exercises_the_bundled_executable_and_namespace_setup(self):
        preflight(False)
        self.run.assert_called_once_with(['/pixi/bin/rootlesskit', '--net=host', '--copy-up=/etc',
                                         '--copy-up=/run', '/bin/true'],
                                         text=True, capture_output=True, timeout=15)

    def test_denied_namespace_reports_complete_diagnostics_and_host_setup_entrypoint(self):
        self.run.return_value = MagicMock(returncode=1, stdout='uid-map diagnostic\n',
            stderr='apparmor_restrict_unprivileged_userns is set to 1\nfork/exec: permission denied\n')
        with self.assertRaises(RuntimeError) as failure:
            preflight(False)
        message = str(failure.exception)
        for expected in ('fix host prerequisites before building', 'uid-map diagnostic',
                         'permission denied', 'pixi run oci-build', 'sudo authentication'):
            self.assertIn(expected, message)
        self.setup.assert_not_called()

    def test_host_setup_repairs_only_an_apparmor_denial_then_verifies_namespace_startup(self):
        self.denied.return_value = True
        self.run.side_effect = [MagicMock(returncode=1, stdout='', stderr='apparmor denial'),
                                MagicMock(returncode=0)]
        preflight(False, setup_host=True)
        self.setup.assert_called_once_with('/pixi/bin/rootlesskit')
        self.assertEqual(self.run.call_count, 2)

    def test_healthy_host_needs_no_setup_or_sudo(self):
        preflight(False, setup_host=True)
        self.denied.assert_not_called()
        self.setup.assert_not_called()

    def test_other_host_failures_never_attempt_privileged_setup(self):
        self.run.return_value = MagicMock(returncode=1, stdout='', stderr='newuidmap: invalid mapping')
        with self.assertRaisesRegex(RuntimeError, 'invalid mapping'):
            preflight(False, setup_host=True)
        self.setup.assert_not_called()

    def test_failed_setup_does_not_repeat_probe_or_continue(self):
        self.denied.return_value = True
        self.run.return_value = MagicMock(returncode=1, stdout='', stderr='apparmor denial')
        self.setup.side_effect = RuntimeError('sudo: a password is required')
        with self.assertRaisesRegex(RuntimeError, 'password is required'):
            preflight(False, setup_host=True)
        self.assertEqual(self.run.call_count, 1)

    def test_persisting_denial_after_setup_is_reported_without_another_setup_attempt(self):
        self.denied.return_value = True
        self.run.return_value = MagicMock(returncode=1, stdout='', stderr='apparmor denial')
        with self.assertRaisesRegex(RuntimeError, 'apparmor denial'):
            preflight(False, setup_host=True)
        self.setup.assert_called_once()
        self.assertEqual(self.run.call_count, 2)

    def test_probe_timeout_is_a_host_prerequisite_failure(self):
        self.run.side_effect = subprocess.TimeoutExpired('rootlesskit', 15)
        with self.assertRaisesRegex(RuntimeError, 'probe exceeded'):
            preflight(False)

    def test_explicit_privileged_build_does_not_probe_user_namespaces(self):
        with patch('builder.os.geteuid', return_value=0):
            preflight(True)
        self.run.assert_not_called()
        with patch('builder.os.geteuid', return_value=1000):
            with self.assertRaisesRegex(RuntimeError, 'explicitly administrator-started'):
                preflight(True)

    def test_missing_uid_helper_fails_before_probe(self):
        self.which.side_effect = None
        self.which.return_value = None
        with self.assertRaisesRegex(RuntimeError, 'newuidmap'):
            preflight(False)
        self.run.assert_not_called()

    def test_profile_grants_userns_to_only_the_pinned_binary_path(self):
        content = apparmor_profile('/pixi/bin/rootlesskit')
        self.assertIn('"/pixi/bin/rootlesskit" flags=(unconfined)', content)
        self.assertIn('  userns,', content)
        self.assertNotIn('/**', content)
        self.run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
