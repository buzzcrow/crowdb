# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host profile setup authenticates through sudo and stays scoped to one binary."""

from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from builder_host import apparmor_denied, apparmor_profile, profile_name, setup_apparmor


class BuilderHostTest(unittest.TestCase):
    def test_only_the_matching_kernel_restriction_and_fork_denial_select_setup(self):
        denied = 'apparmor_restrict_unprivileged_userns is set to 1; fork/exec /proc/self/exe: permission denied'
        with (patch('builder_host.Path.is_file', return_value=True),
              patch('builder_host.Path.read_text', return_value='1\n')):
            self.assertTrue(apparmor_denied(denied))
            self.assertFalse(apparmor_denied('newuidmap: permission denied'))
            self.assertFalse(apparmor_denied('apparmor_restrict_unprivileged_userns: another failure'))
        with (patch('builder_host.Path.is_file', return_value=True),
              patch('builder_host.Path.read_text', return_value='0\n')):
            self.assertFalse(apparmor_denied(denied))

    def test_checkout_paths_get_distinct_exact_profiles_and_escape_globs(self):
        self.assertNotEqual(profile_name('/checkout-one/rootlesskit'), profile_name('/checkout-two/rootlesskit'))
        profile = apparmor_profile('/checkout[one]/bin/rootlesskit')
        self.assertIn('"/checkout\\[one\\]/bin/rootlesskit"', profile)
        self.assertNotIn('"/checkout[one]', profile)

    def install(self, interactive, returncodes=(0, 0, 0)):
        commands = []

        def run(command, **kwargs):
            commands.append(command)
            if len(commands) == 1:
                content = Path(command[-1]).read_text()
                self.assertIn('"/pixi/bin/rootlesskit" flags=(unconfined)', content)
                self.assertIn('  userns,', content)
            self.assertNotIn('input', kwargs)
            self.assertNotIn('capture_output', kwargs)
            return MagicMock(returncode=returncodes[len(commands) - 1])

        with (patch('builder_host.shutil.which', side_effect=lambda name: '/usr/bin/' + name),
              patch('builder_host.os.geteuid', return_value=1000),
              patch('builder_host.sys.stdin.isatty', return_value=interactive),
              patch('builder_host.subprocess.run', side_effect=run)):
            setup_apparmor('/pixi/bin/rootlesskit')
        return commands

    def test_interactive_setup_uses_terminal_sudo_after_validating_profile(self):
        commands = self.install(True)
        target = '/etc/apparmor.d/' + profile_name('/pixi/bin/rootlesskit')
        self.assertEqual(commands[0][:3], ['/usr/bin/apparmor_parser', '--skip-kernel-load', '--skip-cache'])
        self.assertEqual(commands[1][:5], ['/usr/bin/sudo', '--', '/usr/bin/install', '-m', '0644'])
        self.assertEqual(commands[1][-1], target)
        self.assertEqual(commands[2], ['/usr/bin/sudo', '--', '/usr/bin/apparmor_parser', '-r', target])

    def test_unattended_setup_never_waits_for_password_input(self):
        commands = self.install(False)
        self.assertEqual(commands[1][:3], ['/usr/bin/sudo', '-n', '--'])
        self.assertEqual(commands[2][:3], ['/usr/bin/sudo', '-n', '--'])

    def test_profile_validation_and_installation_failures_stop_the_sequence(self):
        for codes in ((1,), (0, 1), (0, 0, 1)):
            with self.subTest(codes=codes), self.assertRaisesRegex(RuntimeError, 'AppArmor profile setup failed'):
                self.install(False, codes)


if __name__ == '__main__':
    unittest.main()
