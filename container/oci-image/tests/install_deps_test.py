# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Dependency setup provisions the Linux host before continuing package installs."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[3] / 'tools/pixi-tasks/install-deps.sh'


class InstallDepsTest(unittest.TestCase):
    def execute(self, platform, fail_setup=False, installed_ui=False, fail_ui=False,
                bashrc=None, repeats=1):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / 'bin'
            binaries.mkdir()
            if bashrc is not None:
                (root / '.bashrc').write_text(bashrc)
            ui = root / 'app/crowdb-web/ui'
            ui.mkdir(parents=True)
            if installed_ui:
                (ui / 'node_modules').mkdir()
            log = root / 'commands'
            for name in ('git', 'cargo', 'pixi', 'uname', 'npm'):
                stub = binaries / name
                stub.write_text('#!/bin/sh\n'
                    + ('printf "%s\\n" "$TEST_PLATFORM"\n' if name == 'uname' else
                       f'printf "%s\\n" "{name} $*" >> "$TEST_COMMANDS"\n'
                       'case "$*" in\n'
                       '  *--setup-apparmor*) exit "$TEST_SETUP_EXIT" ;;\n'
                       f'  ci) exit "$TEST_UI_EXIT" ;;\n'
                       'esac\n'))
                stub.chmod(0o755)
            environment = dict(os.environ, HOME=str(root), PIXI_PROJECT_ROOT=str(root), TEST_PLATFORM=platform,
                TEST_COMMANDS=str(log), TEST_SETUP_EXIT='1' if fail_setup else '0',
                TEST_UI_EXIT='1' if fail_ui else '0',
                PATH=str(binaries) + os.pathsep + os.environ['PATH'])
            for _ in range(repeats):
                result = subprocess.run(['bash', str(SCRIPT)], env=environment,
                                        text=True, capture_output=True, timeout=10)
                if result.returncode:
                    break
            return result, log.read_text().splitlines(), (root / '.bashrc').read_text()

    def test_linux_sets_up_host_before_ui_and_cargo_dependencies(self):
        result, commands, _ = self.execute('Linux x86_64')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('preflight.py --setup-apparmor', commands[1])
        self.assertEqual(commands[2], 'npm ci')
        self.assertTrue(any(command.startswith('cargo install') for command in commands))

    def test_macos_does_not_select_the_linux_only_oci_environment(self):
        result, commands, _ = self.execute('Darwin arm64')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(any('--setup-apparmor' in command for command in commands))
        self.assertIn('npm ci', commands)

    def test_existing_ui_dependencies_do_not_repeat_npm_installation(self):
        result, commands, _ = self.execute('Linux x86_64', installed_ui=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('npm ci', commands)
        self.assertTrue(any(command.startswith('cargo install') for command in commands))

    def test_failed_frontend_install_stops_before_profiling_tools(self):
        result, commands, _ = self.execute('Linux x86_64', fail_ui=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(commands[-1], 'npm ci')
        self.assertFalse(any(command.startswith('cargo install') for command in commands))

    def test_host_setup_failure_stops_before_other_dependency_installs(self):
        result, commands, _ = self.execute('Linux x86_64', fail_setup=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(commands), 2)
        self.assertIn('--setup-apparmor', commands[-1])

    def test_installs_completion_without_overwriting_existing_shell_configuration(self):
        original = 'export PROJECT_SETTING=custom\nalias ll="ls -l"\n'
        result, _, bashrc = self.execute('Linux x86_64', bashrc=original)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(bashrc.startswith(original))
        self.assertIn('source <(pixi completion --shell bash)', bashrc)

    def test_repeated_dependency_setup_does_not_duplicate_completion(self):
        result, _, bashrc = self.execute('Linux x86_64', repeats=2)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(bashrc.count('pixi completion --shell bash'), 1)

    def test_existing_completion_is_preserved_without_adding_another_block(self):
        for source in ('source <(pixi completion --shell bash)', '. <(pixi completion -s bash)'):
            with self.subTest(source=source):
                original = 'export PROJECT_SETTING=custom\n' + source + '\n'
                result, _, bashrc = self.execute('Linux x86_64', bashrc=original)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(bashrc, original)


if __name__ == '__main__':
    unittest.main()
