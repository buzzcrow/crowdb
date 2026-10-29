# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Unified build and clean entry points preserve ordering and scoped cleanup."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[3]


class TaskEntrypointsTest(unittest.TestCase):
    def execute(self, script, args=(), fail_step=''):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binaries = root / 'bin'
            binaries.mkdir()
            tasks = root / 'tools/pixi-tasks'
            tasks.mkdir(parents=True)
            (root / 'app/crowdb-web/ui').mkdir(parents=True)
            shutil.copy(ROOT / 'tools/pixi-tasks/build-cpp.sh', tasks / 'build-cpp.sh')
            (tasks / 'clean-env.sh').write_text('#!/bin/bash\n'
                'printf "%s\\n" runtime-clean >> "$TEST_COMMANDS"\n'
                '[[ "$TEST_FAIL_STEP" != runtime-clean ]]\n')
            log = root / 'commands'
            for name in ('cmake', 'cargo', 'npm', 'sudo', 'ps'):
                stub = binaries / name
                content = '#!/bin/sh\n'
                if name != 'ps':
                    content += f'printf "%s\\n" "{name} $*" >> "$TEST_COMMANDS"\n'
                    content += f'[ "$TEST_FAIL_STEP" != "{name}" ]\n'
                stub.write_text(content)
                stub.chmod(0o755)
            environment = dict(os.environ, PIXI_PROJECT_ROOT=str(root), TEST_COMMANDS=str(log),
                TEST_FAIL_STEP=fail_step, PATH=str(binaries) + os.pathsep + os.environ['PATH'])
            result = subprocess.run(['bash', str(ROOT / 'tools/pixi-tasks' / script), *args],
                env=environment, text=True, capture_output=True, timeout=10)
            return result, log.read_text().splitlines() if log.exists() else []

    def test_build_compiles_cpp_release_frontend_and_tests_without_running_tests(self):
        result, commands = self.execute('build-release.sh')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(commands[0].startswith('cmake -S lib/crowdb-tree'))
        self.assertIn('cargo build --release --workspace --exclude crowdb-kv-client', commands)
        self.assertIn('cargo build --release -p crowdb-kv-client --features ffi', commands)
        self.assertIn('npm run build', commands)
        self.assertEqual(commands[-1], 'cargo test --workspace --all-targets --no-run')

    def test_cpp_failure_stops_before_other_build_steps(self):
        result, commands = self.execute('build-release.sh', fail_step='cmake')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(commands), 1)

    def test_runtime_only_clean_never_removes_build_artifacts(self):
        result, commands = self.execute('clean-build.sh', ['--runtime-only'])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(commands, ['runtime-clean'])

    def test_full_clean_stops_recorded_processes_before_deleting_state_and_artifacts(self):
        result, commands = self.execute('clean-build.sh')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(commands[0], 'runtime-clean')
        self.assertTrue(any(command.startswith('sudo ') and command.endswith('cargo clean')
                            for command in commands))

    def test_failed_runtime_cleanup_prevents_artifact_deletion(self):
        result, commands = self.execute('clean-build.sh', fail_step='runtime-clean')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(commands, ['runtime-clean'])

    def test_unknown_clean_options_fail_before_effects(self):
        result, commands = self.execute('clean-build.sh', ['--unknown'])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(commands, [])

    def test_manifest_exposes_unified_tasks_and_has_no_stale_dependency_names(self):
        tasks = tomllib.loads((ROOT / 'pixi.toml').read_text())['tasks']
        self.assertTrue({'install-deps', 'clean', 'build'} <= tasks.keys())
        self.assertFalse({'install-ui-deps', 'clean-env', 'build-cpp', 'build-tests'} & tasks.keys())
        for name, task in tasks.items():
            if isinstance(task, dict):
                for dependency in task.get('depends-on', []):
                    self.assertIn(dependency, tasks, name)


if __name__ == '__main__':
    unittest.main()
