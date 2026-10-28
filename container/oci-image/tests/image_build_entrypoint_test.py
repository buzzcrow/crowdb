# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""The OCI shortcut loads Docker by default and supports Docker-free archives."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'image-build.sh'


class ImageBuildEntrypointTest(unittest.TestCase):
    def execute(self, args=(), image='crowdb-node:dev', fail_step=''):
        with tempfile.TemporaryDirectory(prefix='crowdb image-') as temporary:
            root = Path(temporary)
            binaries = root / 'bin'
            binaries.mkdir()
            scripts = root / 'container/oci-image'
            scripts.mkdir(parents=True)
            (scripts / 'stage.sh').write_text('#!/bin/sh\nprintf "%s\\n" stage >> "$TEST_COMMANDS"\n')
            log = root / 'commands'
            stubs = {
                'git': 'printf "%s\\n" "$TEST_ROOT"\n',
                'uname': 'printf "%s\\n" "Linux x86_64"\n',
                'docker': 'printf "%s\\n" "docker $*" >> "$TEST_COMMANDS"\n'
                          '[ "$TEST_FAIL_STEP" != docker ]\n',
                'pixi': 'printf "%s\\n" "pixi $*" >> "$TEST_COMMANDS"\n'
                        'case " $* " in\n'
                        '  *" build "*) [ "$TEST_FAIL_STEP" != build ] ;;\n'
                        '  *" import "*) [ "$TEST_FAIL_STEP" != import ] ;;\n'
                        'esac\n',
            }
            for name, content in stubs.items():
                path = binaries / name
                path.write_text('#!/bin/sh\n' + content)
                path.chmod(0o755)
            environment = dict(os.environ, TEST_ROOT=str(root), TEST_COMMANDS=str(log),
                TEST_FAIL_STEP=fail_step, CROWDB_CONTAINER_IMAGE=image,
                PATH=str(binaries) + os.pathsep + os.environ['PATH'])
            result = subprocess.run(['bash', str(SCRIPT), *args], env=environment,
                                    text=True, capture_output=True, timeout=10)
            return result, log.read_text().splitlines()

    def test_default_build_loads_the_validated_archive_after_construction(self):
        result, commands = self.execute()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(commands[0].startswith('docker info'))
        self.assertIn('preflight.py --setup-apparmor', commands[1])
        self.assertEqual(commands[2], 'stage')
        self.assertIn(' build --context ', commands[3])
        self.assertIn(' import ', commands[4])
        self.assertTrue(commands[4].endswith('/target/crowdb.oci.tar --image crowdb-node:dev'))
        self.assertIn('loaded crowdb-node:dev into Docker', result.stdout)

    def test_archive_only_build_never_accesses_docker_or_imports(self):
        result, commands = self.execute(['--no-load'], fail_step='docker')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(any(command.startswith('docker ') or ' import ' in command for command in commands))
        self.assertTrue(any(' build --context ' in command for command in commands))
        self.assertIn('wrote OCI archive:', result.stdout)

    def test_environment_image_and_explicit_image_are_loaded(self):
        for args, expected in (([], 'crowdb-node:custom'), (['--load-image', 'crowdb-node:chosen'], 'crowdb-node:chosen')):
            with self.subTest(args=args):
                result, commands = self.execute(args, image='crowdb-node:custom')
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertTrue(commands[-1].endswith('--image ' + expected))

    def test_explicit_load_still_works_and_staged_skips_compilation(self):
        result, commands = self.execute(['--load', '--staged'])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('stage', commands)
        self.assertIn(' import ', commands[-1])

    def test_unavailable_docker_fails_before_compilation(self):
        result, commands = self.execute(fail_step='docker')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(commands), 1)
        self.assertTrue(commands[0].startswith('docker info'))

    def test_failed_construction_never_loads_an_old_archive(self):
        result, commands = self.execute(fail_step='build')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(' import ' in command for command in commands))

    def test_failed_import_does_not_report_success(self):
        result, commands = self.execute(fail_step='import')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(' import ', commands[-1])
        self.assertNotIn('loaded ', result.stdout)


if __name__ == '__main__':
    unittest.main()
