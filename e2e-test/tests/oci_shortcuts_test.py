# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from types import SimpleNamespace
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import oci_shortcuts


class OciShortcutRetentionTest(unittest.TestCase):
    def test_cleanup_only_selects_owned_receipts_after_success_or_failure(self):
        for fail in (False, True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as directory:
                artifacts = Path(directory)
                receipts = artifacts / 'shortcut-runtime/persistent/oci-clusters'
                foreign = receipts / 'unrelated.json'
                running, cleaned = {}, []

                def invoke(command, **kwargs):
                    receipts.mkdir(parents=True, exist_ok=True)
                    if not foreign.exists():
                        foreign.write_text('unrelated retained data')
                    task, arguments = command[2], command[3:]
                    output, code = '', 0
                    if task == 'sh':
                        output = kwargs['env']['PATH'].split(':')[0] + '/crowdb-cli\n'
                    else:
                        self.assertNotIn('--all', arguments, 'fixture must never clean the shared daemon')
                        name = arguments[arguments.index('--cluster') + 1]
                        if task == 'oci-run':
                            code = 1
                        elif task == 'oci-run-3c':
                            (receipts / (name + '.json')).write_text('owned')
                            running[name] = ['first', 'second', 'third']
                        elif task == 'oci-clean':
                            cleaned.append((name, '--keep-data' in arguments))
                            self.assertEqual((receipts / (name + '.json')).read_text(), 'owned')
                            running.pop(name, None)
                            if '--keep-data' not in arguments:
                                (receipts / (name + '.json')).unlink()
                        else:
                            self.fail(task)
                    return subprocess.CompletedProcess(command, code, output, '')

                def named(_cluster, _invoke, _ready, _ids, _credentials, one, other, _port):
                    if fail:
                        for name in (one, other):
                            (receipts / (name + '.json')).write_text('owned')
                        raise RuntimeError('injected preparation failure')
                    return 'one-id', 'other-id', []

                cluster = SimpleNamespace(artifacts=artifacts, run_id='fixture-123456789012',
                    image='owned-image', image_id='owned-image-id', port_base=20000,
                    command=lambda *args: '\n'.join(running.get(args[-1].rsplit('=', 1)[1], [])))
                ready = dict(phase='ready', observation=dict(policy=dict(mode='local'),
                    initial_voters=3, cluster_id='three-id'))
                with patch.object(oci_shortcuts.subprocess, 'run', side_effect=invoke), \
                     patch.object(oci_shortcuts, 'named_clusters', side_effect=named), \
                     patch.object(oci_shortcuts, 'status', return_value=ready):
                    if fail:
                        with self.assertRaisesRegex(RuntimeError, 'injected preparation failure'):
                            oci_shortcuts.verify(cluster)
                    else:
                        oci_shortcuts.verify(cluster)
                self.assertEqual(foreign.read_text(), 'unrelated retained data')
                expected = [('pixi-123456789012-' + suffix, fail)
                            for suffix in (('a', 'b') if fail else ('c',))]
                self.assertEqual(cleaned, expected)

    def test_named_cluster_recreation_preserves_identity_ports_credentials_and_writes(self):
        saved, running, generations, racks = {}, {}, {}, {}

        def invoke(task, *arguments):
            name = arguments[arguments.index('--cluster') + 1]
            if task in ('oci-run-1c', 'oci-run'):
                if name not in saved:
                    port = int(arguments[arguments.index('--port-base') + 1])
                    saved[name] = (port, name + '-identity', name + '-credentials')
                    racks[name] = []
                generations[name] = generations.get(name, 0) + 1
                running[name] = [name + str(generations[name])]
            elif task == 'oci-stop':
                pass
            elif task == 'oci-clean':
                running.pop(name, None)
                if '--keep-data' not in arguments:
                    saved.pop(name, None)
                    racks.pop(name, None)
            else:
                self.fail(task)

        def selected(offset):
            return next(name for name, value in saved.items() if value[0] == 20000 + offset)

        def ready(offset):
            name = selected(offset)
            self.assertIn(name, running)
            return {'observation': {'cluster_id': saved[name][1], 'initial_voters': 1}}

        def request(origin, _path, body=None):
            name = selected(int(origin.rsplit(':', 1)[1]) - 20000)
            if body is not None:
                racks[name].append(body)
            return racks[name]

        with patch.object(oci_shortcuts, 'request', side_effect=request):
            result = oci_shortcuts.named_clusters(
                SimpleNamespace(image='owned-image'), invoke, ready,
                lambda name: running.get(name, []),
                lambda offset: saved[selected(offset)][2], 'one', 'other', 20000)
        self.assertEqual(result[:2], ('one-identity', 'other-identity'))
        self.assertEqual(saved, {})
        self.assertEqual(running, {})
