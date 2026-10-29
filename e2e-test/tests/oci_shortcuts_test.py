# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from types import SimpleNamespace
import unittest
from unittest.mock import patch

import oci_shortcuts


class OciShortcutRetentionTest(unittest.TestCase):
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
