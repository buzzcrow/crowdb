# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Garbage cleanup preserves live references and retained cluster resources."""

import contextlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from docker_garbage import collect
from docker_local_state import OWNER
from runtime import OWNER as HOST_OWNER


class DockerGarbageTest(unittest.TestCase):
    def setUp(self):
        self.docker = MagicMock()
        self.runtime = dict(daemon='local', context='default', endpoint='unix:///docker.sock')
        self.docker.identity.return_value = self.runtime
        self.items = dict(container={}, network={}, volume={})
        self.used_volumes = set()
        self.removed = []
        self.docker.objects.side_effect = lambda kind, *args: list(self.items[kind].values())
        self.docker.call.side_effect = self.call

    def call(self, *args):
        if args[1] == 'inspect':
            return json.dumps([self.items[args[0]][args[2]]])
        if args[:2] == ('ps', '-aq'):
            return 'live-user' if args[-1].removeprefix('volume=') in self.used_volumes else ''
        if args[:2] == ('builder', 'prune'):
            return 'Total reclaimed space: 0B'
        self.removed.append(args)
        return ''

    def container(self, identity, status='exited', owner=None):
        item = dict(Id=identity, State=dict(Running=status in ('running', 'paused', 'restarting'),
                                           Status=status), Config=dict(Labels={OWNER: owner}))
        self.items['container'][identity] = item
        return item

    def volume(self, name, owner=None):
        self.items['volume'][name] = dict(Name=name, Labels={OWNER: owner})

    def network(self, name, attached=False, owner=None):
        self.items['network'][name] = dict(Id=name, Name=name, Labels={OWNER: owner},
                                          Containers=dict(live={}) if attached else {})

    def collect(self, records=(), hosts=(), **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            collect(self.docker, records, hosts, runtime=self.runtime, **kwargs)

    def test_removes_stopped_containers_before_unused_named_and_anonymous_volumes(self):
        for status in ('created', 'exited', 'dead', 'running', 'paused', 'restarting'):
            self.container(status, status)
        self.volume('anonymous')
        self.volume('named-data')
        self.volume('running-data')
        self.used_volumes.add('running-data')
        self.network('old-bridge')
        self.network('live-bridge', attached=True)
        self.collect()
        self.assertEqual(self.removed, [('rm', 'created'), ('rm', 'exited'), ('rm', 'dead'),
                                       ('network', 'rm', 'old-bridge'), ('volume', 'rm', 'anonymous'),
                                       ('volume', 'rm', 'named-data')])
        self.docker.call.assert_any_call('builder', 'prune', '--all', '--force')
        self.assertFalse(any(item.args[0] in ('stop', 'kill') for item in self.docker.call.call_args_list))

    def test_retained_cluster_keeps_stopped_members_and_detached_resources(self):
        records = [dict(cluster=name, owner=name, nodes=1) for name in ('first', 'second')]
        for record in records:
            self.container(record['cluster'], owner=record['owner'])
            self.volume('crowdb-' + record['cluster'] + '-1-data')
            self.volume('extra-' + record['cluster'], owner=record['owner'])
            self.network('crowdb-' + record['cluster'])
        host = self.container('host-stopped')
        host['Config']['Labels'] = {HOST_OWNER: 'host-owner'}
        self.collect(records, [dict(owner='host-owner')])
        self.assertEqual(self.removed, [])

    def test_keep_data_preserves_all_volumes_but_removes_stopped_containers(self):
        self.container('old')
        self.volume('unused-data')
        self.collect(delete_data=False)
        self.assertEqual(self.removed, [('rm', 'old')])
        self.assertFalse(any(item.args[0] == 'volume' for item in self.docker.call.call_args_list))

    def test_default_networks_survive_even_if_reported_as_unused(self):
        for name in ('bridge', 'host', 'none'):
            self.network(name)
        self.collect()
        self.assertEqual(self.removed, [])

    def test_restarted_container_is_skipped_after_fresh_inspection(self):
        old = self.container('restarted')
        self.docker.objects.side_effect = lambda kind, *args: [old] if kind == 'container' else []
        self.items['container']['restarted'] = dict(old, State=dict(Running=True, Status='running'))
        self.collect()
        self.assertEqual(self.removed, [])

    def test_docker_reference_race_failure_is_reported_without_force_or_cache_prune(self):
        self.volume('newly-used')
        original = self.call

        def call(*args):
            if args[:2] == ('volume', 'rm'):
                raise RuntimeError('volume is in use')
            return original(*args)

        self.docker.call.side_effect = call
        with self.assertRaisesRegex(RuntimeError, 'volume is in use'):
            self.collect()
        self.docker.call.assert_any_call('volume', 'rm', 'newly-used')
        self.assertFalse(any(item.args[0] == 'builder' for item in self.docker.call.call_args_list))

    def test_context_change_before_removal_has_no_effects(self):
        self.container('old')
        self.docker.identity.side_effect = [self.runtime, dict(daemon='foreign')]
        with self.assertRaisesRegex(ValueError, 'endpoint/context changed'):
            self.collect()
        self.assertEqual(self.removed, [])
        self.assertFalse(any(item.args[0] == 'builder' for item in self.docker.call.call_args_list))


if __name__ == '__main__':
    unittest.main()
