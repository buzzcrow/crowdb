# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Interrupted host launches resume only their sealed runtime and node inputs."""

import copy
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

OCI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OCI))
spec = importlib.util.spec_from_file_location('run_node', OCI / 'run-node.py')
run_node = importlib.util.module_from_spec(spec)
sys.modules['run_node'] = run_node
spec.loader.exec_module(run_node)
import host_lifecycle as lifecycle
from runtime import OWNER
from host_runtime import require_owned

DIGEST = 'sha256:' + 'a' * 64
OPTIONS = dict(runtime='containerd', network='host', snapshotter='native', runtime_endpoint=None,
               image=DIGEST, nodes=3, cpus=2, memory_mib=2048, data_root='/data',
               node_policy_file='/policy', password_file=None, device=[], disk_file=[],
               seed=[], physical_host_id='host-1', interface='eth0', management_address='10.0.0.1')
IDENTITY = dict(selection=dict(runtime='containerd', endpoint='/run/containerd/containerd.sock',
                               context='crowdb-test'), daemon='daemon-1', snapshotter='native', binary='/nerdctl')


def container(record):
    values = dict(CROWDB_CLUSTER='test', CROWDB_INITIAL_VOTERS='3', CROWDB_DEPLOYMENT_MODE='production',
                  CROWDB_PHYSICAL_HOST_ID='host-1', CROWDB_MANAGEMENT_INTERFACE='eth0',
                  CROWDB_NODE_POLICY_FILE='/run/crowdb-node-policy.toml', CROWDB_DISK_INPUTS='[]')
    return dict(Id='owned-container', Name='/crowdb-test-host', Image=DIGEST,
                Config=dict(Labels={OWNER: record['owner'], 'org.crowdb.cluster': 'test'},
                            Env=[key + '=' + value for key, value in values.items()]),
                State=dict(Running=True), HostConfig=dict(NetworkMode='host', CpuQuota=200000,
                    CpuPeriod=100000, Memory=2048*1024*1024), Mounts=[
                    dict(Destination='/opt/crowdb/data', Source='/data', RW=True),
                    dict(Destination='/run/crowdb-node-policy.toml', Source='/policy', RW=False)])


class TestRuntime:
    name = 'containerd'
    def __init__(self):
        self.current = None
        self.daemon = copy.deepcopy(IDENTITY)
        self.effects = []
        self.lose_reply = False
    def identity(self): return copy.deepcopy(self.daemon)
    def named(self, name): return self.current
    def inspect(self, name, image=False):
        if image:
            return dict(Id=DIGEST, Os='linux', Architecture='amd64', Config=dict(Labels={
                'org.opencontainers.image.source': 'https://github.com/buzzcrow/crowdb'}))
        return self.current
    def check(self, record):
        if self.daemon != record['runtime']: raise ValueError('runtime identity changed')
    def verify(self, *args): pass
    def invoke(self, action, identity):
        self.effects.append((action, identity))
        self.current['State']['Running'] = action == 'start'
    def create(self, args):
        self.effects.append(('create', args.name))
        self.current = container(dict(owner=args.owner_token))
        if self.lose_reply: raise RuntimeError('lost runtime reply')


class HostCliTest(unittest.TestCase):
    def setUp(self):
        self.runtime = TestRuntime()
        self.validated = ([], dict(node_policy='policy-hash', password=None), ['http://10.0.0.1:9090'])
        self.patches = [patch('host_lifecycle.HostRuntime', return_value=self.runtime),
                        patch('host_lifecycle.options', side_effect=lambda request, previous: copy.deepcopy(OPTIONS)),
                        patch('host_lifecycle.validate', side_effect=lambda record, running: copy.deepcopy(self.validated))]
        for item in self.patches: item.start()
        self.addCleanup(lambda: [item.stop() for item in reversed(self.patches)])
        self.request = dict(cluster='test', options={})
        self.record = lifecycle.prepare(self.request)
        self.request['record'] = self.record

    def test_unsealed_run_has_no_effects(self):
        with self.assertRaisesRegex(ValueError, 'seal accepted'):
            lifecycle.run(dict(cluster='test', options={}))
        self.assertEqual(self.runtime.effects, [])

    def test_lost_create_reply_resumes_same_owned_container(self):
        self.runtime.lose_reply = True
        with self.assertRaisesRegex(RuntimeError, 'lost runtime reply'):
            lifecycle.run(self.request)
        self.assertEqual(lifecycle.run(self.request)['container_id'], 'owned-container')
        self.assertEqual(self.runtime.effects, [('create', 'crowdb-test-host')])

    def test_daemon_change_blocks_start_and_stop(self):
        self.runtime.current = container(self.record)
        self.runtime.daemon['daemon'] = 'foreign'
        for action in (lifecycle.run, lifecycle.stop):
            with self.subTest(action=action.__name__), self.assertRaisesRegex(ValueError, 'runtime identity'):
                action(self.request)
        self.assertEqual(self.runtime.effects, [])

    def test_changed_policy_credentials_or_media_block_effects(self):
        for index, value in [(0, [dict(path='changed')]), (1, dict(node_policy='changed', password=None)),
                             (1, dict(node_policy='policy-hash', password='changed'))]:
            with self.subTest(value=value):
                current = list(self.validated)
                current[index] = value
                with patch('host_lifecycle.validate', return_value=tuple(current)):
                    with self.assertRaisesRegex(ValueError, 'differ from accepted'):
                        lifecycle.run(self.request)
        self.assertEqual(self.runtime.effects, [])

    def test_foreign_or_changed_stopped_container_is_never_started(self):
        for kind in ('owner', 'image', 'mount', 'memory', 'environment'):
            with self.subTest(kind=kind):
                current = container(self.record)
                current['State']['Running'] = False
                if kind == 'owner': current['Config']['Labels'][OWNER] = 'foreign'
                if kind == 'image': current['Image'] = 'foreign'
                if kind == 'mount': current['Mounts'][0]['Source'] = '/foreign'
                if kind == 'memory': current['HostConfig']['Memory'] = 1
                if kind == 'environment': current['Config']['Env'][0] = 'CROWDB_CLUSTER=foreign'
                self.runtime.current = current
                for action in (lifecycle.run, lifecycle.stop):
                    with self.assertRaises(ValueError): action(self.request)
        self.assertEqual(self.runtime.effects, [])

    def test_only_verified_running_owner_skips_port_check(self):
        for running in (True, False):
            self.runtime.current = container(self.record)
            self.runtime.current['State']['Running'] = running
            with patch('host_lifecycle.validate', return_value=self.validated) as validate:
                lifecycle.prepare(self.request)
                self.assertEqual(validate.call_args.args[1], running)
        self.runtime.current['Config']['Labels'][OWNER] = 'foreign'
        with patch('host_lifecycle.validate') as validate, self.assertRaises(ValueError):
            lifecycle.prepare(self.request)
        validate.assert_not_called()

    def test_stop_retains_container_and_inputs(self):
        self.runtime.current = container(self.record)
        saved = copy.deepcopy(self.record)
        self.assertEqual(lifecycle.stop(self.request), dict(data_preserved=True))
        self.assertEqual(self.runtime.effects, [('stop', 'owned-container')])
        self.assertEqual(self.record, saved)

    def test_diskless_nerdctl_null_device_list_preserves_owned_mount_checks(self):
        self.runtime.current = container(self.record)
        self.runtime.current['HostConfig']['Devices'] = None
        self.assertEqual(lifecycle.run(self.request)['container_id'], 'owned-container')
        self.runtime.current['Mounts'][0]['Source'] = '/foreign'
        with self.assertRaisesRegex(ValueError, 'mount differs'):
            lifecycle.run(self.request)
        self.assertEqual(self.runtime.effects, [])

    def test_raw_device_requires_exact_source_target_and_permissions(self):
        record = copy.deepcopy(self.record)
        record['disks'] = [dict(backend='block-device', source='/dev/vdf', path='/opt/crowdb/data/disks/vdf',
                               backing_identity='device:1', resource_identity='device:1')]
        current = container(record)
        declaration = [{key: value for key, value in record['disks'][0].items() if key != 'source'}]
        current['Config']['Env'][-1] = 'CROWDB_DISK_INPUTS=' + json.dumps(declaration)
        device = dict(PathOnHost='/dev/vdf', PathInContainer='/opt/crowdb/data/disks/vdf', CgroupPermissions='rw')
        current['HostConfig']['Devices'] = [device]
        require_owned(current, record)
        for key, value in [('PathOnHost', '/dev/vdg'), ('CgroupPermissions', 'rwm')]:
            changed = copy.deepcopy(current)
            changed['HostConfig']['Devices'][0][key] = value
            with self.assertRaises(ValueError): require_owned(changed, record)


if __name__ == '__main__': unittest.main()
