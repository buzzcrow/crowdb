# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Pinned host-runtime commands and exact owned-container inspection."""

import json
import os
import shutil
import subprocess
from pathlib import Path

from runtime import Runtime, OWNER


def output(command):
    result = subprocess.run(command, text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or 'runtime command failed')
    return result.stdout.strip()


def executable(name):
    binary = shutil.which(name)
    if binary is None:
        prefix = os.environ.get('CONDA_PREFIX')
        candidate = Path(prefix) / 'bin' / name if prefix else None
        if candidate and candidate.is_file():
            binary = str(candidate)
    if binary is None:
        raise ValueError(f'{name} is required on PATH; activate the OCI runtime Pixi environment')
    return str(Path(binary).resolve())


class HostRuntime(Runtime):
    def __init__(self, options, cluster):
        self.name = options['runtime']
        self.namespace = 'crowdb-' + cluster
        self.snapshotter = options['snapshotter']
        self.binary = executable('docker' if self.name == 'docker' else 'nerdctl')
        if self.name == 'docker':
            self.context = output([self.binary, 'context', 'show'])
            details = json.loads(output([self.binary, 'context', 'inspect', self.context]))[0]
            self.address = (options.get('runtime_endpoint') or os.environ.get('DOCKER_HOST')
                            or details['Endpoints']['docker']['Host'])
            if not self.address.startswith('unix://'):
                raise ValueError('host lifecycle requires a local Unix Docker endpoint')
            self.command = [self.binary, '--host', self.address]
        else:
            self.context = self.namespace
            self.address = options.get('runtime_endpoint') or '/run/containerd/containerd.sock'
            if not Path(self.address).is_absolute():
                raise ValueError('containerd address must be an absolute Unix socket path')
            self.command = [self.binary, '--address', self.address, '--namespace', self.namespace,
                            '--snapshotter', self.snapshotter]

    def identity(self):
        info = json.loads(self.invoke('info', '--format', '{{json .}}'))
        daemon = info.get('ID')
        if not isinstance(daemon, str) or not daemon:
            raise ValueError('runtime did not report its stable daemon identity')
        if self.name == 'containerd' and info.get('Driver') != self.snapshotter:
            raise ValueError('containerd did not apply the requested snapshotter')
        if self.name == 'docker' and output([self.binary, 'context', 'show']) != self.context:
            raise ValueError('Docker context changed during host launch')
        return {'selection': {'runtime': self.name, 'endpoint': self.address, 'context': self.context},
                'daemon': daemon, 'snapshotter': self.snapshotter, 'binary': self.binary}

    def check(self, record):
        if self.identity() != record['runtime']:
            raise ValueError('runtime daemon, endpoint, context or snapshotter differs from the host receipt')

    def named(self, name):
        identities = self.invoke('ps', '-aq').splitlines()
        if len(identities) > 4096:
            raise ValueError('runtime inventory exceeds the host inspection bound')
        for identity in identities:
            current = self.inspect(identity)
            if current.get('Name', '').lstrip('/') == name:
                return current
        return None


def require_owned(current, record):
    if (current.get('Config', {}).get('Labels', {}).get(OWNER) != record['owner']
            or current.get('Config', {}).get('Labels', {}).get('org.crowdb.cluster') != record['cluster']
            or current.get('Name', '').lstrip('/') != record['name']):
        raise ValueError('same-name host container is foreign; refusing lifecycle effects')
    if current.get('Image') != record['runtime_image']:
        raise ValueError('owned host container image differs from its accepted artifact')
    host = current.get('HostConfig', {})
    if host.get('NetworkMode') != 'host':
        raise ValueError('owned host container no longer uses host networking')
    period, quota = host.get('CpuPeriod', 0), host.get('CpuQuota', 0)
    if (period <= 0 or abs(quota / period - record['options']['cpus']) > 0.00001
            or host.get('Memory') != record['options']['memory_mib'] * 1024 * 1024):
        raise ValueError('owned host container resource limits differ from accepted inputs')
    expected = {'/opt/crowdb/data': (record['options']['data_root'], True),
                '/run/crowdb-node-policy.toml': (record['options']['node_policy_file'], False)}
    if record['options'].get('password_file'):
        expected['/run/crowdb-ssh-password'] = (record['options']['password_file'], False)
    for disk in record['disks']:
        expected[disk['path']] = (disk['source'], True)
    mounts = {mount['Destination']: mount for mount in current.get('Mounts', [])}
    for target, (source, writable) in expected.items():
        mount = mounts.get(target)
        # Raw devices are represented by HostConfig.Devices rather than Mounts.
        device = next((device for device in (current.get('HostConfig', {}).get('Devices') or [])
                       if device.get('PathInContainer') == target), None)
        if device is not None:
            if device.get('PathOnHost') != source or device.get('CgroupPermissions') != 'rw':
                raise ValueError('owned raw-device mapping differs from its accepted input')
        elif (mount is None or mount.get('Source') != source or mount.get('RW') != writable):
            raise ValueError('owned host container mount differs from its accepted input')
    environment = dict(item.split('=', 1) for item in current.get('Config', {}).get('Env', [])
                       if '=' in item)
    options = record['options']
    for key, value in {'CROWDB_CLUSTER': record['cluster'], 'CROWDB_INITIAL_VOTERS': options['nodes'],
                       'CROWDB_DEPLOYMENT_MODE': 'production',
                       'CROWDB_PHYSICAL_HOST_ID': options['physical_host_id'],
                       'CROWDB_MANAGEMENT_INTERFACE': options['interface'],
                       'CROWDB_NODE_POLICY_FILE': '/run/crowdb-node-policy.toml'}.items():
        if environment.get(key) != str(value):
            raise ValueError('owned host container startup policy differs from its accepted input')
    declaration = [{key: disk[key] for key in ('backend', 'path', 'backing_identity', 'resource_identity')}
                   for disk in record['disks']]
    if json.loads(environment.get('CROWDB_DISK_INPUTS', 'null')) != declaration:
        raise ValueError('owned host container media declaration differs from accepted inputs')
    if environment.get('CROWDB_DISCOVERY_SEEDS', '') != ','.join(options['seed']):
        raise ValueError('owned host container discovery inputs differ from accepted inputs')
    expected_password = '/run/crowdb-ssh-password' if options.get('password_file') else None
    if environment.get('CROWDB_SSH_PASSWORD_FILE') != expected_password:
        raise ValueError('owned host container credential input differs from accepted inputs')
