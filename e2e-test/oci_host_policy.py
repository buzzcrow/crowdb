# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Accept a diskless test policy before attaching unadmitted candidate media."""

import json
import os
from pathlib import Path
import uuid

from host_runtime import HostRuntime
from runtime import OWNER


def accept_candidates(node):
    runtime = HostRuntime(dict(runtime=node.kind, snapshotter='overlayfs', runtime_endpoint=None), node.cluster)
    name = 'crowdb-' + node.cluster + '-candidate-policy'
    token = str(uuid.uuid4())
    identity = runtime.identity()
    if runtime.named(name) is not None:
        raise ValueError('candidate-policy scope already contains a container')
    intent = node.private/'candidate-policy.json'
    with intent.open('x') as stream:
        intent.chmod(0o600)
        json.dump(dict(name=name, owner=token, image=node.image, runtime=identity), stream)
        stream.flush()
        os.fsync(stream.fileno())
    machine = Path('/etc/machine-id')
    physical = (machine.read_text().strip() if machine.is_file() else '') or os.uname().nodename
    try:
        assert runtime.identity() == identity
        result = runtime.invoke('run', '--rm', '--name', name, '--network', 'none', '--read-only',
            '--cpus', '.25', '--memory', '128m', '--user', '10001:10001',
            '--label', f'{OWNER}={token}', '--label', f'org.crowdb.cluster={node.cluster}',
            '--mount', f'type=bind,source={node.data},target=/opt/crowdb/data',
            '--mount', f'type=bind,source={node.policy},target=/run/node-policy.toml,readonly',
            '--env', 'CROWDB_NODE_POLICY_FILE=/run/node-policy.toml',
            '--env', f'CROWDB_PHYSICAL_HOST_ID={physical}',
            '--env', f'CROWDB_CLUSTER={node.cluster}', '--env', 'CROWDB_INITIAL_VOTERS=1',
            '--entrypoint', '/opt/crowdb/bin/crowdb-monitor', node.image, 'accept-policy')
        assert result.strip() == 'production', result
        accepted = json.loads((node.data/'node-policy.json').read_text())
        assert accepted['cluster'] == node.cluster and accepted['setup']['storage']['mode'] == 'diskless'
        assert not (node.data/'node-identity').exists() and not (node.data/'disks').exists()
    finally:
        current = runtime.named(name)
        if current:
            labels = current['Config'].get('Labels', {})
            if (runtime.identity() != identity or current['Image'] != node.image
                    or labels.get(OWNER) != token or labels.get('org.crowdb.cluster') != node.cluster):
                raise ValueError('candidate-policy runtime ownership changed; preserving container')
            if current['State']['Running']:
                runtime.invoke('stop', current['Id'])
            runtime.invoke('rm', current['Id'])
