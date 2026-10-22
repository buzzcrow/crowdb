# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Independent cleanup of a host container, retaining media and launch receipts."""

import json
import os
from pathlib import Path
import re
import stat
import uuid

from host_runtime import HostRuntime, require_owned


def read_private(path):
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('host receipt path cannot contain symlinks')
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077
                or metadata.st_uid != os.geteuid() or metadata.st_size > 1024 * 1024):
            raise ValueError('host receipt must be an owned bounded private regular file')
        content = os.read(descriptor, 1024 * 1024 + 1)
    finally:
        os.close(descriptor)
    if len(content) > 1024 * 1024:
        raise ValueError('host receipt exceeds its bound')
    return json.loads(content)


def selected(cluster, root):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,47}', cluster):
        raise ValueError('invalid cluster name')
    record = read_private(root / 'persistent/oci-hosts' / (cluster + '.json'))
    if record is None:
        raise ValueError('no accepted host ownership receipt')
    owner = uuid.UUID(record['owner'])
    if (record['version'] != 1 or record['cluster'] != cluster
            or record['name'] != 'crowdb-' + cluster + '-host'
            or str(owner) != record['owner'] or owner.int == 0
            or record['options']['network'] != 'host'
            or record['options']['runtime'] not in ('docker', 'containerd')):
        raise ValueError('invalid accepted host ownership')
    bridge = read_private(root / 'persistent/oci-clusters' / (cluster + '.json'))
    if bridge is not None:
        raise ValueError('conflicting host and bridge ownership receipts')
    binding = read_private(root / 'persistent/clusters' / cluster / 'target.json')
    if binding is not None and (binding.get('version') != 1 or binding.get('name') != cluster
            or binding.get('management_endpoints') != record['origins']
            or binding.get('runtime') != dict(selection=record['runtime']['selection'], owner=record['owner'])):
        raise ValueError('application target conflicts with accepted host ownership')
    return record


def clean(record):
    runtime = HostRuntime(record['options'], record['cluster'])
    runtime.check(record)
    current = runtime.named(record['name'])
    if current is None:
        return
    require_owned(current, record)
    identity = current['Id']
    if current['State']['Running']:
        runtime.check(record)
        runtime.invoke('stop', identity)
    current = runtime.inspect(identity)
    require_owned(current, record)
    if current['State']['Running']:
        raise ValueError('owned container is still running; refusing removal')
    runtime.check(record)
    runtime.invoke('rm', identity)
