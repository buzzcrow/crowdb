# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Reuse the ordinary host launch validator before accepting runtime ownership."""

import hashlib
import ipaddress
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tomllib

import run_node
from runtime import node_policy_file
from storage import inputs, validate_retained_inputs
from host_runtime import output


class Parser:
    @staticmethod
    def error(message):
        raise ValueError(message)


def options(request, previous):
    proposed = request['options']
    if previous:
        accepted = previous['options']
        for key, value in proposed.items():
            if value is not None and value != [] and value != accepted.get(key):
                raise ValueError('host launch arguments differ from accepted inputs: ' + key)
        return dict(accepted)
    current = {'runtime': 'docker', 'network': 'host', 'snapshotter': 'overlayfs',
               'device': [], 'disk_file': [], 'seed': [], 'runtime_endpoint': None,
               'password_file': None}
    current.update({key: value for key, value in proposed.items() if value is not None})
    for key in ('image', 'cpus', 'memory_mib', 'data_root', 'physical_host_id', 'interface',
                'management_address', 'node_policy_file'):
        if not current.get(key):
            raise ValueError('new host launch requires --' + key.replace('_', '-'))
    if current['network'] != 'host':
        raise ValueError('this host lifecycle requires explicit host networking')
    if current['runtime'] not in ('docker', 'containerd'):
        raise ValueError('unsupported host runtime')
    if current['runtime'] == 'docker' and proposed.get('snapshotter') is not None:
        raise ValueError('--snapshotter requires containerd')
    policy_path = node_policy_file(SimpleNamespace(node_policy_file=Path(current['node_policy_file'])))
    with policy_path.open('rb') as source:
        content = source.read(32769)
    if len(content) > 32768:
        raise ValueError('node policy exceeds its bound')
    try:
        policy = tomllib.loads(content.decode('utf-8'))
    except (UnicodeError, tomllib.TOMLDecodeError):
        raise ValueError('node policy is not valid UTF-8 TOML') from None
    current['nodes'] = current.get('nodes') or policy.get('initial_voters', 3)
    if (policy.get('version') != 1 or policy.get('mode') != 'production'
            or policy.get('cluster') != request['cluster']
            or policy.get('initial_voters') != current['nodes']):
        raise ValueError('node policy formation differs from the selected production host launch')
    return current


def arguments(record):
    values = dict(record['options'])
    for key in ('data_root', 'node_policy_file', 'password_file'):
        if values.get(key):
            values[key] = Path(values[key])
    for key in ('device', 'disk_file'):
        values[key] = [Path(path) for path in values[key]]
    values.update(name=record['name'], cluster=record['cluster'], initial_voters=values['nodes'],
                  ui_port=None, cpu_shares=None, worker_threads=None, owner_token=record['owner'])
    return SimpleNamespace(**values)


def management(options):
    address = ipaddress.ip_address(options['management_address'])
    if address.is_unspecified or address.is_multicast:
        raise ValueError('management address must identify this host interface')
    interfaces = json.loads(output(['ip', '-json', 'address', 'show', 'dev', options['interface']]))
    if str(address) not in {value['local'] for interface in interfaces
                            for value in interface.get('addr_info', [])}:
        raise ValueError('management address is not assigned to the selected interface')
    return 'http://' + (f'[{address}]' if address.version == 6 else str(address)) + ':9090'


def validate(record, running):
    args = arguments(record)
    for key in ('data_root', 'node_policy_file', 'password_file'):
        value = getattr(args, key)
        if value and any(path.is_symlink() for path in (value, *value.parents)):
            raise ValueError('host launch paths cannot contain symlinks')
    policy = node_policy_file(args)
    secret = args.password_file
    if secret and secret.stat().st_size > 4096:
        raise ValueError('SSH password file exceeds its bound')
    run_node.validate(args, Parser(), check_ports=not running)
    disks = inputs(args)
    validate_retained_inputs(args.data_root, disks)
    hashes = {'node_policy': hashlib.sha256(policy.read_bytes()).hexdigest(),
              'password': hashlib.sha256(secret.read_bytes()).hexdigest() if secret else None}
    return disks, hashes, [management(record['options'])]
