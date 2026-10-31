# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Reclaim unused Docker resources without stopping containers or retained clusters."""

import json

from docker_local_state import OWNER, labels, resource_names
from runtime import OWNER as HOST_OWNER


def collect(docker, records, hosts, *, delete_data=True, runtime):
    """Keep retained receipts and let non-force Docker removals guard reference races."""
    owners = {record['owner'] for record in (*records, *hosts)}
    networks = {name for record in records for name in resource_names(record, 'network')}
    volumes = {name for record in records for name in resource_names(record, 'volume')}

    def retained(item, kind):
        metadata = labels(item, kind)
        return any(metadata.get(key) in owners for key in (OWNER, HOST_OWNER))

    def check_runtime():
        if docker.identity() != runtime:
            raise ValueError('Docker endpoint/context changed during garbage cleanup')

    def remove(kind, identity):
        check_runtime()
        # Never force removal: Docker rejects containers that restarted and
        # volumes/networks that acquired a reference since inspection.
        print(f'Removing unused Docker {kind}: {identity}', flush=True)
        result = docker.call('rm', identity) if kind == 'container' else docker.call(kind, 'rm', identity)
        if result:
            print(result, flush=True)

    check_runtime()
    for candidate in docker.objects('container'):
        if retained(candidate, 'container'):
            continue
        item = json.loads(docker.call('container', 'inspect', candidate['Id']))[0]
        if item['State'].get('Status') not in ('created', 'exited', 'dead'):
            continue
        if item['State']['Running'] or retained(item, 'container'):
            continue
        remove('container', item['Id'])

    for kind, protected in (('network', networks), ('volume', volumes)):
        if kind == 'volume' and not delete_data:
            print('Preserved unused Docker volumes (--keep-data)', flush=True)
            continue
        for candidate in docker.objects(kind, ('dangling=true',)):
            identity = candidate['Name'] if kind == 'volume' else candidate['Id']
            item = json.loads(docker.call(kind, 'inspect', identity))[0]
            if item['Name'] in protected or retained(item, kind):
                continue
            if kind == 'network':
                if item.get('Containers') or item['Name'] in ('bridge', 'host', 'none'):
                    continue
            elif docker.call('ps', '-aq', '--filter', 'volume=' + item['Name']):
                continue
            remove(kind, identity)

    check_runtime()
    print('Pruning unused Docker build cache', flush=True)
    print(docker.call('builder', 'prune', '--all', '--force'), flush=True)
