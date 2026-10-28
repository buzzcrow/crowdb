# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Confirmed nonvoter cold recovery across new names, addresses and an empty old bridge."""

import concurrent.futures
import json
from pathlib import Path
import socket
import time
import uuid
from urllib.parse import urlparse

from fixture import HTTP, LABEL, request, wait
from profiles.node import request as admission_request
import oci_reference_runtime as runtime
import oci_reference_protocols as protocols


def origin(cluster, index):
    return f'http://127.0.0.1:{cluster.port_base + index * 10}'


def create_nonvoter(cluster):
    name = cluster.expected_names[3]
    volume = runtime.node_volume(cluster, 3)
    policy = cluster.artifacts / 'later-member.toml'
    lines = ['version = 1', 'mode = "local"', f'cluster = "{cluster.run_id}"',
             'initial_voters = 3', '[storage]', 'mode = "diskless"']
    for kind in ('paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server'):
        lines += ['[[services]]', f'kind = "{kind}"', 'enabled = false']
    content = '\n'.join(lines) + '\n'
    if policy.exists():
        assert policy.read_text() == content
    else:
        policy.write_text(content)
        policy.chmod(0o644)
    cluster.command('volume', 'create', '--label', f'{LABEL}={cluster.run_id}', volume)
    cluster.command('create', '--name', name, '--label', f'{LABEL}={cluster.run_id}',
        '--network', getattr(cluster, 'reference_network', cluster.run_id),
        '--cpus', '0.25', '--memory', '768m', '--env', 'TOKIO_WORKER_THREADS=1',
        '--mount', f'type=volume,source={volume},target=/opt/crowdb/data',
        '--mount', f'type=bind,source={policy.resolve()},target=/run/later-member.toml,readonly',
        '--env', 'CROWDB_NODE_POLICY_FILE=/run/later-member.toml',
        '--env', f'CROWDB_CLUSTER={cluster.run_id}', '--env', 'CROWDB_INITIAL_VOTERS=3',
        '--publish', f'127.0.0.1:{cluster.port_base + 30}:6135', cluster.image)
    cluster.containers.append(name)


def read_json(cluster, name, path):
    return json.loads(cluster.command('exec', '--user', '10001:10001', name,
                                     'cat', '/opt/crowdb/data/' + path))


def identity(cluster, name):
    accepted = read_json(cluster, name, 'accepted-node.json')
    manifest = read_json(cluster, name, 'prepared-bootstrap.json')
    pins = read_json(cluster, name, 'confirmed-member-trust.json')
    assert len(manifest['nodes']) == len(pins['initial']['members']) == 3
    assert sorted(member['node_id'] for member in pins['current']['members']) == [1, 2, 3, 4]
    assert manifest['identity'] == accepted['bootstrap'] == pins['current']['bootstrap']
    paths = ['node-identity', 'node-policy.json', 'prepared-bootstrap.json', 'member-trust.json',
             'ssh/id_ed25519.pub', 'ssh/ssh_host_ed25519_key.pub']
    if accepted.get('admission'):
        operation = uuid.UUID(accepted['admission']['operation_id'])
        paths.append(f'admitted-member-trust-{operation}.json')
    hashes = cluster.command('exec', name, 'sha256sum', *['/opt/crowdb/data/' + path for path in paths])
    return dict(node_id=accepted['node_id'], bootstrap=accepted['bootstrap'], hashes=hashes)


def cluster_report(cluster, index):
    # Match the public CLI's unchanged observation budget. Aggregate status may
    # spend six seconds collecting an absent member; health reads use five.
    # Connection errors remain retryable only inside the bounded read-only wait.
    with HTTP.open(origin(cluster, index) + '/api/cluster/status', timeout=10) as response:
        return json.load(response)


def authority(report):
    return next((value for value in report['observation']['dependencies']
                 if value['id'] == 'cluster/authority'), None)


def no_old_transports(addresses):
    def unreachable(endpoint):
        try:
            with socket.create_connection(endpoint, timeout=.25):
                raise AssertionError(f'old management transport remains reachable: {endpoint}')
        except OSError:
            return endpoint
    endpoints = [(address, port) for address in addresses for port in (2222, 6135, 26135, 10135, 11135)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        return list(executor.map(unreachable, endpoints))


def replace_addresses(cluster, before):
    items = runtime.inventory(cluster, 'container')
    for item in items:
        runtime.validate_container(cluster, item)
    runtime.validate_networks(cluster, items)
    retained = [runtime.node_volume(cluster, index) for index in range(4)]
    assert {item['Name'] for item in runtime.inventory(cluster, 'volume')} == set(retained)
    network = cluster.run_id + '-recovered'
    names = [name + '-cold' for name in cluster.expected_names]
    (cluster.artifacts / 'cold-runtime-intent.json').write_text(json.dumps(dict(
        old_names=cluster.expected_names, names=names, volumes=retained,
        old_network=cluster.run_id, network=network, image_id=cluster.image_id), indent=2))
    for item in items:
        cluster.command('stop', '--time', '30', item['Id'])
    for item in runtime.inventory(cluster, 'container'):
        runtime.validate_container(cluster, item)
        assert not item['State']['Running']
        cluster.command('rm', item['Id'])
    # Keep the empty original bridge so Docker cannot allocate its old subnet.
    cluster.reference_networks = {cluster.run_id, network}
    cluster.command('network', 'create', '--label', f'{LABEL}={cluster.run_id}', network)
    cluster.reference_network = network
    cluster.reference_volumes = retained
    cluster.expected_names = names
    cluster.containers = []
    runtime.create_docker_nodes(cluster, reversed(range(3)))
    create_nonvoter(cluster)
    created = runtime.inventory(cluster, 'container')
    for item in created:
        runtime.validate_container(cluster, item)
    current = {item['NetworkSettings']['Networks'][network]['IPAddress'] for item in created}
    # Docker may populate endpoint addresses only when the container starts.
    assert not (current - {''}) & set(before)
    old = next(item for item in runtime.inventory(cluster, 'network') if item['Name'] == cluster.run_id)
    assert not old.get('Containers')


def verify(cluster, cluster_id, witness, credentials):
    assert cluster.implementation == 'docker' and cluster.node_count == 3
    cluster.expected_names.append(cluster.run_id + '-node4')
    create_nonvoter(cluster)
    cluster.command('start', cluster.expected_names[3])
    local = wait(lambda: request(origin(cluster, 3), '/api/node/candidates'),
                 lambda value: bool(value.get('local')), 'later member discovery', seconds=30)
    discovery = local['local']['advertisement']['discovery_id']
    wait(lambda: request(origin(cluster, 0), '/api/node/candidates'),
         lambda value: any(row['advertisement']['discovery_id'] == discovery for row in value['candidates']),
         'later member multicast discovery', seconds=30)
    # Reuse the host deployment client's existing admission bound. The short
    # observation client must not cancel mutual SSH preparation mid-request.
    admitted = admission_request(origin(cluster, 0), '/api/node/admit', dict(discovery_id=discovery,
        rack_id=1,ssh_user='crowdb',ssh_port=2222,ssh_password='crowdb'))
    assert admitted['node_id'] == 4
    wait(lambda: request(origin(cluster, 3), '/api/node/status'),
         lambda value: value['phase'] == 'active', 'confirmed later nonvoter', seconds=180)
    cluster.ready_report()
    before = [identity(cluster, name) for name in cluster.expected_names]
    assert before[3]['node_id'] == 4
    items = runtime.refresh(cluster)
    old_ips = [item['NetworkSettings']['Networks'][cluster.run_id]['IPAddress'] for item in items]
    for name in cluster.expected_names:
        binding = read_json(cluster, name, 'node-binding.json')
        assert all(urlparse(seed).hostname in old_ips for seed in binding['management_seeds'])
    replace_addresses(cluster, old_ips)
    cluster.command('start', cluster.expected_names[3])
    cluster.command('start', cluster.expected_names[1])
    wait(lambda: cluster_report(cluster, 3),
         lambda value: bool(authority(value)), 'quorum-free fourth member management', seconds=30)
    negative = []
    for _ in range(8):
        observed = cluster_report(cluster, 3)
        assert observed['phase'] != 'ready' and authority(observed)['state'] != 'ready', observed
        negative.append(observed['phase'])
        time.sleep(.5)
    for name in (cluster.expected_names[3], cluster.expected_names[1]):
        current = json.loads(cluster.command('inspect', name))[0]
        runtime.validate_container(cluster, current)
        assert current['State']['Running'], current['State']
    cluster.command('start', cluster.expected_names[0])
    partial = wait(lambda: cluster_report(cluster, 3),
                   lambda value: authority(value) and authority(value)['state'] == 'ready',
                   'two original voters restore authority', seconds=180)
    assert partial['phase'] != 'ready', partial
    cluster.command('start', cluster.expected_names[2])
    report = cluster.ready_report()
    assert report['observation']['cluster_id'] == cluster_id
    after = [identity(cluster, name) for name in cluster.expected_names]
    assert before == after
    replicas = request(cluster.endpoint(0), '/api/stores/0/groups/0/replicas')
    assert sorted(row['node_id'] for row in replicas) == [1, 2, 3]
    items = runtime.refresh(cluster)
    current_ips = [item['NetworkSettings']['Networks'][cluster.reference_network]['IPAddress'] for item in items]
    assert not set(old_ips) & set(current_ips)
    unreachable = no_old_transports(old_ips)
    assert [protocols.credentials(cluster, index) for index in range(3)] == credentials
    protocols.read_witnesses(cluster, witness)
    (cluster.artifacts / 'cold-recovery-proof.json').write_text(json.dumps(dict(
        cluster_id=cluster_id, identities=after, initial_voters=3, confirmed_contacts=4,
        original_voter_ids=[1,2,3], negative_phases=negative, partial_phase=partial['phase'],
        admission_submissions=1,
        old_addresses=old_ips, current_addresses=current_ips, old_transports_unreachable=unreachable,
        stable_names=False, peer_list=False, retained_credentials=True,
        s3_retained_bytes=True, iceberg_committed_properties=True, dataset_snapshot_manifest=True), indent=2))
    print('PASS confirmed nonvoter cold recovery with new names/IPs and unchanged original votes', flush=True)
