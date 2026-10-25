# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Real Pixi shortcuts without a usable CLI, including independent named roots."""

import hashlib
import json
import os
from pathlib import Path
import subprocess

from fixture import HTTP, request, wait

ROOT = Path(__file__).resolve().parents[1]


def status(port):
    with HTTP.open(f'http://127.0.0.1:{port}/api/cluster/status', timeout=10) as response:
        return json.load(response)


def named_clusters(cluster, invoke, ready, container_ids, credentials, one, other, port):
    for name, offset in ((one,0), (other,10)):
        invoke('oci-run-1c', '--cluster', name, '--image', cluster.image,
               '--port-base', str(port+offset))
    first, second = ready(0), ready(10)
    first_id, second_id = first['observation']['cluster_id'], second['observation']['cluster_id']
    assert first_id != second_id
    assert first['observation']['initial_voters']==second['observation']['initial_voters']==1
    original, unrelated = container_ids(one), container_ids(other)
    credentials_before = credentials(0)
    request(f'http://127.0.0.1:{port}', '/api/racks', {'id':67,'name':'Pixi retained'})
    invoke('oci-stop', '--cluster', one)
    invoke('oci-clean', '--cluster', one)
    assert not container_ids(one) and container_ids(other)==unrelated
    assert ready(10)['observation']['cluster_id']==second_id
    invoke('oci-run', '--cluster', one)
    assert ready(0)['observation']['cluster_id']==first_id
    assert container_ids(one)!=original and credentials(0)==credentials_before
    assert any(row['id']==67 for row in request(f'http://127.0.0.1:{port}', '/api/racks'))
    assert all(row['id']!=67 for row in request(f'http://127.0.0.1:{port+10}', '/api/racks'))
    for name in (one, other):
        invoke('oci-stop', '--cluster', name)
        invoke('oci-clean', '--cluster', name)
    return first_id, second_id, unrelated


def verify(cluster):
    root = cluster.artifacts/'shortcut-runtime'
    root.mkdir(mode=0o700)
    poison = root/'no-cli'
    poison.mkdir()
    binary = poison/'crowdb-cli'
    binary.write_text('#!/bin/sh\nprintf unexpected > "$CROWDB_TEST_CLI_MARKER"\nexit 97\n')
    binary.chmod(0o755)
    marker = root/'unexpected-cli'
    environment = dict(os.environ, CROWDB_RUNTIME_ROOT=str(root.resolve()),
        CROWDB_TEST_CLI_MARKER=str(marker.resolve()), PATH=str(poison.resolve())+':'+os.environ['PATH'])
    actions = []

    def invoke(task, *arguments, code=0):
        result = subprocess.run(['pixi', 'run', task, *arguments], cwd=ROOT, env=environment,
                                text=True, capture_output=True, timeout=210)
        (cluster.artifacts/f'shortcut-{len(actions)}.log').write_text(result.stdout+result.stderr)
        actions.append(dict(task=task, arguments=arguments, exit_code=result.returncode))
        assert not marker.exists(), 'shortcut invoked crowdb-cli'
        if result.returncode != code:
            print(result.stdout+result.stderr, flush=True)
            raise AssertionError(f'{task} exited {result.returncode}; expected {code}')
        return result.stdout

    selected = invoke('sh', '-c', 'command -v crowdb-cli').strip()
    assert selected == str(binary.resolve()), selected
    prefix = 'pixi-' + cluster.run_id[-12:]
    one, other, three, missing = [prefix+'-'+suffix for suffix in ('a', 'b', 'c', 'missing')]
    port = cluster.port_base

    def ready(offset):
        report = wait(lambda: status(port+offset), lambda row: row['phase']=='ready',
                      'ordinary Pixi node preparation', seconds=180)
        assert report['observation']['policy']['mode']=='local', report
        return report

    def container_ids(name):
        return cluster.command('ps', '-aq', '--filter', f'label=org.crowdb.cluster={name}').splitlines()

    def credentials(offset):
        value = request(f'http://127.0.0.1:{port+offset}', '/api/access/credentials')
        return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()

    success = False
    try:
        invoke('oci-run', '--cluster', missing, '--image', 'crowdb-node:missing-'+cluster.run_id, code=1)
        assert not container_ids(missing)
        first_id, second_id, unrelated = named_clusters(
            cluster, invoke, ready, container_ids, credentials, one, other, port)
        invoke('oci-run-3c', '--cluster', three, '--image', cluster.image, '--port-base', str(port))
        report = ready(0)
        assert report['observation']['initial_voters']==3
        assert len(container_ids(three))==3
        success = True
        proof = dict(image_id=cluster.image_id, independent_clusters=[first_id,second_id],
            three_node_cluster=report['observation']['cluster_id'], cli_invocations=0,
            retained_credentials=True, retained_management_write=True,
            unrelated_container_ids=unrelated, actions=actions)
    finally:
        invoke('oci-clean', '--all', *(['--delete-data'] if success else []))
    assert all(not container_ids(name) for name in (one,other,three,missing))
    (cluster.artifacts/'shortcuts-proof.json').write_text(json.dumps(proof,indent=2))
    print('PASS real Pixi one/three-node shortcuts, retained data and independent scoped cleanup without CLI',flush=True)
