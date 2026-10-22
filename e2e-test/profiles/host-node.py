#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Common CLI Docker host lifecycle, protocols and supplied-media durability."""

import json
import os
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'e2e-test'))
from fixture import wait, redact
from failure_logs import print_failure
from oci_host_node import HostNode
from oci_host_policy import accept_candidates
from oci_local_host_node import LocalHostNode
from node import request
from node_resources import verify_reaping, verify_stopped
from supplied_media import Media, provision, remove_and_rejoin, wait_for_descriptors
from host_runtime import HostRuntime


class NodeRuntime:
    def __init__(self, runtime, name):
        self.runtime, self.name = runtime, name

    def invoke(self, *arguments):
        return self.runtime.invoke(*(self.name if value == 'crowdb-node' else value for value in arguments))


def protocols(root, bundle, image):
    node = LocalHostNode(root, bundle, image, 'docker')
    try:
        report = node.start(fresh=True)
        before = node.facts()
        runtime = node.runtime()
        name = node.record()['name']
        verify_reaping(runtime.invoke, name, wait)
        witness, credentials = node.protocols()
        runtime.invoke('stop','--time','30',name)
        verify_stopped(runtime.invoke, name)
        node.recreate()
        restored = json.loads(node.command(['cluster', 'check', '--wait', '180', '--json']))
        assert report['observation']['cluster_id'] == restored['observation']['cluster_id']
        _, current = node.protocols(witness)
        assert current == credentials
        after = node.facts()
        assert before['container_id'] != after['container_id']
        for field in ('daemon_id', 'image', 'discovery_id', 'host_network', 'cpu_max', 'memory_max'):
            assert before[field] == after[field], field
        (root / 'protocol-recreation-proof.json').write_text(json.dumps(dict(
            before=before, after=after, cluster_id=report['observation']['cluster_id'],
            s3_retained_bytes=True, iceberg_committed_properties=True, dataset_snapshot_manifest=True), indent=2))
    except BaseException as error:
        print_failure(root, [node.data], 'deployment.host.protocols', error, redact)
        raise
    finally:
        node.cleanup()


def supplied(root, bundle, image):
    root.mkdir(mode=0o700)
    os.environ['CROWDB_OWNED_MEDIA_ROOT'] = str(root)
    media = Media(root / 'supplied-media')
    node = None
    try:
        node = HostNode(root, bundle, image, 'docker', metadata_only=True, media=media)
        # This fixture tests manual candidate admission after a genuine accepted
        # diskless policy. Fresh populated automatic/diskless roots must reject.
        accept_candidates(node)
        node.start(fresh=True)
        record = node.record()
        os.environ['CROWDB_MEDIA_HOST_ID'] = record['options']['physical_host_id']
        runtime = HostRuntime(record['options'], node.cluster)
        api = lambda path, body=None: request(node.origin, path, body)
        def observe(action, predicate, description, seconds=10):
            return wait(action, predicate, description, seconds=seconds)
        report = node.admit()
        active = api('/api/node/status')
        accepted = json.loads((node.data / 'accepted-node.json').read_text())
        state = provision(media, node.data, accepted['node_id'], active['cluster_id'], api, observe)
        owned = NodeRuntime(runtime, record['name'])
        remove_and_rejoin(media, state, owned, api, observe)
        key = (node.data / 'ssh/id_ed25519.pub').read_bytes()
        node.recreate()
        restored = api('/api/node/status')
        assert restored['cluster_id'] == active['cluster_id'] == report['observation']['cluster_id']
        assert (node.data / 'ssh/id_ed25519.pub').read_bytes() == key
        wait_for_descriptors(media, state, owned, api, observe)
        media.probe(state, 'verify')
        assert not media.exclusive_available()
        media.verify_tails()
        cleanup = api('/api/node/cleanup', dict(operation_id=active['operation_id'], confirm_delete_system_store=True))
        assert cleanup['pending'] == [], cleanup
        (root / 'supplied-media-proof.json').write_text(json.dumps(dict(
            retained_bytes=True, offline_releases_raw=True, remove_rejoin_preserves_id=True,
            original_file_and_raw_descriptors=True, physical_tails_unchanged=True,
            cluster_id=active['cluster_id']), indent=2))
    except BaseException as error:
        print_failure(root, [node.data] if node else [], 'deployment.host.supplied', error, redact)
        raise
    finally:
        try:
            if node:
                node.cleanup()
        finally:
            media.close()


def main():
    if os.geteuid() != 0:
        raise RuntimeError('host acceptance requires an explicitly privileged disposable Linux host')
    root = Path(os.environ['CROWDB_E2E_PROFILE_DATA'])
    bundle = Path(os.environ['CROWDB_E2E_CLI_BUNDLE'])
    image = os.environ['CROWDB_CONTAINER_IMAGE']
    protocols(root / 'protocols', bundle, image)
    supplied(root / 'supplied', bundle, image)
    print('Docker host Local protocols, production metadata recreation and supplied-media lifecycle passed', flush=True)


if __name__ == '__main__':
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt('owned host acceptance interrupted')
    signal.signal(signal.SIGTERM, interrupt)
    main()
