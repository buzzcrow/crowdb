# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Docker-free production metadata lifecycle and Local protocol recreation."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'e2e-test'))
sys.path.append(str(ROOT / 'container/oci-image'))
from artifact import receipt
from oci_native_runtime import NativeRuntime, docker_free
from oci_host_node import HostNode
from oci_local_host_node import LocalHostNode
from fixture import redact
from failure_logs import print_failure


def metadata_runtime(root,bundle,image,address):
    node = HostNode(root,bundle,image,'containerd',address=address,metadata_only=True)
    try:
        node.start(fresh=True)
        before = node.admit()
        node.recreate()
        after = node.metadata_report()
        assert before['observation']['cluster_id'] == after['observation']['cluster_id']
        (root/'common-cli-metadata-proof.json').write_text(json.dumps(dict(
            cluster_id=after['observation']['cluster_id'],production=True,diskless=True,
            retained_recreation=True,common_cli=True),indent=2))
    except BaseException as error:
        print_failure(root,[node.data],'deployment.containerd.cli',error,redact)
        raise
    finally:
        node.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('--serve', action='store_true', help='serve a diskless voter for the separately labeled Docker-provisioned cluster fixture')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('this acceptance needs an explicitly privileged disposable Linux host')
    docker_free(allow_container_host=args.serve)
    verified = receipt(args.archive)
    root = Path(os.environ['CROWDB_CONTAINERD_DATA'])
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    bundle = Path(os.environ['CROWDB_E2E_CLI_BUNDLE'])
    cluster = os.environ.get('CROWDB_CONTAINERD_CLUSTER', 'oci-' + root.parent.name[-20:])
    os.environ['CROWDB_CONTAINERD_CLUSTER'] = cluster
    with NativeRuntime(root / 'daemon', cluster, docker_provisioned=args.serve) as daemon:
        daemon.runtime.invoke('load', '--input', str(args.archive.resolve()))
        if args.serve:
            node = HostNode(root,bundle,verified['image_digest'],'containerd',address=daemon.socket,
                            voters=3,metadata_only=True)
        else:
            metadata_runtime(root/'metadata',bundle,verified['image_digest'],daemon.socket)
            node = LocalHostNode(root/'evaluation',bundle,verified['image_digest'],'containerd',address=daemon.socket)
        try:
            report = node.start(fresh=True)
            before = node.facts()
            (root / 'host-facts-before.json').write_text(json.dumps(before, indent=2))
            if args.serve:
                Path('/tmp/crowdb-containerd-address').write_text(daemon.socket)
                Path('/tmp/crowdb-containerd-name').write_text(node.record()['name'])
                while daemon.process.poll() is None:
                    time.sleep(1)
                raise RuntimeError('owned containerd exited during fixture')
            witness, credentials = node.protocols()
            node.recreate()
            restored = json.loads(node.command(['cluster', 'check', '--wait', '180', '--json']))
            assert report['observation']['cluster_id'] == restored['observation']['cluster_id']
            _, after_credentials = node.protocols(witness)
            assert credentials == after_credentials
            after = node.facts()
            assert before['container_id'] != after['container_id']
            for field in ('daemon_id','image','discovery_id','cpu_max','memory_max','host_network'):
                assert before[field] == after[field], field
            docker_free()
            proof = dict(runtime='native rootful containerd', docker_client=False, docker_socket=False,
                docker_daemon=False, image_manifest=verified['image_digest'],
                policy='local',node_failure_tolerance=0,production_cli_metadata_recreation=True,
                cluster_id=report['observation']['cluster_id'], before=before, after=after,
                s3_retained_bytes=True, iceberg_committed_properties=True, dataset_snapshot_manifest=True)
            (root / 'protocol-recreation-proof.json').write_text(json.dumps(proof, indent=2))
            print('Native containerd Local protocols and production metadata recreation passed: ' + verified['image_digest'], flush=True)
        except BaseException as error:
            print_failure(root, [node.data], 'deployment.containerd.native', error, redact)
            raise
        finally:
            node.cleanup()


if __name__ == '__main__':
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt('owned native host interrupted')
    signal.signal(signal.SIGTERM, interrupt)
    main()
