# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit Docker host acceptance outside the concurrent bridge domain matrix."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
from fixture import docker, redact
from image_identity import verify_server_sources
from client import bundle_client
from native_containerd_profile import client_bundle
from oci_host_node import clean, read_private, selected
from oci_local_host_node import cleanup_local_host

ROOT = Path(__file__).resolve().parents[1]


def cleanup_owned_hosts(data):
    errors = []
    for case in ('protocols', 'supplied'):
        try:
            cleanup_local_host(data/case)
        except Exception as error:
            errors.append(str(error))
        runtime_root = data / case / 'private/runtime'
        directory = runtime_root / 'persistent/oci-hosts'
        for path in directory.glob('*.json'):
            try:
                if read_private(path) is not None:
                    clean(selected(path.stem, runtime_root))
            except Exception as error:
                errors.append(str(error))
    if errors:
        raise RuntimeError('host cleanup failed: ' + '; '.join(errors))


def media_client(destination):
    supplied = os.environ.get('CROWDB_E2E_DOMAIN_BUNDLE')
    if not supplied:
        return bundle_client(destination)
    source = Path(supplied).resolve()
    (destination / 'lib').mkdir(parents=True)
    shutil.copy2(source / 'domain-test', destination / 'domain-test')
    for path in (source / 'lib').iterdir():
        if path.is_file():
            shutil.copy2(path, destination / 'lib' / path.name)
    return destination


def run_host(args, parser):
    if args.layer != 'all' or args.scope != 'all' or args.case or args.require_isolation:
        parser.error('host profile has one deployment acceptance; domain selectors do not apply')
    if args.concurrency != 1:
        parser.error('host fixed ports require --concurrency 1 and an exclusive host')
    if args.cpus < 1.5 or args.memory_mib < 2560:
        parser.error('host profile requires 1.5 CPU and 2560 MiB including the node and host client')
    if args.list:
        print(json.dumps([dict(scope='deployment.host', case='host_network_admission_and_recovery', nodes=1)]))
        return
    if sys.platform != 'linux' or os.geteuid() != 0:
        parser.error('host acceptance requires an explicitly privileged disposable Linux host')
    info = json.loads(docker('info', '--format', '{{json .}}'))
    if info['OSType'] != 'linux' or info['NCPU'] < 1.5 or info['MemTotal'] < 2560 * 1048576:
        parser.error('host deployment requires a Linux daemon with 1.5 CPU / 2560 MiB capacity')
    identity = json.loads(docker('image', 'inspect', args.image))[0]
    labels = identity['Config'].get('Labels') or {}
    try:
        source = verify_server_sources(ROOT, labels.get('org.opencontainers.image.revision'),
                                       labels.get('org.crowdb.source.tree') or None)
    except ValueError as error:
        parser.error(str(error))
    run_root = args.artifacts / ('run-' + uuid.uuid4().hex)
    run_root.mkdir(parents=True, mode=0o700)
    data = (args.data_root / run_root.name / 'host' if args.data_root else
            ROOT / '.crowdb-runtime/test-data/container-e2e' / run_root.name / 'host').resolve()
    data.mkdir(parents=True, mode=0o700)
    (run_root / 'identity.json').write_text(json.dumps(dict(profile='host', id=identity['Id'],
        labels=labels, sources=source, concurrency=1, cpus=1.5, memory_mib=2560,
        host_data=str(data), core_policy=Path('/proc/sys/kernel/core_pattern').read_text().strip()), indent=2))
    cli = client_bundle(run_root / 'cli')
    client = media_client(run_root / 'media-client')
    env = dict(os.environ, CROWDB_CONTAINER_IMAGE=identity['Id'], CROWDB_E2E_PROFILE_DATA=str(data),
               CROWDB_E2E_CLI_BUNDLE=str(cli), CROWDB_MEDIA_CLIENT=str(client),
               CROWDB_CONTAINERD_CLUSTER='oci-' + run_root.name[-20:])
    started, status, code = time.monotonic(), 'failed', None
    print(f"Docker host deployment: exact image {identity['Id']}; artifacts {run_root}", flush=True)
    child = subprocess.Popen([sys.executable, str(ROOT / 'e2e-test/profiles/host-node.py')],
        cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        stdout, _ = child.communicate(timeout=900)
        code = child.returncode
        (run_root / 'client-host.log').write_text(redact(stdout))
        print(redact(stdout), end='', flush=True)
        if code:
            raise RuntimeError('host acceptance failed; inspect retained client log and node data')
        status = 'pass'
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                stdout, _ = child.communicate(timeout=45)
            except subprocess.TimeoutExpired:
                child.kill()
                stdout, _ = child.communicate(timeout=5)
            (run_root / 'client-host.log').write_text(redact(stdout))
            print(redact(stdout), end='', flush=True)
        try:
            cleanup_owned_hosts(data)
        except Exception:
            status = 'failed'
            raise
        finally:
            (run_root / 'results.json').write_text(json.dumps(dict(profile='host', status=status,
                exit_code=code, wall_seconds=time.monotonic()-started, passed=int(status=='pass'),
                failed=int(status!='pass'), skipped=0), indent=2))
