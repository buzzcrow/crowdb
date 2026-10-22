# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Registered native containerd acceptance, independent of Docker host provisioning."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

from client import bundle_cli
from fixture import redact
from image_identity import verify_server_sources
from oci_native_runtime import docker_free

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / 'container/oci-image'))
from artifact import receipt


def client_bundle(destination):
    destination.mkdir(parents=True)
    (destination / 'lib').mkdir()
    supplied = os.environ.get('CROWDB_E2E_CLI_BUNDLE')
    if supplied:
        source = Path(supplied).resolve()
        shutil.copy2(source / 'crowdb-cli', destination / 'crowdb-cli')
        for path in (source / 'lib').iterdir():
            if path.is_file():
                shutil.copy2(path, destination / 'lib' / path.name)
        return destination
    bundle_cli(destination)
    result = subprocess.run(['ldd', '-r', str(destination / 'crowdb-cli')], text=True,
                            capture_output=True, check=True, timeout=30)
    if 'not found' in result.stdout or 'undefined symbol' in result.stdout:
        raise RuntimeError(result.stdout)
    prefix = Path(os.environ['CONDA_PREFIX']).resolve()
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[1] == '=>':
            source = Path(fields[2]).resolve()
            if source.is_relative_to(prefix):
                shutil.copy2(source, destination / 'lib' / fields[0])
    return destination


def run_profile(args, parser):
    if args.layer != 'all' or args.scope != 'all' or args.case or args.require_isolation:
        parser.error('native containerd has one host lifecycle acceptance; domain selectors do not apply')
    if args.concurrency != 1 or args.cpus < 1.5 or args.memory_mib < 2560:
        parser.error('native containerd requires --concurrency 1, 1.5 CPU and 2560 MiB including its host client')
    if args.list:
        print(json.dumps([dict(scope='deployment.containerd.native',
            case='local_protocols_and_production_metadata_recreation', nodes=1)]))
        return
    if sys.platform != 'linux' or os.geteuid() != 0:
        parser.error('native containerd acceptance requires an explicitly privileged disposable Linux host')
    docker_free()
    print('Native containerd preflight: verifying artifact and read-only server sources', flush=True)
    archive = args.archive.resolve()
    verified = receipt(archive)
    sources = verify_server_sources(ROOT, verified['revision'], verified.get('source_tree') or None)
    root = args.artifacts / ('run-' + uuid.uuid4().hex)
    root.mkdir(parents=True, mode=0o700)
    bundle = client_bundle(root / 'client')
    environment = dict(os.environ, CROWDB_E2E_CLI_BUNDLE=str(bundle),
        CROWDB_CONTAINERD_DATA=str(root / 'native-host'), CROWDB_CONTAINERD_CLUSTER='oci-' + root.name[-20:])
    (root / 'identity.json').write_text(json.dumps(dict(profile=args.profile, artifact=verified,
        sources=sources, execution='native Linux host', docker_client=False,
        docker_socket=False, docker_daemon=False), indent=2))
    print(f"Native containerd {verified['image_digest']}; artifacts {root}", flush=True)
    started = time.monotonic()
    child = subprocess.Popen([sys.executable, str(ROOT / 'e2e-test/profiles/containerd-host.py'), str(archive)],
        env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = child.communicate(timeout=900)
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                stdout, stderr = child.communicate(timeout=45)
            except subprocess.TimeoutExpired:
                child.kill()
                stdout, stderr = child.communicate(timeout=5)
            (root / 'client-containerd-interrupted.log').write_text(redact(stdout + stderr))
    code = child.returncode
    output = redact(stdout + stderr)
    (root / 'client-containerd.log').write_text(output)
    print(output, end='', flush=True)
    (root / 'results.json').write_text(json.dumps(dict(profile=args.profile,
        status='pass' if code == 0 else 'failed', wall_seconds=time.monotonic()-started,
        passed=int(code == 0), failed=int(code != 0), skipped=0), indent=2))
    if code:
        raise RuntimeError('native containerd acceptance failed; retained diagnostics: ' + str(root))
