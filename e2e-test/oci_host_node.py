# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""One ordinary production node driven by the common CLI and independent cleanup."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from fixture import request, redact, wait
from profiles.node import request as admission_request
import oci_reference_protocols as protocols

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / 'container/oci-image'))
from host_cleanup import clean, selected, read_private
from host_runtime import HostRuntime
from storage import inputs
from types import SimpleNamespace


def interface_address():
    result = subprocess.run(['ip', '-json', 'route', 'get', '192.0.2.1'],
                            capture_output=True, text=True, check=True, timeout=10)
    route = json.loads(result.stdout)[0]
    return route['dev'], route.get('prefsrc') or route['src']


class HostNode:
    node_count = 1

    def __init__(self, root, bundle, image, runtime, *, address=None, voters=1, metadata_only=False, media=None):
        self.root, self.bundle = Path(root), Path(bundle)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.image, self.kind, self.address = image, runtime, address
        self.cluster = os.environ.get('CROWDB_CONTAINERD_CLUSTER', 'oci-' + self.root.parent.name[-20:])
        self.cli = self.bundle / 'crowdb-cli'
        self.cli_digest = self.client_digest()
        self.interface, self.ip = interface_address()
        self.origin = f'http://{self.ip}:6135'
        self.voters, self.metadata_only = voters, metadata_only
        self.private = self.root / 'private'
        self.private.mkdir(mode=0o700)
        self.data = self.private / 'node'
        self.data.mkdir(mode=0o755)
        os.chown(self.data, 10001, 10001)
        self.secret = self.private / 'password'
        self.secret.write_text('oci-host-acceptance-password')
        self.secret.chmod(0o600)
        self.policy = self.private / 'node-policy.toml'
        self.devices = ([] if metadata_only else
                        [Path(value) for value in json.loads(os.environ.get('CROWDB_E2E_HOST_DEVICES', '[]'))])
        self.disk_files = []
        if media:
            self.devices.append(media.raw)
            self.disk_files = [media.file, media.bad]
        self.make_policy()
        self.env = dict(os.environ, CROWDB_RUNTIME_ROOT=str(self.private / 'runtime'),
                        LD_LIBRARY_PATH=str(self.bundle / 'lib'), NO_PROXY='*', no_proxy='*')
        for key in ('CROWDB_CLUSTER', 'CROWDB_SYSTEM_IP', 'CROWDB_SYSTEM_PORT'):
            self.env.pop(key, None)

    def make_policy(self):
        lines = ['version = 1', 'mode = "production"', f'cluster = "{self.cluster}"',
                 f'initial_voters = {self.voters}']
        if self.metadata_only:
            lines += ['[storage]', 'mode = "diskless"']
            for kind in ('paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server'):
                lines += ['[[services]]', f'kind = "{kind}"', f'enabled = {str(kind == "paxos-kv").lower()}']
                if kind == 'paxos-kv':
                    lines += ['[services.listeners]', 'http_port = 10135', 'rpc_port = 11135']
        else:
            lines += ['[public_access]', *[f'{kind} = "http://{self.ip}:{port}"'
                for kind, port in [('s3',7135), ('iceberg',8135), ('dataset',9135)]], '[storage]']
            if self.devices:
                for device in self.devices:
                    os.chown(device, 10001, 10001)
                    device.chmod(0o660)
                lines += ['mode = "supplied"']
                for disk in inputs(SimpleNamespace(device=self.devices, disk_file=[])):
                    capacity = int(Path('/sys/class/block', Path(disk['source']).name, 'size').read_text()) * 512
                    lines += ['[[storage.disks]]', *[f'{key} = {json.dumps(disk[key])}'
                        for key in ('backend','path','backing_identity','resource_identity')],
                        f'capacity_bytes = {capacity}', 'initialize = true']
            else:
                lines += ['mode = "automatic"', 'capacity_bytes = 8589934592',
                          'reserve_bytes = 1073741824', 'sparse = false']
        self.policy.write_text('\n'.join(lines) + '\n')
        self.policy.chmod(0o644)

    def command(self, arguments, expected=(0,), endpoint=None):
        assert self.client_digest() == self.cli_digest
        prefix = [str(self.cli), '--cluster', self.cluster]
        if endpoint:
            prefix += ['--management-endpoint', endpoint]
        result = subprocess.run([*prefix, *arguments], env=self.env, text=True,
                                capture_output=True, timeout=210)
        if result.returncode not in expected:
            print(redact(result.stdout + result.stderr), flush=True)
            raise AssertionError(f'host CLI {arguments[:2]} exited {result.returncode}')
        return result.stdout

    def client_digest(self):
        with self.cli.open('rb') as source:
            return hashlib.file_digest(source, 'sha256').hexdigest()

    def start(self, fresh=False):
        args = ['cluster', 'start', '--wait', '0' if fresh or self.metadata_only else '180', '--json']
        if fresh:
            identity = Path('/etc/machine-id')
            physical = (identity.read_text().strip() if identity.is_file() else '') or os.uname().nodename
            args += ['--runtime', self.kind, '--network', 'host', '--image', self.image,
                     '--nodes', str(self.voters), '--cpus', '1', '--memory-mib', '2048',
                     '--data-root', str(self.data), '--physical-host-id', physical,
                     '--interface', self.interface, '--management-address', self.ip,
                     '--node-policy-file', str(self.policy), '--password-file', str(self.secret)]
            if self.kind == 'containerd':
                args += ['--runtime-endpoint', self.address, '--snapshotter', 'native']
            for device in self.devices:
                args += ['--device', str(device)]
            for disk in self.disk_files:
                args += ['--disk-file', str(disk)]
        report = json.loads(self.command(args, (0, 1) if self.metadata_only else ((1,) if fresh else (0,))))
        print('Host lifecycle', self.kind, self.cluster, 'start' if fresh else 'resume', report['phase'], flush=True)
        return report

    def record(self):
        return selected(self.cluster, Path(self.env['CROWDB_RUNTIME_ROOT']))

    def facts(self):
        record = self.record()
        runtime = HostRuntime(record['options'], self.cluster)
        runtime.check(record)
        current = runtime.inspect(record['name'])
        runtime.verify(current['Id'], 1, 2048, True)
        return dict(runtime=self.kind, daemon_id=record['runtime']['daemon'],
                    container_id=current['Id'], image=current['Image'], host_network=True,
                    cpu_max=runtime.invoke('exec', current['Id'], 'cat', '/sys/fs/cgroup/cpu.max').strip(),
                    memory_max=runtime.invoke('exec', current['Id'], 'cat', '/sys/fs/cgroup/memory.max').strip(),
                    discovery_id=(self.data / 'node-identity').read_text().strip())

    def admit(self):
        deadline = time.monotonic() + 180
        while not (self.data / 'node-identity').is_file():
            if time.monotonic() > deadline:
                raise AssertionError('ordinary entrypoint did not create node identity')
            time.sleep(.25)
        discovery = (self.data / 'node-identity').read_text().strip()
        wait(lambda: request(self.origin, '/api/node/candidates'),
             lambda value: (value.get('local') or {}).get('advertisement', {}).get('discovery_id') == discovery,
             'ordinary host management and discovery', seconds=180)
        request(self.origin, '/api/racks', {'id':1, 'name':'host-production'})
        admitted = admission_request(self.origin, '/api/node/admit', dict(discovery_id=discovery, rack_id=1,
            ssh_user='crowdb', ssh_port=2222, ssh_password=self.secret.read_text()))
        request(self.origin, '/api/cluster/init', {'nodes':[admitted['node_id']]})
        if self.metadata_only:
            self.metadata_report()
        return self.start()

    def metadata_report(self):
        def authority_ready(report):
            return any(item['id'] == 'cluster/authority' and item['state'] == 'ready'
                       for item in report['observation']['dependencies'])
        return wait(lambda: request(self.origin, '/api/cluster/status'), authority_ready,
                    'production metadata authority', seconds=180)

    def endpoint(self, index):
        assert index == 0
        return self.origin

    def invoke(self, *arguments, endpoint=None):
        return self.command(list(arguments), endpoint=endpoint or self.origin)

    def protocols(self, witness=None):
        credentials = request(self.origin, '/api/access/credentials')
        original = protocols.credentials
        protocols.credentials = lambda _cluster, _index: request(self.origin, '/api/access/credentials')
        try:
            witness = protocols.write_witnesses(self) if witness is None else witness
            protocols.read_witnesses(self, witness)
            return witness, credentials
        finally:
            protocols.credentials = original

    def recreate(self):
        self.command(['cluster','stop'])
        # This cleanup remains usable when the common CLI is absent.
        clean(self.record())
        self.start()

    def cleanup(self):
        path = Path(self.env['CROWDB_RUNTIME_ROOT']) / 'persistent/oci-hosts' / (self.cluster + '.json')
        if read_private(path) is None:
            return
        record = self.record()
        runtime = HostRuntime(record['options'], self.cluster)
        current = runtime.named(record['name'])
        if current:
            result = subprocess.run([*runtime.command, 'logs', current['Id']], capture_output=True,
                                    text=True, timeout=60)
            (self.root / 'node.log').write_text(redact(result.stdout + result.stderr))
        clean(record)
