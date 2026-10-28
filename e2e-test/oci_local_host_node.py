# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit local one-node policy for full host-network runtime qualification."""

import json
import os
from pathlib import Path
import subprocess
import uuid

from fixture import redact
from oci_host_node import HostNode
from host_runtime import HostRuntime
from runtime import OWNER
from host_cleanup import read_private


class LocalHostNode(HostNode):
    def make_policy(self):
        self.devices, self.disk_files = [], []
        lines = ['version = 1', 'mode = "local"', f'cluster = "{self.cluster}"', 'initial_voters = 1',
                 '[public_access]', *[f'{kind} = "http://{self.ip}:{port}"'
                 for kind, port in [('s3',7135), ('iceberg',8135), ('dataset',9135)]],
                 '[storage]', 'mode = "automatic"', 'capacity_bytes = 8589934592',
                 'reserve_bytes = 1073741824', 'sparse = true']
        self.policy.write_text('\n'.join(lines) + '\n')
        self.policy.chmod(0o644)

    def runtime(self):
        return HostRuntime(dict(runtime=self.kind, snapshotter='native' if self.kind == 'containerd' else 'overlayfs',
                                runtime_endpoint=self.address), self.cluster)

    def record(self):
        path = self.private / 'local-runtime.json'
        record = read_private(path)
        if record is None:
            raise ValueError('local test ownership is absent')
        return record

    def save(self, record):
        path = self.private / 'local-runtime.json'
        temporary = self.private / ('.local-runtime-' + uuid.uuid4().hex)
        with temporary.open('x') as stream:
            temporary.chmod(0o600)
            json.dump(record,stream,indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary,path)

    def owned(self, runtime, record):
        if runtime.identity() != record['runtime']:
            raise ValueError('local test runtime identity changed')
        current = runtime.named(record['name'])
        if current is None:
            return None
        labels = current.get('Config', {}).get('Labels', {})
        if (current['Image'] != self.image or labels.get(OWNER) != record['owner']
                or labels.get('org.crowdb.cluster') != self.cluster):
            raise ValueError('local test container ownership or image changed')
        host = current['HostConfig']
        cpus = host.get('NanoCpus', 0) / 1_000_000_000
        if not cpus and host.get('CpuPeriod', 0):
            cpus = host['CpuQuota'] / host['CpuPeriod']
        if host['NetworkMode'] != 'host' or abs(cpus - 1) > .00001 or host['Memory'] != 2048 * 1048576:
            raise ValueError('local test networking or resource limits changed')
        expected = {'/opt/crowdb/data':(str(self.data),True),
                    '/run/crowdb-node-policy.toml':(str(self.policy),False),
                    '/run/crowdb-ssh-password':(str(self.secret),False)}
        observed = {mount['Destination']:(mount['Source'],mount['RW']) for mount in current.get('Mounts',[])}
        if observed != expected:
            raise ValueError('local test mounts changed')
        environment = dict(value.split('=',1) for value in current['Config']['Env'] if '=' in value)
        for key,value in dict(CROWDB_DEPLOYMENT_MODE='local',CROWDB_INITIAL_VOTERS='1',
                              CROWDB_CLUSTER=self.cluster,CROWDB_MANAGEMENT_INTERFACE=self.interface,
                              TOKIO_WORKER_THREADS='1').items():
            if environment.get(key) != value:
                raise ValueError('local test startup policy changed')
        return current

    def command(self, arguments, expected=(0,), endpoint=None):
        assert self.client_digest() == self.cli_digest
        result = subprocess.run([str(self.cli),'--management-endpoint',endpoint or self.origin,*arguments],
                                env=self.env,text=True,capture_output=True,timeout=210)
        if result.returncode not in expected:
            print(redact(result.stdout + result.stderr),flush=True)
            raise AssertionError(f'local host client {arguments[:2]} exited {result.returncode}')
        return result.stdout

    def start(self, fresh=False):
        runtime = self.runtime()
        path = self.private / 'local-runtime.json'
        if not path.exists():
            if runtime.named('crowdb-' + self.cluster + '-evaluation'):
                raise ValueError('local evaluation scope already contains a foreign container')
            self.save(dict(version=1,owner=str(uuid.uuid4()),name='crowdb-' + self.cluster + '-evaluation',
                           runtime=runtime.identity(),policy='local',node_failure_tolerance=0,
                           options=dict(kind=self.kind,address=self.address,image=self.image,
                                        cluster=self.cluster,interface=self.interface,ip=self.ip)))
        record = self.record()
        current = self.owned(runtime,record)
        if current is None:
            identity = Path('/etc/machine-id').read_text().strip()
            runtime.invoke('create','--name',record['name'],'--label',f"{OWNER}={record['owner']}",
                '--label',f'org.crowdb.cluster={self.cluster}','--network','host','--cpus','1','--memory','2048m',
                '--mount',f'type=bind,source={self.data},target=/opt/crowdb/data',
                '--mount',f'type=bind,source={self.policy},target=/run/crowdb-node-policy.toml,readonly',
                '--mount',f'type=bind,source={self.secret},target=/run/crowdb-ssh-password,readonly',
                '--env','CROWDB_DEPLOYMENT_MODE=local','--env','CROWDB_INITIAL_VOTERS=1',
                '--env','TOKIO_WORKER_THREADS=1',
                '--env',f'CROWDB_CLUSTER={self.cluster}','--env',f'CROWDB_PHYSICAL_HOST_ID={identity}',
                '--env',f'CROWDB_MANAGEMENT_INTERFACE={self.interface}',
                '--env','CROWDB_NODE_POLICY_FILE=/run/crowdb-node-policy.toml',
                '--env','CROWDB_SSH_PASSWORD_FILE=/run/crowdb-ssh-password',self.image)
            current = self.owned(runtime,record)
        if not current['State']['Running']:
            runtime.invoke('start',current['Id'])
        current = self.owned(runtime,record)
        runtime.verify(current['Id'],1,2048,True)
        report = json.loads(self.command(['cluster','check','--wait','180','--json']))
        actual = report['observation']['cluster_id']
        assert record.get('cluster_id',actual) == actual
        record['cluster_id'] = actual
        self.save(record)
        print('Local host evaluation ready',self.kind,actual,'zero node-failure tolerance',flush=True)
        return report

    def facts(self):
        runtime,record = self.runtime(),self.record()
        current = self.owned(runtime,record)
        runtime.verify(current['Id'],1,2048,True)
        return dict(runtime=self.kind,daemon_id=record['runtime']['daemon'],container_id=current['Id'],
                    image=current['Image'],host_network=True,policy='local',node_failure_tolerance=0,
                    cpu_max=runtime.invoke('exec',current['Id'],'cat','/sys/fs/cgroup/cpu.max').strip(),
                    memory_max=runtime.invoke('exec',current['Id'],'cat','/sys/fs/cgroup/memory.max').strip(),
                    discovery_id=(self.data/'node-identity').read_text().strip())

    def recreate(self):
        self.cleanup()
        self.start()

    def cleanup(self):
        if not (self.private/'local-runtime.json').exists():
            return
        runtime,record = self.runtime(),self.record()
        current = self.owned(runtime,record)
        if current is None:
            return
        output = subprocess.run([*runtime.command,'logs',current['Id']],capture_output=True,text=True,timeout=60)
        (self.root/'node-logs.log').write_text(redact(output.stdout + output.stderr))
        if current['State']['Running']:
            runtime.invoke('stop','--time','30',current['Id'])
        current = self.owned(runtime,record)
        assert not current['State']['Running']
        runtime.invoke('rm',current['Id'])


def cleanup_local_host(root):
    root = Path(root)
    record = read_private(root/'private/local-runtime.json')
    if record is None:
        return
    if (record['version'] != 1 or record['policy'] != 'local' or record['node_failure_tolerance'] != 0
            or record['name'] != 'crowdb-' + record['options']['cluster'] + '-evaluation'
            or str(uuid.UUID(record['owner'])) != record['owner']):
        raise ValueError('invalid local host test ownership')
    node = LocalHostNode.__new__(LocalHostNode)
    node.root, node.private = root, root/'private'
    for key,value in record['options'].items():
        setattr(node,key,value)
    node.data, node.policy, node.secret = (node.private/'node',node.private/'node-policy.toml',node.private/'password')
    node.cleanup()
