# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""An owned rootful containerd daemon on a Docker-free Linux host."""

import json
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / 'container/oci-image'))
from runtime import Runtime


def docker_free(*, allow_container_host=False):
    if shutil.which('docker') or Path('/var/run/docker.sock').exists():
        raise ValueError('native containerd acceptance requires no Docker client or socket')
    if not allow_container_host and (Path('/.dockerenv').exists()
            or Path('/run/.containerenv').exists() or os.environ.get('container')):
        raise ValueError('native host acceptance cannot substitute a container-provisioned Linux host')
    for path in Path('/proc').glob('[0-9]*/comm'):
        try:
            if path.read_text().strip() == 'dockerd':
                raise ValueError('native containerd acceptance requires no Docker daemon')
        except FileNotFoundError:
            continue


class NativeRuntime:
    def __init__(self, root, cluster, *, docker_provisioned=False):
        self.root = Path(root)
        self.root.mkdir(mode=0o700)
        self.cluster = cluster
        self.docker_provisioned = docker_provisioned
        self.prefix = Path(os.environ.get('CROWDB_OCI_RUNTIME_PREFIX',
            ROOT / 'container/oci-image/.pixi/envs/runtime')).resolve()
        for name in ('containerd', 'containerd-shim-runc-v2', 'nerdctl', 'runc'):
            if not (self.prefix / 'bin' / name).is_file():
                raise ValueError('install the locked OCI runtime Pixi environment before host acceptance')
        os.environ['CONDA_PREFIX'] = str(self.prefix)
        os.environ['PATH'] = str(self.prefix / 'bin') + ':' + os.environ.get('PATH', '')
        self.socket_root = Path(tempfile.mkdtemp(prefix='crowdb-ctd-'))
        self.socket = str(self.socket_root / 'containerd.sock')
        self.runtime = Runtime('containerd', self.socket, 'crowdb-' + cluster, 'native')
        self.process, self.log = None, None

    def __enter__(self):
        docker_free(allow_container_host=self.docker_provisioned)
        if os.geteuid() != 0:
            raise ValueError('native host acceptance needs an explicitly privileged disposable Linux host')
        binary = str(self.prefix / 'bin/containerd')
        config = subprocess.run([binary, 'config', 'default'], capture_output=True,
                                text=True, check=True, timeout=10).stdout
        # Retain the pinned runtime's complete defaults, including its diff
        # service, while isolating every daemon socket and configuration import.
        config = re.sub(r'(?m)^imports\s*=.*$', 'imports = []', config)
        disabled = ["io.containerd.cri.v1.images", "io.containerd.cri.v1.runtime",
                    "io.containerd.grpc.v1.cri", "io.containerd.podsandbox.controller.v1.podsandbox",
                    "io.containerd.nri.v1.nri"]
        config = re.sub(r'(?m)^disabled_plugins\s*=.*$', 'disabled_plugins = ' + json.dumps(disabled), config)
        if '/run/containerd/containerd.sock' not in config:
            raise ValueError('pinned containerd default socket configuration is unsupported')
        config = config.replace('/run/containerd/containerd.sock', self.socket)
        config += '\n[[plugins."io.containerd.transfer.v1.local".unpack_config]]\nplatform="linux/amd64"\nsnapshotter="native"\ndiffer="walking"\n'
        path = self.root / 'config.toml'
        path.write_text(config)
        self.log = (self.root / 'daemon.log').open('w')
        self.process = subprocess.Popen([binary, '--config', str(path), '--root', str(self.root / 'content'),
            '--state', str(self.socket_root / 'state'), '--address', self.socket],
            stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            while True:
                if self.process.poll() is not None:
                    raise RuntimeError('owned containerd exited; see daemon.log')
                try:
                    self.info = json.loads(self.runtime.invoke('info', '--format', '{{json .}}'))
                    break
                except RuntimeError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(.1)
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process and self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        if self.log:
            self.log.close()

    def __exit__(self, *_failure):
        self.close()
