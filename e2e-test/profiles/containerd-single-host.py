# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Manual and single-node acceptance inside one disposable privileged Linux host."""

import json
import os
from pathlib import Path
import signal
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def docker(*args):
    return subprocess.check_output(["docker", *args],text=True,timeout=60).strip()


def main():
    prefix = os.environ["CROWDB_CONTAINERD_PREFIX"]
    archive = Path(os.environ["CROWDB_CONTAINERD_ARCHIVE"])
    data = Path(os.environ["CROWDB_CONTAINERD_DATA"])
    pixi = Path(subprocess.check_output(["which","pixi"],text=True).strip()).resolve()
    name = prefix + "-host"
    try:
        docker("run","-d","--name",name,"--label",f"org.crowdb.e2e.run={prefix}",
            "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1","--memory","1536m","--privileged","--cgroupns","host",
            "--mount",f"type=bind,source={ROOT},target={ROOT},readonly",
            "--mount",f"type=bind,source={pixi},target=/usr/local/bin/pixi,readonly",
            "--mount","type=bind,source=/etc/ssl/certs,target=/etc/ssl/certs,readonly",
            "--mount",f"type=bind,source={data},target=/opt/crowdb-e2e/data",
            "--mount",f"type=bind,source={archive},target=/opt/crowdb-e2e/archive.tar,readonly",
            "--mount",f"type=bind,source={os.environ['CROWDB_CONTAINERD_MEDIA_CLIENT']},target=/opt/crowdb-e2e/client,readonly",
            "--mount",f"type=bind,source={archive}.json,target=/opt/crowdb-e2e/archive.tar.json,readonly",
            "--env","CROWDB_CONTAINERD_DATA=/opt/crowdb-e2e/data","--workdir",str(ROOT),
            os.environ["CROWDB_CONTAINERD_HOST_IMAGE"],"/bin/bash","-euc",
            '! command -v docker; test ! -e /var/run/docker.sock; '
            'pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime '
            'python e2e-test/profiles/containerd-host.py /opt/crowdb-e2e/archive.tar')
        # The host/client environment is separate from the formal node image.
        # Docker wait returns the worker's actual status, including failures.
        status = int(subprocess.check_output(["docker","wait",name],text=True))
        print(docker("logs",name),flush=True)
        if status:
            raise RuntimeError(f"isolated containerd host acceptance exited {status}")
    finally:
        docker("rm","-f",name)


if __name__ == "__main__":
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt("owned containerd host interrupted")
    signal.signal(signal.SIGTERM,interrupt)
    main()
