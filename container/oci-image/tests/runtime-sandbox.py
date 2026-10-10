# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Provide an isolated Linux host for containerd acceptance on a developer PC."""

import subprocess
import uuid
from pathlib import Path

root = Path(__file__).resolve().parents[3]
pixi = Path(subprocess.check_output(["which", "pixi"], text=True).strip()).resolve()
name = "crowdb-oci-runtime-" + uuid.uuid4().hex[:8]
command = ["docker", "run", "--rm", "--privileged", "--cgroupns", "host", "--name", name,
           "--mount", f"type=bind,source={root},target={root},readonly",
           "--mount", f"type=bind,source={pixi},target=/usr/local/bin/pixi,readonly",
           "--mount", "type=bind,source=/etc/ssl/certs,target=/etc/ssl/certs,readonly",
           "--workdir", str(root), "ubuntu:24.04", "/bin/bash", "-euc",
           ('! command -v docker; test ! -e /var/run/docker.sock; '
           'pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime '
           'python container/oci-image/tests/containerd-host.py target/crowdb.oci.tar')]
try:
    subprocess.run(command, check=True)
finally:
    result = subprocess.run(["docker", "container", "inspect", name], check=False, capture_output=True)
    if result.returncode == 0:
        subprocess.run(["docker", "rm", "-f", name], check=True)
