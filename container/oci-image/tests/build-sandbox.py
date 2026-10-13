# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Verify Docker-free BuildKit in a disposable Linux host sandbox.

Docker is only the external test sandbox provider; it is absent inside the
builder, and no Docker socket or containerd service is provided to the build.
"""

import argparse
import json
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="target/crowdb.oci.tar")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    pixi = Path(subprocess.check_output(["which", "pixi"], text=True).strip()).resolve()
    name = "crowdb-oci-build-" + uuid.uuid4().hex[:8]
    output = (root / args.output).resolve()
    context = root / "target/container-runtime"
    cache = root / "target/oci-sandbox-cache"
    cache.mkdir(exist_ok=True)
    # External sandbox setup only: replicate this host's registry routing.
    mirrors = json.loads(subprocess.check_output(["docker", "info", "--format", "{{json .RegistryConfig.Mirrors}}"], text=True)) or []
    options = []
    for mirror in mirrors:
        parsed = urlsplit(mirror)
        if parsed.username or parsed.password or parsed.scheme != "https":
            raise ValueError("sandbox mirrors must use credential-free HTTPS")
        options += ["--registry-mirror", (parsed.netloc + parsed.path).rstrip("/")]
    command = ["docker", "run", "--rm", "--name", name, "--privileged",
               "--mount", f"type=bind,source={root},target={root},readonly",
               "--mount", f"type=bind,source={root / 'target'},target={root / 'target'}",
               "--mount", f"type=bind,source={cache},target=/var/lib/crowdb-buildkit",
               "--mount", f"type=bind,source={pixi},target=/usr/local/bin/pixi,readonly",
               "--mount", "type=bind,source=/etc/ssl/certs,target=/etc/ssl/certs,readonly",
               "--workdir", str(root), "ubuntu:24.04", "/bin/bash", "-euc",
               ('test ! -e /var/run/docker.sock; ! command -v docker; ! command -v containerd; '
               'pixi run --as-is --manifest-path container/oci-image/pixi.toml build '
               '--privileged --context "$1" --output "$2" --cache /var/lib/crowdb-buildkit "${@:3}"'),
               "_", str(context), str(output), *options]
    try:
        subprocess.run(command, check=True)
    finally:
        result = subprocess.run(["docker", "container", "inspect", name], check=False, capture_output=True)
        if result.returncode == 0:
            subprocess.run(["docker", "rm", "-f", name], check=True)


if __name__ == "__main__":
    main()
