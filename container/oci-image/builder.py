# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Task-owned standalone BuildKit process lifecycle."""

import contextlib
import os
import platform
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path


def tool(name):
    prefix = Path(os.environ["CONDA_PREFIX"])
    binary = prefix / "bin" / name
    if not binary.is_file():
        raise RuntimeError(f"{name} must be installed in the container Pixi environment")
    return str(binary)


def preflight(privileged):
    if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
        raise RuntimeError("Linux amd64 builder required; macOS Linux-VM backend is reserved")
    for name in ("buildctl", "buildkitd", "runc"):
        tool(name)
    if privileged:
        if os.geteuid() != 0:
            raise RuntimeError("--privileged requires an explicitly administrator-started Pixi environment")
    else:
        tool("rootlesskit")
        for name in ("newuidmap", "newgidmap"):
            if not shutil.which(name):
                raise RuntimeError(f"rootless builds require host setuid {name} and subordinate UID/GID ranges")


@contextlib.contextmanager
def running(cache, privileged=False, mirrors=()):
    preflight(privileged)
    cache.mkdir(parents=True, exist_ok=True)
    # BuildKit owns a persistent exclusive state lock; concurrent builds use
    # different cache roots rather than sharing a writable daemon state tree.
    with tempfile.TemporaryDirectory(prefix="crowdb-buildkit-") as transient:
        socket = f"unix://{transient}/buildkit.sock"
        command = [tool("buildkitd"), "--addr", socket, "--root", str(cache),
                   "--containerd-worker=false", "--oci-worker=true",
                   "--oci-worker-binary", tool("runc"), "--oci-worker-snapshotter=native",
                   "--oci-worker-net=host"]
        if mirrors:
            import json
            config = Path(transient) / "buildkit.toml"
            config.write_text('[registry."docker.io"]\nmirrors = ' + json.dumps(list(mirrors)) + '\n')
            command += ["--config", str(config)]
        if not privileged:
            command = [tool("rootlesskit"), "--net=host", "--copy-up=/etc", "--copy-up=/run", *command, "--rootless"]
        with open(Path(transient) / "daemon.log", "w+") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("BuildKit startup failed; check rootless host prerequisites")
                    if Path(transient, "buildkit.sock").exists():
                        probe = subprocess.run([tool("buildctl"), "--addr", socket, "debug", "workers"], check=False, capture_output=True, timeout=3)
                        if probe.returncode == 0:
                            yield socket
                            return
                    time.sleep(0.1)
                raise TimeoutError("BuildKit socket readiness exceeded 30 seconds")
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                log.seek(0)
                print(log.read(), end="", flush=True)
