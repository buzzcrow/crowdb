# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Owned process groups and structured server readiness."""

import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid


class Runtime:
    def __init__(self, root, output, pixi, topology="native", image="crowdb-node:dev"):
        self.root, self.output, self.pixi = root, output, pixi
        self.topology, self.image = topology, image
        self.names = {}
        self.server_pid = None

    def command(self, argv, env, side, cpus=None):
        command = (["taskset", "-c", cpus] if cpus else []) + [str(arg) for arg in argv]
        if self.topology == "native":
            return [self.pixi, "run", "--", *command]
        name = "crowdb-rpc-bench-" + side + "-" + uuid.uuid4().hex[:12]
        self.names[side] = name
        lib = self.root / ".pixi/envs/default/lib"
        docker = ["docker", "run", "--rm", "--init", "--name", name, "--network=host",
                  "--user", f"{os.getuid()}:{os.getgid()}", "--ulimit", "memlock=-1:-1",
                  "--volume", f"{self.root}:{self.root}:ro", "--volume", f"{self.output}:{self.output}:rw",
                  "--volume", f"{lib}:/tmp/crowdb-libs:ro", "--workdir", str(self.root)]
        if self.topology == "containers":
            docker += ["--device", "/dev/infiniband/rdma_cm"]
            for device in sorted(Path("/dev/infiniband").glob("uverbs*")):
                docker += ["--device", str(device)]
        for key, value in env.items():
            if key.startswith("CROWDB_RDMA_") or key in ("CROWDB_RUNTIME_ROOT", "CROWDB_LOG_ROOT", "CROWDB_PORT_OFFSET"):
                docker += ["--env", f"{key}={value}"]
        docker += ["--env", "LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/opt/crowdb/lib:/tmp/crowdb-libs",
                   "--env", "LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libibverbs.so.1:/usr/lib/x86_64-linux-gnu/librdmacm.so.1",
                   "--env", "RDMAV_DRIVERS_DIR=/opt/crowdb/lib/libibverbs", "--entrypoint", command[0],
                   self.image, *command[1:]]
        return [self.pixi, "run", "--", *docker]

    def stop(self, process, side):
        if process.poll() is not None:
            return process.returncode
        if side in self.names:
            subprocess.run([self.pixi, "run", "--", "docker", "stop", "--time", "5", self.names[side]],
                           check=False, timeout=15)
        else:
            pid = self.server_pid if side == "server" else None
            try:
                if pid is not None and os.getpgid(pid) == process.pid:
                    os.kill(pid, signal.SIGTERM)
                else:
                    os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            return process.wait(timeout=6)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            if side in self.names:
                subprocess.run([self.pixi, "run", "--", "docker", "rm", "-f", self.names[side]],
                               check=False, timeout=15)
            return process.wait(timeout=5)


def start(command, env, log, root):
    return subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)


def ready(process, status_path, transport, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited before readiness: {process.returncode}")
        try:
            status = json.loads(status_path.read_text())
            if status.get("schema_version") != 1:
                raise RuntimeError("unsupported startup status schema")
            native = status.get("rdma") or {}
            if transport == "rdma" and not (native.get("listen_active") is True
                                            and native.get("ready") is True
                                            and native.get("cm_port", 0) > 0
                                            and status.get("allow_tcp_fallback") is False):
                raise RuntimeError("server did not establish a strict RDMA listener")
            return status
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.05)
    raise RuntimeError("server readiness deadline expired")


def wait_client(client, server, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.poll() is not None:
            return client.returncode
        if server.poll() is not None:
            raise RuntimeError(f"server exited during workload: {server.returncode}")
        time.sleep(0.05)
    raise RuntimeError("client deadline expired")
