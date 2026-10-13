# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Prepare the disposable CI builder and expose prerequisite failures."""

import subprocess
import socket
import stat
import sys
from pathlib import Path


def remove_stale_socket(path):
    try:
        before = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(before.st_mode):
        raise RuntimeError(f"refusing to remove non-socket path: {path}")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(1)
        try:
            probe.connect(str(path))
        except ConnectionRefusedError:
            pass
        else:
            raise RuntimeError(f"container listener remains active: {path}")
    after = path.lstat()
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise RuntimeError(f"socket ownership changed before cleanup: {path}")
    path.unlink()


def run(label, command):
    print(f"::group::{label}", flush=True)
    output = []
    with subprocess.Popen(command, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT) as process:
        for line in process.stdout:
            print(line, end="", flush=True)
            output.append(line)
        code = process.wait()
    print("::endgroup::", flush=True)
    if code:
        diagnostic = "".join(output).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::error title={label}::{' '.join(command)} exited {code}: {diagnostic}", flush=True)
        raise SystemExit(code)


def main():
    if sys.argv[1:] == ["--remove-stale-socket"]:
        remove_stale_socket(Path("/var/run/docker.sock"))
        return
    for label, command in (
        ("Refresh build prerequisites", ["sudo", "apt-get", "update"]),
        ("Install protobuf", ["sudo", "apt-get", "install", "-y", "protobuf-compiler"]),
        ("Stop preinstalled container services", ["sudo", "systemctl", "stop",
            "docker.service", "docker.socket", "containerd.service"]),
        ("Remove preinstalled Docker packages", ["sudo", "apt-get", "purge", "-y",
            "docker-ce", "docker-ce-cli", "containerd.io", "docker-buildx-plugin", "docker-compose-plugin"]),
        ("Verify Docker CLI is absent", ["bash", "-euc", "! command -v docker"]),
        ("Remove stopped Docker socket", ["sudo", sys.executable, __file__, "--remove-stale-socket"]),
        ("Verify Docker socket is absent", ["bash", "-euc", "test ! -S /var/run/docker.sock"]),
    ):
        run(label, command)


if __name__ == "__main__":
    main()
