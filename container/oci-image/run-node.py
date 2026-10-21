#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Launch a digest-pinned node using host Docker or Pixi nerdctl."""

import argparse
import os
import platform
import re
import socket
import stat
from pathlib import Path

from runtime import Runtime
from storage import inputs


def digest_reference(reference):
    digest = reference.split("@")[-1]
    return digest.startswith("sha256:") and len(digest) == 71 and all(c in "0123456789abcdef" for c in digest[7:])


# Fixed in-image listeners; host networking admits only one node per host.
HOST_PORTS = (2222, 7100, 7101, 7200, 7300, 7301, 7400, 7401,
              9090, 9091, 9092, 9093, 9094, 9095, 10000, 10100)


def check_host_ports(ports=HOST_PORTS):
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            try:
                listener.bind(("0.0.0.0", port))
            except OSError as error:
                raise ValueError(f"host node TCP port {port} is already occupied") from error


def validate(args, parser):
    if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
        parser.error("Linux amd64 host required; macOS VM backend is reserved")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,47}", args.cluster):
        parser.error("cluster must be 1–48 ASCII letters, digits, dots, underscores or hyphens")
    if not digest_reference(args.image):
        parser.error("image must use an immutable SHA256 digest")
    if not Path("/sys/fs/cgroup/cgroup.controllers").is_file():
        parser.error("production requires cgroup v2 CPU/memory controllers")
    controllers = Path("/sys/fs/cgroup/cgroup.controllers").read_text().split()
    if not {"cpu", "memory"}.issubset(controllers):
        parser.error("CPU/memory cgroup controllers are unavailable")
    if args.cpu_shares is not None and not 2 <= args.cpu_shares <= 262144:
        parser.error("CPU shares must be between 2 and 262144")
    if args.worker_threads is not None and args.worker_threads < 1:
        parser.error("worker thread count must be positive")
    available = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // (1024 * 1024)
    if not (0 < args.cpus <= (os.cpu_count() or 1)) or not 512 <= args.memory_mib <= available:
        parser.error("CPU and memory boundaries must fit the host; memory must be at least 512 MiB")
    if not args.data_root.is_absolute() or args.data_root.is_symlink() or not args.data_root.is_dir():
        parser.error("data root must be an existing absolute persistent directory")
    metadata = args.data_root.stat()
    if metadata.st_uid != 10001 or not metadata.st_mode & stat.S_IWUSR:
        parser.error("data root must be writable by container uid 10001")
    if args.password_file:
        secret = args.password_file
        if not secret.is_absolute() or secret.is_symlink() or not secret.is_file() or secret.stat().st_mode & 0o077:
            parser.error("password file must be an absolute private regular file")
        password = secret.read_text().strip()
        if not password or len(password) > 1024 or "\n" in password or "\r" in password:
            parser.error("password file must contain one nonempty password")
    elif not (args.data_root / "ssh/authorized_keys").is_file():
        parser.error("production requires a password file or preinstalled authorized_keys")
    try:
        inputs(args)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if not args.interface or not args.physical_host_id:
        parser.error("management interface and physical host identity are required")
    if args.network == "host":
        if args.ui_port is not None:
            parser.error("host networking uses the node's port 9090 directly")
        try:
            socket.if_nametoindex(args.interface)
            check_host_ports()
        except (OSError, ValueError) as error:
            parser.error(str(error))
    elif args.runtime != "docker":
        parser.error("initial containerd production support requires host networking")
    elif not args.ui_port or not 1 <= args.ui_port <= 65535:
        parser.error("bridge deployment requires an explicit published UI port")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", choices=["docker", "containerd"], default="docker")
    parser.add_argument("--address", default="/run/containerd/containerd.sock")
    parser.add_argument("--snapshotter", choices=["overlayfs", "native"], default="overlayfs")
    parser.add_argument("--image", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--network", default="host")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--physical-host-id", required=True)
    parser.add_argument("--interface", required=True)
    cpu = parser.add_mutually_exclusive_group(required=True)
    cpu.add_argument("--cpus", type=float, default=1)
    cpu.add_argument("--cpu-shares", type=int, help="Relative CPU weight instead of a hard quota")
    parser.add_argument("--worker-threads", type=int, help="Explicit Tokio worker count inherited by node services")
    parser.add_argument("--memory-mib", type=int, required=True)
    parser.add_argument("--ui-port", type=int)
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--device", type=Path, action="append", default=[])
    parser.add_argument("--disk-file", type=Path, action="append", default=[],
                        help="Existing disk file, mapped beneath /opt/crowdb/data/disks by filename")
    parser.add_argument("--seed", action="append", default=[])
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--initial-voters", type=int, choices=(1, 3), default=3)
    args = parser.parse_args()
    validate(args, parser)
    runtime = Runtime(args.runtime, args.address, "crowdb-" + args.cluster, args.snapshotter)
    image = runtime.image(args.image)
    if (image["Os"], image["Architecture"]) != ("linux", "amd64"):
        parser.error("this packaging supports Linux amd64")
    if args.network != "host":
        network = __import__("json").loads(runtime.invoke("network", "inspect", args.network))[0]
        if network["Driver"] != "bridge" or args.network == "bridge":
            parser.error("choose an existing dedicated Docker bridge")
    print(runtime.create(args))


if __name__ == "__main__":
    main()
