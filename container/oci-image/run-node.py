#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Launch a digest-pinned node using host Docker or Pixi nerdctl."""

import argparse
import os
import platform
import stat
from pathlib import Path

from runtime import Runtime


def digest_reference(reference):
    digest = reference.split("@")[-1]
    return digest.startswith("sha256:") and len(digest) == 71 and all(c in "0123456789abcdef" for c in digest[7:])


def validate(args, parser):
    if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
        parser.error("Linux amd64 host required; macOS VM backend is reserved")
    if not digest_reference(args.image):
        parser.error("image must use an immutable SHA256 digest")
    if not Path("/sys/fs/cgroup/cgroup.controllers").is_file():
        parser.error("production requires cgroup v2 CPU/memory controllers")
    controllers = Path("/sys/fs/cgroup/cgroup.controllers").read_text().split()
    if not {"cpu", "memory"}.issubset(controllers):
        parser.error("CPU/memory cgroup controllers are unavailable")
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
    for device in args.device:
        if not device.is_absolute() or device.is_symlink() or not stat.S_ISBLK(device.stat().st_mode):
            parser.error("each permitted device must be an explicit absolute block-device path")
    if not args.interface or not args.physical_host_id:
        parser.error("management interface and physical host identity are required")
    if args.network == "host":
        if args.ui_port is not None:
            parser.error("host networking uses the node's port 9090 directly")
    elif args.runtime != "docker":
        parser.error("initial containerd production support requires host networking")
    elif not args.ui_port or not 1 <= args.ui_port <= 65535:
        parser.error("bridge deployment requires an explicit published UI port")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", choices=["docker", "containerd"], default="docker")
    parser.add_argument("--address", default="/run/containerd/containerd.sock")
    parser.add_argument("--namespace", default="crowdb")
    parser.add_argument("--snapshotter", choices=["overlayfs", "native"], default="overlayfs")
    parser.add_argument("--image", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--network", default="host")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--physical-host-id", required=True)
    parser.add_argument("--interface", required=True)
    parser.add_argument("--cpus", type=float, required=True)
    parser.add_argument("--memory-mib", type=int, required=True)
    parser.add_argument("--ui-port", type=int)
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--device", type=Path, action="append", default=[])
    parser.add_argument("--seed", action="append", default=[])
    parser.add_argument("--startup-mode", choices=["manual", "single"], default="manual")
    args = parser.parse_args()
    validate(args, parser)
    runtime = Runtime(args.runtime, args.address, args.namespace, args.snapshotter)
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
