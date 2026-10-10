#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Start a production node through the system Docker daemon with explicit resources."""
import argparse
import json
import os
from pathlib import Path
import stat
import subprocess


def docker(*args):
    return subprocess.run(["docker", *args], check=True, text=True, capture_output=True, timeout=60).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Local image digest (sha256:...) or immutable repo@sha256 digest")
    parser.add_argument("--name", required=True)
    parser.add_argument("--network", required=True, help="host or an existing dedicated Docker bridge")
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--physical-host-id", required=True)
    parser.add_argument("--interface", required=True)
    parser.add_argument("--cpus", required=True, type=float)
    parser.add_argument("--memory-mib", required=True, type=int)
    parser.add_argument("--ui-port", type=int)
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--device", action="append", default=[], type=Path)
    parser.add_argument("--seed", action="append", default=[])
    args = parser.parse_args()
    digest = args.image.split("@")[-1]
    if not digest.startswith("sha256:") or len(digest) != 71 or any(ch not in "0123456789abcdef" for ch in digest[7:]):
        parser.error("image must use an immutable SHA256 digest")
    image = json.loads(docker("image", "inspect", args.image))[0]
    if image["Os"] != "linux" or image["Architecture"] != "amd64":
        parser.error("this packaging currently supports Linux amd64")
    host_memory_mib = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // (1024 * 1024)
    if not (0 < args.cpus <= (os.cpu_count() or 1)) or not 512 <= args.memory_mib <= host_memory_mib:
        parser.error("CPU and memory boundaries must fit the host; memory must be at least 512 MiB")
    if not args.data_root.is_absolute() or args.data_root.is_symlink() or not args.data_root.is_dir():
        parser.error("data root must be an existing absolute persistent directory")
    metadata = args.data_root.stat()
    if metadata.st_uid != 10001 or not metadata.st_mode & stat.S_IWUSR:
        parser.error("data root must be writable by container uid 10001")
    if not args.physical_host_id or not args.interface:
        parser.error("physical host and management interface are required")
    if args.network == "host":
        if args.ui_port is not None:
            parser.error("host networking uses the node's port 9090 directly")
    else:
        network = json.loads(docker("network", "inspect", args.network))[0]
        if network["Driver"] != "bridge" or args.network == "bridge":
            parser.error("choose an existing dedicated Docker bridge")
        if not args.ui_port or not 1 <= args.ui_port <= 65535:
            parser.error("bridge deployment requires an explicit published UI port")
    command = ["run", "-d", "--name", args.name, "--network", args.network,
               "--cpus", str(args.cpus), "--memory", f"{args.memory_mib}m",
               "--mount", f"type=bind,source={args.data_root},target=/opt/crowdb/data",
               "-e", "CROWDB_STARTUP_MODE=manual", "-e", "CROWDB_DEPLOYMENT_MODE=production",
               "-e", f"CROWDB_PHYSICAL_HOST_ID={args.physical_host_id}",
               "-e", f"CROWDB_MANAGEMENT_INTERFACE={args.interface}"]
    if args.password_file:
        secret = args.password_file
        if not secret.is_absolute() or secret.is_symlink() or not secret.is_file() or secret.stat().st_mode & 0o077:
            parser.error("password file must be an absolute private regular file")
        password = secret.read_text().strip()
        if not password or len(password) > 1024 or "\n" in password or "\r" in password:
            parser.error("password file must contain one nonempty password")
        command += ["--mount", f"type=bind,source={secret},target=/run/crowdb-ssh-password,readonly",
                    "-e", "CROWDB_SSH_PASSWORD_FILE=/run/crowdb-ssh-password"]
    elif not (args.data_root / "ssh/authorized_keys").is_file():
        parser.error("production requires a password file or preinstalled authorized_keys")
    for device in args.device:
        if not device.is_absolute() or device.is_symlink() or not stat.S_ISBLK(device.stat().st_mode):
            parser.error("each permitted device must be an explicit absolute block-device path")
        command += ["--device", f"{device}:{device}:rwm"]
    if args.seed:
        command += ["-e", "CROWDB_DISCOVERY_SEEDS=" + ",".join(args.seed)]
    if args.ui_port:
        command += ["-p", f"{args.ui_port}:9090"]
    command.append(image["Id"])
    container = docker(*command).strip()
    actual = json.loads(docker("inspect", container))[0]
    if actual["HostConfig"]["Memory"] != args.memory_mib * 1024 * 1024 or actual["HostConfig"]["NanoCpus"] != round(args.cpus * 1_000_000_000):
        raise RuntimeError(f"Container {container} resources differ from the requested limits")
    print(container)


if __name__ == "__main__":
    main()
