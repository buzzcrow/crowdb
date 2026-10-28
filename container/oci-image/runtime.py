# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host Docker/containerd commands and verified container resource boundaries."""

from ports import WEB_BASE

import json
import math
import os
import subprocess
import stat
import tomllib
import uuid
from pathlib import Path

from builder import tool
from storage import inputs, record_inputs, runtime_arguments, validate_retained_inputs

OWNER = "org.crowdb.launcher"


def node_policy_file(args):
    """Check the bounded startup file; the monitor validates and seals its semantics."""
    path = getattr(args, "node_policy_file", None)
    if path is None:
        return None
    path = Path(path)
    if (not path.is_absolute() or any(part.is_symlink() for part in (path, *path.parents))
            or any(character in str(path) for character in (",", "\n", "\r"))):
        raise ValueError("node policy must be an absolute non-symlink path without mount delimiters")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 32768:
            raise ValueError("node policy must be a regular TOML file of at most 32768 bytes")
        if not (metadata.st_mode & 0o004
                or metadata.st_uid == 10001 and metadata.st_mode & 0o400
                or metadata.st_gid == 10001 and metadata.st_mode & 0o040):
            raise ValueError("node policy must be readable by container uid 10001")
        content = b""
        while len(content) < 32769:
            part = os.read(descriptor, 32769 - len(content))
            if not part:
                break
            content += part
    finally:
        os.close(descriptor)
    if len(content) > 32768:
        raise ValueError("node policy exceeds 32768 bytes")
    try:
        tomllib.loads(content.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError):
        raise ValueError("node policy is not valid UTF-8 TOML") from None
    return path


class Runtime:
    def __init__(self, name, address="/run/containerd/containerd.sock", namespace="crowdb", snapshotter="overlayfs"):
        self.name = name
        if name == "docker":
            self.command = ["docker"]
        else:
            if namespace == "k8s.io" or not namespace:
                raise ValueError("use a dedicated non-Kubernetes containerd namespace")
            self.command = [tool("nerdctl"), "--address", address, "--namespace", namespace, "--snapshotter", snapshotter]

    def invoke(self, *args):
        result = subprocess.run([*self.command, *map(str, args)], check=False, text=True, capture_output=True, timeout=60)
        if result.returncode:
            raise RuntimeError(f"{self.name} {args[0]} failed: {result.stderr}")
        return result.stdout + result.stderr if args and args[0] == 'logs' else result.stdout

    def inspect(self, name, image=False):
        return json.loads(self.invoke(*(["image", "inspect"] if image else ["inspect"]), name))[0]

    def image(self, reference):
        # nerdctl digest inspection is independent of repository tag aliases.
        identity = reference.rsplit("@", 1)[-1] if self.name == "containerd" else reference
        try:
            return self.inspect(identity, image=True)
        except RuntimeError:
            if "@sha256:" not in reference:
                raise
            self.invoke("pull", reference)
            return self.inspect(identity, image=True)

    def verify(self, name, cpus, memory_mib, host_network, cpu_shares=None):
        cpu = self.invoke("exec", name, "cat", "/sys/fs/cgroup/cpu.max").split()
        memory = self.invoke("exec", name, "cat", "/sys/fs/cgroup/memory.max").strip()
        if cpu_shares is not None:
            # runc supports the legacy linear and current default-preserving mapping.
            # https://github.com/opencontainers/cgroups/blob/v0.0.6/utils.go
            logarithm = math.log2(cpu_shares)
            expected = {1 + (cpu_shares - 2) * 9999 // 262142,
                        math.ceil(10 ** ((logarithm ** 2 + 125 * logarithm) / 612 - 7 / 34))}
            weight = self.invoke("exec", name, "cat", "/sys/fs/cgroup/cpu.weight").strip()
            if cpu[0] != "max" or int(weight) not in expected:
                raise RuntimeError(f"applied CPU shares differ: cpu.max={cpu}, weight={weight}, expected={sorted(expected)}")
        elif cpu[0] == "max" or abs(int(cpu[0]) / int(cpu[1]) - cpus) > 0.00001:
            raise RuntimeError("applied CPU quota differs from request")
        if memory == "max" or int(memory) != memory_mib * 1024 * 1024:
            raise RuntimeError("applied memory limit differs from request")
        if host_network:
            observed = self.invoke("exec", name, "readlink", "/proc/self/ns/net").strip()
            if observed != os.readlink("/proc/self/ns/net"):
                raise RuntimeError("container does not share this host network namespace")

    def create(self, args):
        policy = node_policy_file(args)
        disks = inputs(args)
        validate_retained_inputs(args.data_root, disks)
        token = getattr(args, "owner_token", None) or str(uuid.uuid4())
        shares = getattr(args, "cpu_shares", None)
        cpu_args = ["--cpu-shares", str(shares)] if shares is not None else ["--cpus", str(args.cpus)]
        command = ["run", "-d", "--name", args.name, "--label", f"{OWNER}={token}",
                   "--label", f"org.crowdb.cluster={args.cluster}",
                   "--network", args.network, *cpu_args, "--memory", f"{args.memory_mib}m",
                   "--restart", "unless-stopped", "--cgroupns", "private",
                   "--mount", f"type=bind,source={args.data_root},target=/opt/crowdb/data",
                   "-e", f"CROWDB_CLUSTER={args.cluster}",
                   "-e", f"CROWDB_INITIAL_VOTERS={args.initial_voters}",
                   "-e", "CROWDB_DEPLOYMENT_MODE=production",
                   "-e", f"CROWDB_PHYSICAL_HOST_ID={args.physical_host_id}",
                   "-e", f"CROWDB_MANAGEMENT_INTERFACE={args.interface}"]
        workers = getattr(args, "worker_threads", None)
        if workers is not None:
            command += ["-e", f"TOKIO_WORKER_THREADS={workers}"]
        if args.password_file:
            command += ["--mount", f"type=bind,source={args.password_file},target=/run/crowdb-ssh-password,readonly",
                        "-e", "CROWDB_SSH_PASSWORD_FILE=/run/crowdb-ssh-password"]
        if policy is not None:
            command += ["--mount", f"type=bind,source={policy},target=/run/crowdb-node-policy.toml,readonly",
                        "-e", "CROWDB_NODE_POLICY_FILE=/run/crowdb-node-policy.toml"]
        command += runtime_arguments(disks)
        if args.seed:
            command += ["-e", "CROWDB_DISCOVERY_SEEDS=" + ",".join(args.seed)]
        if args.ui_port:
            command += ["-p", f"127.0.0.1:{args.ui_port}:{WEB_BASE}"]
        try:
            container = self.invoke(*command, args.image).strip()
            self.verify(container, args.cpus, args.memory_mib, args.network == "host", shares)
            for disk in disks:
                kind = "-f" if disk["backend"] == "file" else "-b"
                self.invoke("exec", "--user", "10001:10136", container, "sh", "-c",
                            'test "$1" "$2" && test -r "$2" && test -w "$2"', "sh", kind, disk["path"])
            record_inputs(args.data_root, disks)
            return container
        except BaseException:
            # A failed verification must retain owned runtime and storage state
            # for diagnosis/resume. An independent explicit cleanup owns removal.
            try:
                current = self.inspect(args.name)
                if current.get("Config", {}).get("Labels", {}).get(OWNER) == token:
                    print(f"Startup failure state: {json.dumps(current.get('State', {}))}", flush=True)
                    try:
                        print(self.invoke("logs", args.name), flush=True)
                    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as log_error:
                        print(f"Startup failure logs unavailable: {log_error}", flush=True)
                    print(f"Owned failed container retained: {args.name}", flush=True)
            except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as cleanup_error:
                print(f"Inspect/cleanup after launch failure: {cleanup_error}", flush=True)
            raise
