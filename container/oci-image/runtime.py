# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host Docker/containerd commands and verified container resource boundaries."""

import json
import math
import os
import subprocess
import uuid

from builder import tool
from storage import inputs, record_inputs, runtime_arguments, validate_retained_inputs

OWNER = "org.crowdb.launcher"


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
        return result.stdout

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
        disks = inputs(args)
        validate_retained_inputs(args.data_root, disks)
        token = str(uuid.uuid4())
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
        command += runtime_arguments(disks)
        if args.seed:
            command += ["-e", "CROWDB_DISCOVERY_SEEDS=" + ",".join(args.seed)]
        if args.ui_port:
            command += ["-p", f"127.0.0.1:{args.ui_port}:9090"]
        try:
            container = self.invoke(*command, args.image).strip()
            self.verify(container, args.cpus, args.memory_mib, args.network == "host", shares)
            for disk in disks:
                kind = "-f" if disk["backend"] == "file" else "-b"
                self.invoke("exec", "--user", "10001:10001", container, "sh", "-c",
                            'test "$1" "$2" && test -r "$2" && test -w "$2"', "sh", kind, disk["path"])
            record_inputs(args.data_root, disks)
            return container
        except BaseException:
            # A failed create can leave a container; only this operation owns it.
            try:
                current = self.inspect(args.name)
                if current.get("Config", {}).get("Labels", {}).get(OWNER) == token:
                    print(f"Startup failure state: {json.dumps(current.get('State', {}))}", flush=True)
                    try:
                        print(self.invoke("logs", args.name), flush=True)
                    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as log_error:
                        print(f"Startup failure logs unavailable: {log_error}", flush=True)
                    self.invoke("rm", "-f", args.name)
            except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as cleanup_error:
                print(f"Inspect/cleanup after launch failure: {cleanup_error}", flush=True)
            raise
