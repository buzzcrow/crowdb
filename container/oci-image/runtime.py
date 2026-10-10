# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host Docker/containerd commands and verified container resource boundaries."""

import json
import os
import subprocess
import uuid

from builder import tool

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

    def verify(self, name, cpus, memory_mib, host_network):
        cpu = self.invoke("exec", name, "cat", "/sys/fs/cgroup/cpu.max").split()
        memory = self.invoke("exec", name, "cat", "/sys/fs/cgroup/memory.max").strip()
        if cpu[0] == "max" or abs(int(cpu[0]) / int(cpu[1]) - cpus) > 0.00001:
            raise RuntimeError("applied CPU quota differs from request")
        if memory == "max" or int(memory) != memory_mib * 1024 * 1024:
            raise RuntimeError("applied memory limit differs from request")
        if host_network:
            observed = self.invoke("exec", name, "readlink", "/proc/self/ns/net").strip()
            if observed != os.readlink("/proc/self/ns/net"):
                raise RuntimeError("container does not share this host network namespace")

    def create(self, args):
        token = str(uuid.uuid4())
        command = ["run", "-d", "--name", args.name, "--label", f"{OWNER}={token}",
                   "--network", args.network, "--cpus", str(args.cpus), "--memory", f"{args.memory_mib}m",
                   "--restart", "unless-stopped", "--cgroupns", "private",
                   "--mount", f"type=bind,source={args.data_root},target=/opt/crowdb/data",
                   "-e", f"CROWDB_STARTUP_MODE={args.startup_mode}",
                   "-e", "CROWDB_DEPLOYMENT_MODE=production",
                   "-e", f"CROWDB_PHYSICAL_HOST_ID={args.physical_host_id}",
                   "-e", f"CROWDB_MANAGEMENT_INTERFACE={args.interface}"]
        if args.password_file:
            command += ["--mount", f"type=bind,source={args.password_file},target=/run/crowdb-ssh-password,readonly",
                        "-e", "CROWDB_SSH_PASSWORD_FILE=/run/crowdb-ssh-password"]
        for device in args.device:
            command += ["--device", f"{device}:{device}:rwm"]
        if args.seed:
            command += ["-e", "CROWDB_DISCOVERY_SEEDS=" + ",".join(args.seed)]
        if args.ui_port:
            command += ["-p", f"127.0.0.1:{args.ui_port}:9090"]
        try:
            container = self.invoke(*command, args.image).strip()
            self.verify(container, args.cpus, args.memory_mib, args.network == "host")
            for device in args.device:
                self.invoke("exec", "--user", "10001:10001", container, "sh", "-c",
                            'test -b "$1" && test -r "$1" && test -w "$1"', "sh", device)
            return container
        except BaseException:
            # A failed create can leave a container; only this operation owns it.
            try:
                current = self.inspect(args.name)
                if current.get("Config", {}).get("Labels", {}).get(OWNER) == token:
                    self.invoke("rm", "-f", args.name)
            except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as cleanup_error:
                print(f"Inspect/cleanup after launch failure: {cleanup_error}", flush=True)
            raise
