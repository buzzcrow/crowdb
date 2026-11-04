# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Bounded DiskIO packet loss inside verified CLI-owned network namespaces."""

from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess


class DiskIoOutage:
    def __init__(self, cluster):
        self.cluster = cluster
        self.chain = "CROWDB_E2E_" + hashlib.sha256(cluster.run_id.encode()).hexdigest()[:12]
        self.nodes = []
        for index, name in enumerate(cluster.servers):
            policy = cluster.read_owned("node-policy.json", index)
            services = [service for service in policy["setup"]["services"]
                        if service["kind"] == "diskio" and service["enabled"]]
            if len(services) != 1:
                raise ValueError("fault needs one accepted DiskIO listener per owned node")
            port = services[0]["listeners"]["rpc_port"]
            if type(port) is not int or not 0 < port < 65536:
                raise ValueError("invalid owned DiskIO listener")
            self.nodes.append((name, port))
        if len(self.nodes) != 3:
            raise ValueError("terminal writer fault requires three owned nodes")

    def _prepare_tools(self):
        binary = shutil.which("iptables-nft")
        if not binary:
            raise RuntimeError("owned network fault requires the host iptables-nft executable")
        self.tools = self.cluster.artifacts / "fault-network-tools"
        self.tools.mkdir()
        shutil.copy2(binary, self.tools / "iptables-nft")
        result = subprocess.run(["ldd", binary], capture_output=True, text=True, check=False, timeout=10)
        if result.returncode:
            raise RuntimeError(f"netfilter dependency inspection failed: {result.stdout}{result.stderr}")
        libraries = [Path(path) for path in re.findall(r"(?:=>\s*|^\s*)(/\S+)", result.stdout, re.M)]
        loaders = [path for path in libraries if path.name.startswith("ld-linux")]
        extensions = [path.parent / "xtables" for path in libraries if path.name.startswith("libxtables")]
        if len(loaders) != 1 or len(extensions) != 1 or not extensions[0].is_dir():
            raise RuntimeError(f"cannot locate the host netfilter loader/extensions: {result.stdout}")
        self.loader = loaders[0].name
        for library in libraries:
            shutil.copy2(library, self.tools / library.name)
        shutil.copytree(extensions[0], self.tools / "xtables")
        files = {str(path.relative_to(self.tools)): hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in self.tools.rglob("*") if path.is_file()}
        (self.cluster.artifacts / "fault-network-tools.json").write_text(json.dumps(files, indent=2))

    def _command(self, node, *arguments):
        name, _port = node
        info = self.cluster.inspect_owned(name)
        if info["HostConfig"]["NetworkMode"] != "crowdb-" + self.cluster.run_id:
            raise ValueError("fault must remain inside the owned CLI bridge namespace")
        root = "/run/crowdb-netfilter"
        return self.cluster.command("run", "--rm", "--network", "container:" + info["Id"],
            "--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--memory", "64m", "--pids-limit", "32",
            "--cpu-shares", "1024", "--label", "org.crowdb.e2e.run=" + self.cluster.run_id,
            "--mount", f"type=bind,source={self.tools.resolve()},target={root},readonly",
            "--env", f"XTABLES_LIBDIR={root}/xtables", "--entrypoint", f"{root}/{self.loader}",
            self.cluster.image, "--library-path", root, f"{root}/iptables-nft", "-w", "2", *arguments)

    def __enter__(self):
        self._prepare_tools()
        with ExitStack() as cleanup:
            for node in self.nodes:
                self._command(node, "-N", self.chain)
                cleanup.callback(self._command, node, "-X", self.chain)
                self._command(node, "-A", self.chain, "-p", "tcp", "--dport", str(node[1]), "-j", "DROP")
                cleanup.callback(self._command, node, "-F", self.chain)
                self._command(node, "-I", "INPUT", "1", "-j", self.chain)
                cleanup.callback(self._command, node, "-D", "INPUT", "-j", self.chain)
            self.cleanup = cleanup.pop_all()
        return self

    def verify(self):
        proof = []
        for node in self.nodes:
            self._command(node, "-C", "INPUT", "-j", self.chain)
            self._command(node, "-C", self.chain, "-p", "tcp", "--dport", str(node[1]), "-j", "DROP")
            proof.append({"container": self.cluster.inspect_owned(node[0])["Id"],
                          "diskio_port": node[1], "chain": self.chain, "blocked": True})
        (self.cluster.artifacts / "fault-network-blocked.json").write_text(json.dumps(proof, indent=2))

    def __exit__(self, *exception):
        return self.cleanup.__exit__(*exception)
