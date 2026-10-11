# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Release Web is a child so forced exit does not destroy its orphan namespace."""

import json
import time
from allocation import LABEL, create_network, node_data
from fixture import Cluster, HTTP, wait

RUNTIME = "/opt/crowdb/data/test-runtime"


class DisposableWebCluster(Cluster):
    def __init__(self, image, artifacts, **kwargs):
        super().__init__(image, artifacts, node_count=1, wire_replicas=False,
                         advertise=True, **kwargs)

    def start(self):
        subnet = create_network(self)
        data = node_data(self, 0)
        sentinel = data / "test-runtime/persistent/console/N-1/operator-data"
        sentinel.parent.mkdir(parents=True)
        sentinel.write_text("preserve")
        name = self.run_id + "-node0"
        self.containers.append(name)
        self.servers.append(name)
        address = str(subnet.network_address + 2)
        self.addresses.append(address)
        # Only the namespace holder survives; the packaged entrypoint launches
        # the actual Web server, and all requests/assertions stay on the host.
        self.command("run", "-d", "--name", name, "--label", f"{LABEL}={self.run_id}",
            "--network", self.run_id, "--ip", address, "--cpus", str(self.node_cpus),
            "--memory", "512m", "--pids-limit", "256", "--ulimit", "core=-1",
            "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
            "--entrypoint", "/bin/sleep", self.image, "infinity")
        self.command("exec", name, "chown", "-R", "crowdb:crowdb", RUNTIME)
        self.command("exec", "-d", name, "sh", "-c",
            "CROWDB_RUNTIME_ROOT=" + RUNTIME + " /opt/crowdb/bin/entrypoint server web "
            "--bind 0.0.0.0 --port 9090 --test-mode --ui-root /opt/crowdb/ui "
            "--log-dir /opt/crowdb/data/log/web > /opt/crowdb/data/log/web-launch.log 2>&1 & "
            "child=$!; echo $child > /opt/crowdb/data/web.pid; "
            "wait $child; echo $? > /opt/crowdb/data/web.exit")
        wait(lambda: HTTP.open(f"http://{address}:9090/healthz", timeout=5).read(),
             lambda body: body == b"ok", "disposable release Web readiness", seconds=3)
        self.web_pid = int(self.command("exec", name, "cat", "/opt/crowdb/data/web.pid"))
        self.identity(self.web_pid, "crowdb-web")
        return self

    def identity(self, pid, executable):
        if type(pid) is not int or pid <= 1:
            raise ValueError("invalid owned child PID")
        observed = self.command("exec", "--user", "crowdb", self.servers[0], "readlink", f"/proc/{pid}/exe")
        if observed != "/opt/crowdb/bin/" + executable:
            raise ValueError("owned child executable changed")
        stat = self.command("exec", self.servers[0], "cat", f"/proc/{pid}/stat")
        return stat.rsplit(") ", 1)[1].split()[19]

    def manifests(self):
        paths = self.command("exec", self.servers[0], "find", RUNTIME + "/ephemeral",
                             "-type", "f", "(", "-name", "namespace.json", "-o", "-name",
                             "process-owner.json", ")").splitlines()
        return [json.loads(self.command("exec", self.servers[0], "cat", path)) for path in paths]

    def alive(self, pid):
        return self.command("exec", self.servers[0], "sh", "-c",
            f"if test -r /proc/{pid}/stat; then "
            f"awk '{{print $3 == \"Z\" ? \"dead\" : \"alive\"}}' /proc/{pid}/stat; "
            "else echo dead; fi") == "alive"

    def inspect_runtime(self):
        sentinel = self.command("exec", self.servers[0], "cat",
                               RUNTIME + "/persistent/console/N-1/operator-data")
        ephemeral = self.command("exec", self.servers[0], "sh", "-c",
            f"if test -d {RUNTIME}/ephemeral; then find {RUNTIME}/ephemeral "
            "-mindepth 1 -maxdepth 1; fi")
        return {"child_alive": self.alive(self.child_pid), "ephemeral": bool(ephemeral),
                "sentinel": sentinel}

    def control(self, body):
        action = body.get("action")
        if action not in ("record-disposable-child", "terminate-disposable-web",
                          "inspect-disposable-runtime", "cleanup-disposable-runtime"):
            return super().control(body)
        super().control({"action": "status", "node": body.get("node")})
        if action == "record-disposable-child":
            pid = body.get("pid")
            start = self.identity(pid, "crowdb-kv-server")
            owners = self.manifests()
            if not any(record.get("pid") == pid and record.get("start") == start
                       for owner in owners for record in owner.get("processes", [])):
                raise ValueError("server child is missing its exact disposable process identity")
            self.child_pid, self.child_start = pid, start
            return {"recorded": True, **self.inspect_runtime()}
        if action == "terminate-disposable-web":
            signal = body.get("signal")
            if signal not in ("TERM", "KILL"):
                raise ValueError("unsupported disposable Web signal")
            self.identity(self.web_pid, "crowdb-web")
            started = time.monotonic()
            self.command("exec", self.servers[0], "kill", "-" + signal, str(self.web_pid))
            exit_path = self.data_paths[0] / "web.exit"
            wait(lambda: exit_path.read_text() if exit_path.exists() else None,
                 lambda code: code is not None, "release Web exit status", seconds=5)
            return {"exit_code": int(exit_path.read_text()), "seconds": time.monotonic() - started,
                    **self.inspect_runtime()}
        if action == "cleanup-disposable-runtime":
            # Component clean-env tests verify stale/persistent identity and
            # claim pruning. Here the host controller verifies Web's real
            # manifest wiring before acting within its owned PID namespace.
            if self.identity(self.child_pid, "crowdb-kv-server") != self.child_start:
                raise ValueError("disposable process identity changed before cleanup")
            if not any(record.get("pid") == self.child_pid and record.get("start") == self.child_start
                       for owner in self.manifests() for record in owner.get("processes", [])):
                raise ValueError("orphan lost its owned namespace record")
            self.command("exec", self.servers[0], "kill", "-TERM", str(self.child_pid))
            wait(lambda: self.alive(self.child_pid), lambda alive: not alive,
                 "owned orphan cleanup", seconds=5)
            self.command("exec", self.servers[0], "rm", "-rf", RUNTIME + "/ephemeral")
        return self.inspect_runtime()
