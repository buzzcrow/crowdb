# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host CLI recovery against three owned formal release containers."""

import json
import os
import signal
import subprocess
import time

from fixture import Cluster, redact

LABEL = "org.crowdb.cluster.id"
ROOT_LABEL = "org.crowdb.cluster.root"


class CliContainerCluster(Cluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, node_count=3, wire_replicas=False, **kwargs)
        self.bundle = bundle
        self.root = (self.data_root / "cluster").resolve()
        self.phase_number = 0

    def start(self):
        self.root.mkdir(parents=True)
        return self

    def arguments(self, *arguments):
        return [str(self.bundle / "crowdb-cli"), "s3", *arguments]

    def environment(self):
        return dict(os.environ, NO_PROXY="*", no_proxy="*")

    def invoke(self, *arguments):
        self.phase_number += 1
        started = time.monotonic()
        result = subprocess.run(self.arguments(*arguments), env=self.environment(),
                                capture_output=True, text=True, timeout=180)
        output = result.stdout + result.stderr
        (self.artifacts / f"client-cli-{self.phase_number}.log").write_text(redact(output))
        (self.artifacts / f"client-cli-{self.phase_number}.json").write_text(json.dumps({
            "case": self.case_name, "phase": ".".join(arguments[:2]), "execution": "host",
            "seconds": time.monotonic() - started, "exit_code": result.returncode}, indent=2))
        if result.returncode:
            raise AssertionError(output)
        return result.stdout

    def state(self):
        return json.loads((self.root / "container-cluster.json").read_text())

    def interrupt(self, checkpoint):
        relative = os.path.relpath(self.root, os.getcwd())
        arguments = self.arguments("cluster", "start", "--root", relative,
            "--container", self.run_id, "--image", self.image,
            "--node-cpus", str(self.node_cpus), "--memory-mib", "2048")
        log = self.artifacts / "client-interrupted-start.log"
        interrupted_at = time.monotonic()
        with log.open("w") as output:
            process = subprocess.Popen(arguments, env=self.environment(), stdout=output, stderr=output)
            try:
                deadline = time.monotonic() + 90
                while True:
                    if process.poll() is not None:
                        raise AssertionError(f"CLI ended before {checkpoint}: {log.read_text()}")
                    path = self.root / "container-cluster.json"
                    state = json.loads(path.read_text()) if path.exists() else {}
                    if state.get("phase") == checkpoint:
                        process.send_signal(signal.SIGSTOP)
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError(f"CLI never reached {checkpoint}: {state}")
                    time.sleep(0.01)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=10)
        control = self.artifacts / "control"
        control.mkdir(exist_ok=True)
        (control / "cli-interruption.response.json").write_text(json.dumps({
            "ok": True, "result": {"checkpoint": checkpoint, "signal": "SIGKILL",
            "exit_code": process.returncode}, "seconds": time.monotonic() - interrupted_at}, indent=2))

    def replay(self, checkpoint):
        self.interrupt(checkpoint)
        assert (self.root / "s3-mini-cluster.initializing.json").is_file()
        assert (self.root / "s3-local-state.toml").is_file()
        assert not (self.root / "console.toml").exists()
        before = self.state()
        self.refresh()
        binding = self.read_owned("node-binding.json")
        diskdb = self.read_owned("services/diskdb-1/execution.json") if checkpoint == "storage" else None
        if diskdb is not None:
            assert diskdb["complete"] and diskdb["pid"] > 1
            self.command("exec", self.servers[0], "/bin/kill", "-0", str(diskdb["pid"]))
        ids = [json.loads(self.command("inspect", name))[0]["Id"] for name in self.servers]
        self.invoke("cluster", "start", "--container", self.servers[1])
        after = self.state()
        assert before["id"] == after["id"] and before["addresses"] == after["addresses"]
        assert binding == self.read_owned("node-binding.json")
        if diskdb is not None:
            assert diskdb == self.read_owned("services/diskdb-1/execution.json"), "confirmed Diskdb execution must survive resume"
            self.command("exec", self.servers[0], "/bin/kill", "-0", str(diskdb["pid"]))
        assert ids == [json.loads(self.command("inspect", name))[0]["Id"] for name in self.servers]
        assert not (self.root / "s3-mini-cluster.initializing.json").exists()
        assert not (self.root / "bootstrap-intent.toml").exists()
        assert "[[rack]]" not in (self.root / "s3-local-state.toml").read_text()
        assert "Containers: 3/3" in self.invoke("cluster", "status", "--container", self.servers[2])
        if checkpoint == "bound":
            self.lifecycle()
        self.diagnostics()
        deleting = time.monotonic()
        self.invoke("cluster", "delete", "--container", self.servers[2])
        assert not self.root.exists()
        self.closed = True
        self.timings["stop"] = time.monotonic() - deleting
        self.timings["total"] = time.monotonic() - self.started
        self.write_timings()

    def lifecycle(self):
        self.invoke("bucket", "put", "--container", self.run_id, "cli-replay")
        self.invoke("object", "put", "--container", self.run_id, "cli-replay", "witness", "--text", "durable-container-witness")
        self.invoke("cluster", "stop", "--container", self.servers[1])
        assert "Containers: 0/3" in self.invoke("cluster", "status", "--root", str(self.root))
        self.invoke("cluster", "restart", "--container", self.run_id)
        assert self.invoke("object", "get", "--container", self.servers[2], "cli-replay", "witness") == "durable-container-witness"


    def refresh(self):
        state = self.state()
        self.servers = [state["name"], state["name"] + "-node2", state["name"] + "-node3"]
        self.containers = self.servers.copy()
        self.addresses = state["addresses"]
        self.origins = [f"http://{address}:10000" for address in self.addresses]
        self.data_paths = [self.root / "nodes" / f"node{index}" / "data" for index in range(3)]
        (self.artifacts / "mounts.json").write_text(json.dumps({
            "container_data": "/opt/crowdb/data", "host_data": [str(path) for path in self.data_paths]}, indent=2))
        (self.artifacts / "endpoints.json").write_text(json.dumps({
            "addresses": self.addresses, "web_port": 9090, "kv_port": 10000,
            "diskdb_ports": [7100, 7101, 7102], "diskio_port": 7200,
            "chunkdb_ports": [7300, 7301], "chunk_kv_ports": [7400, 7401],
            "access_ports": [9000, 9001, 9002]}, indent=2))

    def read_owned(self, path, index=0):
        if path not in ("node-binding.json", "services/diskdb-1/execution.json"):
            raise ValueError("unsupported CLI recovery observation")
        if type(index) is not int or index < 0 or index >= len(self.servers):
            raise ValueError("CLI recovery node is outside the owned cluster")
        info = json.loads(self.command("inspect", self.servers[index]))[0]
        labels = info["Config"]["Labels"]
        if labels.get(LABEL) != self.state()["id"] or labels.get(ROOT_LABEL) != str(self.root) or info["Image"] != self.image:
            raise ValueError("CLI recovery observation ownership changed")
        return json.loads(self.command("exec", info["Id"], "cat", "/opt/crowdb/data/" + path))

    def diagnostics(self):
        if (self.root / "container-cluster.json").exists():
            self.refresh()
            for index, name in enumerate(self.servers):
                destination = self.artifacts / "runtime-logs" / f"node{index}"
                destination.mkdir(parents=True, exist_ok=True)
                try:
                    self.read_owned("node-binding.json", index)
                    self.command("cp", f"{name}:/opt/crowdb/data/log", str(destination))
                except (RuntimeError, OSError) as error:
                    (destination / "collection.error").write_text(redact(str(error)))
        super().diagnostics()

    def close(self):
        if self.closed:
            return
        started = time.monotonic()
        if (self.root / "container-cluster.json").exists():
            state = self.state()
            for kind in ("container", "network"):
                listing = ("-a",) if kind == "container" else ()
                names = self.command(kind, "ls", *listing, "--filter", f"label={LABEL}={state['id']}",
                    "--filter", f"label={ROOT_LABEL}={self.root}", "--format", "{{.Names}}" if kind == "container" else "{{.Name}}")
                for name in names.splitlines():
                    info = json.loads(self.command("inspect", name) if kind == "container"
                                      else self.command("network", "inspect", name))[0]
                    labels = info["Config"]["Labels"] if kind == "container" else info["Labels"]
                    if labels[LABEL] != state["id"] or labels[ROOT_LABEL] != str(self.root):
                        raise AssertionError("CLI cleanup ownership changed")
                    self.command(kind, "rm", *(("-f", name) if kind == "container" else (name,)))
        self.closed = True
        self.timings["stop"] = time.monotonic() - started
        self.timings["total"] = time.monotonic() - self.started
        self.write_timings()
