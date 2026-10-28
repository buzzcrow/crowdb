# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Common CLI interruption/recovery against ordinary OCI node startup."""

import json
import os
from pathlib import Path
import signal
import subprocess
import time

from fixture import Cluster, redact, request
from console_cli_recovery import terminal_writer_recovery
from console_cli_client import CliClient
from default_ports import (WEB_BASE, ACCESS_SERVER_HTTP_BASE,
    ACCESS_SERVER_ICEBERG_HTTP_BASE, ACCESS_SERVER_DATASET_HTTP_BASE)

LABEL = "org.crowdb.cluster"
OWNER_LABEL = "org.crowdb.cluster.owner"
ROOT = Path(__file__).resolve().parents[1]


class CliContainerCluster(CliClient, Cluster):
    def __init__(self, image, artifacts, bundle, checkpoint=None, **kwargs):
        super().__init__(image, artifacts, node_count=3, wire_replicas=False, **kwargs)
        self.bundle = Path(bundle).resolve()
        self.root = (self.data_root / "runtime").resolve()
        self.phase_number = 0
        self.checkpoint = checkpoint
        self.port_base = None

    def state(self):
        return json.loads((self.root / "persistent/oci-clusters" / f"{self.run_id}.json").read_text())

    def start_arguments(self):
        return ("cluster", "start", "--nodes", "3", "--image", self.image,
                "--cpus", str(self.node_cpus), "--memory-mib", "2048",
                "--port-base", str(self.port_base), "--wait", "180", "--json")

    def start(self):
        self.root.mkdir(parents=True, mode=0o700)
        # Claims belong to this live runner in the shared allocator. Receipt roots
        # remain per case; separate roots must not allocate overlapping host ports.
        services = (("web", WEB_BASE), ("access-http", ACCESS_SERVER_HTTP_BASE),
                    ("access-iceberg-http", ACCESS_SERVER_ICEBERG_HTTP_BASE),
                    ("access-dataset-http", ACCESS_SERVER_DATASET_HTTP_BASE))
        offset = 0
        for service, base in services:
            result = subprocess.run([str(self.bundle / "crowdb-cli"), "port-alloc",
                "--service", service, "--instance", str(offset), "--count", "3",
                "--owner-pid", str(os.getpid())],
                capture_output=True, text=True, timeout=10, check=True)
            ports = [int(port) for port in result.stdout.split()]
            if service == "web" and ports:
                offset = ports[0] - WEB_BASE
                self.port_base = ports[0]
            expected = list(range(base + offset, base + offset + 3))
            if ports != expected:
                raise AssertionError(f"CLI fixture needs aligned {service} ports {expected}, got {ports}")
        self.interrupt("storage" if self.checkpoint == "terminal-writer" else self.checkpoint)
        self.refresh()
        return self

    def interrupt(self, checkpoint):
        if checkpoint not in ("bound", "storage"):
            raise ValueError("unsupported common CLI recovery checkpoint")
        log = self.artifacts / "client-interrupted-start.log"
        interrupted_at = time.monotonic()
        with log.open("w") as output:
            process = subprocess.Popen(self.arguments(*self.start_arguments()), env=self.environment(),
                                       stdout=output, stderr=output)
            try:
                deadline = time.monotonic() + 180
                while True:
                    if process.poll() is not None:
                        raise AssertionError(f"CLI ended before {checkpoint}: {log.read_text()}")
                    if self.checkpoint_reached(checkpoint):
                        process.send_signal(signal.SIGSTOP)
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError(f"CLI never reached {checkpoint}: {log.read_text()}")
                    time.sleep(0.02)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=10)
        control = self.artifacts / "control"
        control.mkdir(exist_ok=True)
        (control / "cli-interruption.response.json").write_text(json.dumps({
            "ok": True, "result": {"checkpoint": checkpoint, "signal": "SIGKILL",
            "exit_code": process.returncode}, "seconds": time.monotonic() - interrupted_at}, indent=2))

    def checkpoint_reached(self, checkpoint):
        receipt = self.root / "persistent/oci-clusters" / f"{self.run_id}.json"
        if not receipt.exists():
            return False
        self.refresh()
        if len(self.servers) != 3:
            return False
        path = "node-binding.json" if checkpoint == "bound" else "diskdb-execution"
        try:
            value = self.read_owned(path)
        except RuntimeError:
            return False  # The monitor has not published this checkpoint yet.
        return bool(value.get("bootstrap")) if checkpoint == "bound" else bool(value.get("complete"))

    def replay(self, checkpoint):
        if checkpoint != self.checkpoint:
            raise ValueError("CLI recovery checkpoint was not prepared before client dispatch")
        before = self.state()
        binding = self.read_owned("node-binding.json")
        container_ids = [self.inspect_owned(name)["Id"] for name in self.servers]
        execution = self.read_owned("diskdb-execution") if checkpoint == "storage" else None
        report = json.loads(self.invoke("cluster", "start", "--wait", "180", "--json"))
        assert report["phase"] == "ready", report
        assert before == self.state(), "resume must retain accepted launch inputs"
        assert binding == self.read_owned("node-binding.json"), "resume must retain accepted binding"
        assert container_ids == [self.inspect_owned(name)["Id"] for name in self.servers]
        if execution is not None:
            assert execution == self.read_owned("diskdb-execution"), "confirmed storage must survive CLI interruption"
        if checkpoint == "terminal-writer":
            terminal_writer_recovery(self, report["observation"]["cluster_id"])
            return
        self.lifecycle(report["observation"]["cluster_id"])
        self.current_readiness(report["observation"]["cluster_id"])

    def lifecycle(self, cluster_id):
        files = self.file_disks()
        self.invoke("s3", "bucket", "put", "cli-replay")
        self.invoke("s3", "object", "put", "cli-replay", "witness", "--text", "durable-container-witness")
        self.invoke("cluster", "stop")
        assert all(not self.inspect_owned(name)["State"]["Running"] for name in self.servers)
        report = json.loads(self.invoke("cluster", "start", "--wait", "180", "--json"))
        assert report["phase"] == "ready" and report["observation"]["cluster_id"] == cluster_id
        assert self.file_disks() == files, "restart must retain the accepted file disk identities"
        assert self.invoke("s3", "object", "get", "cli-replay", "witness") == "durable-container-witness"
        # Alternate-member endpoint attachment uses no local runtime selector.
        report = json.loads(self.invoke("cluster", "check", "--json",
            endpoint=f"http://{self.addresses[2]}:6135"))
        assert report["phase"] == "ready" and report["observation"]["cluster_id"] == cluster_id

    def current_readiness(self, cluster_id):
        origin = f"http://{self.addresses[0]}:6135"
        node_id = self.read_owned("node-binding.json")["node_id"]
        services = request(origin, "/api/servers")
        service = next(item for item in services if item["node_id"] == node_id
                       and item.get("service_type") == "diskio")
        service_id = service["id"]
        dependency = f"node/{node_id}/service/{service_id}"
        # Restart belongs to the fixture's explicit mutation, never to check.
        try:
            request(origin, f"/api/services/{service_id}/stop", {})
            started = time.monotonic()
            stopped = json.loads(self.invoke("cluster", "check", "--wait", "2", "--json",
                                            endpoint=origin, expected_exit=1))
            assert time.monotonic() - started < 15, "endpoint-only check exceeded its bounded budget"
            if stopped["phase"] == "unavailable":
                # The two-second endpoint budget can also expire while collecting
                # authority. Its higher-priority reason must remain explicit.
                assert any(reason["dependency"] == "cluster/authority"
                           and reason["code"] == "management-origin-unavailable"
                           for reason in stopped["reasons"]), stopped["reasons"]
            else:
                assert stopped["phase"] == "degraded", stopped["reasons"]
            assert stopped["observation"]["cluster_id"] == cluster_id
            fact = next(item for item in stopped["observation"]["dependencies"] if item["id"] == dependency)
            assert fact["state"] == "stopped" and fact["current_operation_id"], fact
            assert fact["reason"]["code"] == "required-service-not-ready", fact
            assert "current committed action is stop or delete" in fact["detail"], fact
            again = request(origin, "/api/cluster/status")
            current = next(item for item in again["observation"]["dependencies"] if item["id"] == dependency)
            assert current["state"] == "stopped", current
            assert current["current_operation_id"] == fact["current_operation_id"], "check changed service intent"
        finally:
            request(origin, f"/api/services/{service_id}/restart", {})
        recovered = json.loads(self.invoke("cluster", "check", "--wait", "180", "--json", endpoint=origin))
        assert recovered["phase"] == "ready" and recovered["observation"]["cluster_id"] == cluster_id

    def inspect_owned(self, name):
        info = json.loads(self.command("inspect", name))[0]
        record = self.state()
        labels = info["Config"]["Labels"]
        if (labels.get(LABEL) != self.run_id or labels.get(OWNER_LABEL) != record["owner"]
                or info["Image"] != record["image_id"]):
            raise ValueError("CLI recovery observation ownership or image changed")
        return info

    def file_disks(self):
        observed = []
        for index, address in enumerate(self.addresses):
            node_id = self.read_owned("node-binding.json", index)["node_id"]
            page = request(f"http://{address}:6135", f"/api/nodes/{node_id}/disk-files")
            assert page["next"] is None and len(page["items"]) == 1, page
            disk = page["items"][0]
            assert disk["phase"] == "registered" and disk["preparation_complete"] and disk["backing_identity"], disk
            observed.append({key: disk[key] for key in ("request", "path", "backing_identity")})
        return observed

    def refresh(self):
        self.servers, self.addresses = [], []
        network = "crowdb-" + self.run_id
        for index in range(1, 4):
            name = f"{network}-{index}"
            try:
                info = self.inspect_owned(name)
            except RuntimeError:
                continue
            self.servers.append(name)
            self.addresses.append(info["NetworkSettings"]["Networks"][network]["IPAddress"])
        self.containers = self.servers.copy()
        self.origins = [f"http://{address}:10135" for address in self.addresses]
        (self.artifacts / "endpoints.json").write_text(json.dumps({
            "addresses": self.addresses, "web_port": 6135,
            "published_console_ports": [self.port_base + index for index in range(3)]}, indent=2))

    def read_owned(self, path, index=0):
        if path not in ("node-binding.json", "diskdb-execution"):
            raise ValueError("unsupported CLI recovery observation")
        if type(index) is not int or not 0 <= index < len(getattr(self, "servers", ())):
            raise ValueError("CLI recovery node is outside the owned cluster")
        info = self.inspect_owned(self.servers[index])
        if path == "diskdb-execution":
            binding = self.read_owned("node-binding.json", index)
            node_id = binding["node_id"]
            if type(node_id) is not int or node_id < 1:
                raise ValueError("invalid owned node binding")
            path = f"services/diskdb-{node_id}/execution.json"
        return json.loads(self.command("exec", info["Id"], "cat", "/opt/crowdb/data/" + path))

    def diagnostics(self):
        if not (self.root / "persistent/oci-clusters" / f"{self.run_id}.json").exists():
            return
        self.refresh()
        for index, address in enumerate(self.addresses):
            for path in ("/api/cluster/status", "/api/disk-groups", "/api/disks", "/api/servers"):
                name = path.removeprefix("/api/").replace("/", "-")
                try:
                    value = request(f"http://{address}:6135", path)
                    (self.artifacts / f"{name}-{index}.json").write_text(redact(value))
                except (OSError, ValueError) as error:
                    (self.artifacts / f"{name}-{index}.error").write_text(redact(str(error)))
        for index, name in enumerate(self.servers):
            destination = self.artifacts / "runtime-logs" / f"node{index}"
            destination.mkdir(parents=True, exist_ok=True)
            self.inspect_owned(name)
            self.command("cp", f"{name}:/opt/crowdb/data/log", str(destination))
        super().diagnostics()

    def close(self):
        if self.closed:
            return
        started = time.monotonic()
        # Cleanup is an independent host operation, never a product CLI command.
        deletion = ["--delete-data"] if self.timings["status"] == "pass" else []
        result = subprocess.run(["python", str(ROOT / "container/oci-image/docker-local.py"),
            "clean", "--cluster", self.run_id, *deletion], env=self.environment(),
            capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise RuntimeError(f"CLI fixture owned cleanup failed: {result.stdout}\n{result.stderr}")
        self.closed = True
        self.timings["stop"] = time.monotonic() - started
        if self.started is not None:
            self.timings["total"] = time.monotonic() - self.started
        self.write_timings()
