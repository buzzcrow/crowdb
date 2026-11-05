# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Attach a host CLI to ordinary Docker nodes without a launch receipt."""

import json
import time
from pathlib import Path

from allocation import create_network, node_data, start_node
from console_cli_client import CliClient
import console_cli_operations
from fixture import Cluster, LABEL, request


class CliAttachedCluster(CliClient, Cluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, node_count=3, wire_replicas=False,
                         advertise=True, **kwargs)
        self.bundle = Path(bundle).resolve()
        self.root = (self.data_root / "client-runtime").resolve()
        self.phase_number = 0

    def start(self):
        subnet = create_network(self)
        host = json.loads(self.command("info", "--format", "{{json .}}"))["ID"]
        if not isinstance(host, str) or not host:
            raise AssertionError("Docker physical host identity is absent")
        peers = [str(subnet.network_address + index + 2) for index in range(3)]
        for index, address in enumerate(peers):
            data = node_data(self, index, prepare_logs=False)
            preparer = f"{self.run_id}-prepare{index}"
            self.containers.append(preparer)
            self.command("run", "--rm", "--name", preparer, "--network", "none", "--label", f"{LABEL}={self.run_id}",
                         "--cpu-shares", "1024", "--memory", "64m", "--pids-limit", "32",
                         "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
                         "--entrypoint", "/bin/chown", self.image,
                         "-R", "crowdb:crowdb", "/opt/crowdb/data")
            self.containers.remove(preparer)
            self.addresses.append(start_node(self, index, data, (), address=address,
                memory="2048m", pids_limit=512, environment=(
                    f"CROWDB_CLUSTER={self.run_id}", "CROWDB_INITIAL_VOTERS=3",
                    "CROWDB_DEPLOYMENT_MODE=local",
                    f"CROWDB_PHYSICAL_HOST_ID={host}",
                    f"CROWDB_S3_PUBLIC_URI=http://{address}:7135",
                    f"CROWDB_ICEBERG_PUBLIC_URI=http://{address}:8135",
                    f"CROWDB_DATASET_PUBLIC_URI=http://{address}:9135",
                    "CROWDB_DISCOVERY_SEEDS=" + ",".join(f"http://{peer}:26135" for peer in peers))))
        self.origins = [f"http://{address}:10135" for address in self.addresses]
        for name in self.servers:
            if not json.loads(self.command("inspect", name))[0]["State"]["Running"]:
                raise AssertionError("plain Docker node exited: " + self.command("logs", name))
        self.ready_deadline = time.monotonic() + 180
        report = json.loads(self.invoke("cluster", "check", "--wait", "180", "--json",
                                       endpoint=self.endpoint(0)))
        assert report["phase"] == "ready", report
        self.cluster_id = report["observation"]["cluster_id"]
        binding = self.root / "persistent/clusters" / self.run_id / "target.json"
        binding.parent.mkdir(parents=True, mode=0o700)
        binding.touch(mode=0o600)
        binding.write_text(json.dumps({"version": 1, "name": self.run_id,
            "cluster_id": self.cluster_id, "management_endpoints": [self.endpoint(0)], "runtime": None}))
        return self

    def endpoint(self, index):
        return f"http://{self.addresses[index]}:6135"

    def access_listeners(self, index):
        return {"s3_port": 16000, "health_port": 19000}

    def inventory(self):
        containers = [json.loads(self.command("inspect", name))[0] for name in self.servers]
        assert all(item["Config"]["Labels"].get(LABEL) == self.run_id
                   and item["Image"] == self.image and item["State"]["Running"]
                   for item in containers), "CLI inventory container ownership or image changed"
        processes = {}
        nodes = set()
        for container in containers:
            identity = container["Id"]
            binding = json.loads(self.command("exec", identity, "cat", "/opt/crowdb/data/node-binding.json"))
            node = binding["node_id"]
            assert type(node) is int and node > 0 and node not in nodes, binding
            assert binding["bootstrap"]["cluster_id"] == self.cluster_id, binding
            nodes.add(node)
            status = json.loads(self.command("exec", identity, "cat", "/opt/crowdb/run/status/monitor.json"))
            for service, current in status["services"].items():
                assert type(current.get("pid")) is int and current["pid"] > 1, (node, service, "invalid monitor PID", current)
                processes[node, service] = (current["pid"], current["generation"], current["operation_id"])
            self.command("exec", identity, "sh", "-c", """
                for pid do
                    state=$(awk '{print $3}' "/proc/$pid/stat") || exit 1
                    case "$state" in ''|Z|X)
                        echo "owned service PID $pid is not live ($state)" >&2
                        exit 1;;
                    esac
                done
                """, "owned-service-pids", *(str(current["pid"]) for current in status["services"].values()))
        # Aggregate PID fields are optional fresh samples. Lifecycle invariance
        # uses each owning monitor, including services absent from that cache.
        services = request(self.endpoint(0), "/api/servers")
        assert len({item["id"] for item in services}) == len(services), services
        kinds = {"paxos-kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access-server"}
        for node in nodes:
            registered = [item for item in services if item["node_id"] == node]
            assert len(registered) == len(kinds) and {item["service_type"] for item in registered} == kinds, registered
            required = {"web"} | {"kv" if item["service_type"] == "paxos-kv" else item["id"] for item in registered}
            observed = {service for owner, service in processes if owner == node}
            assert required <= observed, ("owned monitor service is absent", node, required, observed)
        assert {item["node_id"] for item in services} == nodes, services
        for item in services:
            service = "kv" if item["service_type"] == "paxos-kv" else item["id"]
            actual = processes[item["node_id"], service]
            if item.get("pid") is not None:
                assert item.get("pid_node_id") == item["node_id"], ("cached PID namespace changed", item)
                assert item["pid"] == actual[0], ("cached PID differs from owning monitor", item, actual)
        volatile = {"pid", "pid_node_id", "health", "start_pending"}
        registrations = [{key: value for key, value in item.items() if key not in volatile} for item in services]
        return {"containers": [(item["Id"], item["Image"]) for item in containers],
                "processes": sorted((node, service, *identity) for (node, service), identity in processes.items()),
                "registrations": sorted(registrations, key=lambda item: item["id"])}

    def check_ready(self, endpoint=None):
        # Automatic catalog publication can temporarily fence an older serving
        # grant. Use the original startup deadline for the final lifecycle gate;
        # the operation does not receive another readiness budget or a replay.
        remaining = int(self.ready_deadline - time.monotonic())
        assert remaining > 0, "CLI flow exhausted its original readiness deadline"
        report = json.loads(self.invoke("cluster", "check", "--wait", str(remaining), "--json",
                                       endpoint=endpoint))
        assert report["phase"] == "ready" and report["observation"]["cluster_id"] == self.cluster_id, report
        return report

    def replay(self, checkpoint):
        if checkpoint != "attachment":
            before = self.inventory()
            operation = {"observation": console_cli_operations.observation,
                         "hardware": console_cli_operations.hardware,
                         "logical": console_cli_operations.logical,
                         "data": console_cli_operations.data}[checkpoint]
            operation(self)
            self.check_ready()
            assert self.inventory() == before, "logical CLI operation changed runtime lifecycle"
            (self.artifacts / "operation-proof.json").write_text(json.dumps({
                "cluster_id": self.cluster_id, "operation": checkpoint, "inventory": before}, indent=2))
            print("PASS selected-cluster CLI", checkpoint, flush=True)
            return
        self.attachment()

    def attachment(self):
        before = self.inventory()
        for domain in ("rack", "node", "disk-group", "disk"):
            assert self.invoke("cluster", domain, "list", endpoint=self.endpoint(1)).strip()
        assert self.invoke("paxos-kv", "store", "list").strip()
        assert self.invoke("paxos-kv", "group", "list", "--store", "0").strip()
        key, value = "cli-attach/" + self.run_id, "verified-selected-cluster"
        self.invoke("paxos-kv", "data", "put",
                    "--store", "0", "--group", "0", "--key", key, "--value", value)
        for index in range(3):
            found = self.invoke("paxos-kv", "data", "get", "--store", "0", "--group", "0", "--key", key,
                                endpoint=self.endpoint(index))
            assert value in found, found
        self.invoke("paxos-kv", "data", "delete", "--store", "0", "--group", "0", "--key", key)
        assert "not found" in self.invoke("paxos-kv", "data", "get", "--store", "0", "--group", "0", "--key", key)
        self.invoke("s3", "bucket", "put", "cli-attach", endpoint=self.endpoint(2))
        self.invoke("s3", "object", "put", "cli-attach", "witness", "--text", value)
        for index in range(3):
            assert self.invoke("s3", "object", "get", "cli-attach", "witness",
                               endpoint=self.endpoint(index)) == value
        self.invoke("cluster", "stop", endpoint=self.endpoint(0), expected_exit=2)
        self.check_ready(endpoint=self.endpoint(2))
        assert self.inventory() == before, "attachment changed container or service lifecycle"
        assert not (self.root / "persistent/oci-clusters").exists(), "attachment created a runtime receipt"
        (self.artifacts / "attachment-proof.json").write_text(json.dumps({
            "cluster_id": self.cluster_id, "inventory": before,
            "kv_reads": 3, "s3_reads": 3, "runtime_receipt": False}, indent=2))
        print("PASS named and endpoint-only attachment to plain Docker nodes", flush=True)
