# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Real file-backed packaged Diskio with a fixed endpoint and retained disk."""

import ipaddress
import json
import socket
import time
from allocation import create_network, node_data, start_node
from fixture import Cluster, wait


class DiskioCluster(Cluster):
    def __init__(self, image, artifacts, direct=False, **kwargs):
        self.direct = direct
        super().__init__(image, artifacts, node_count=1, advertise=True, wire_replicas=False, **kwargs)

    def start(self):
        subnet = create_network(self)
        data = node_data(self, 0)
        disk = data / "disk.dat"
        with disk.open("wb") as output:
            output.truncate(16 * 1024 * 1024)
        # This owned fixture file is a regular writable data device for the
        # released service UID; it is never shared with another run.
        disk.chmod(0o666)
        address = str(subnet.network_address + 2)
        self.addresses.append(start_node(self, 0, data, (
            "server", "diskio", "--port", "7200", "--bind", "0.0.0.0",
            "--disk", "1:/opt/crowdb/data/disk.dat:16777216",
            *(() if self.direct else ("--no-o-direct",))), address))
        self.ready()
        (self.artifacts / "endpoints.json").write_text(json.dumps({"nodes": [{
            "ip": self.addresses[0], "rpc_port": 7200, "data": str(data),
            "disk": str(disk), "disk_id": "0:1", "capacity_bytes": 16777216}]}, indent=2))
        return self

    def ready(self):
        def reachable():
            state = json.loads(self.command("inspect", self.servers[0]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Diskio exited: " + self.command("logs", self.servers[0]))
            with socket.create_connection((self.addresses[0], 7200), timeout=1):
                return True
        wait(reachable, bool, "host-to-packaged Diskio RPC listener")

    def crash_restart(self):
        started = time.monotonic()
        self.command("kill", "--signal", "KILL", self.servers[0])
        self.command("start", self.servers[0])
        info = json.loads(self.command("inspect", self.servers[0]))[0]
        if info["NetworkSettings"]["Networks"][self.run_id]["IPAddress"] != self.addresses[0]:
            raise AssertionError("Diskio IP changed during persisted restart")
        self.ready()
        self.timings["restart"] = time.monotonic() - started


class DiskioDiscoveryCluster(DiskioCluster):
    def __init__(self, image, artifacts, bundle, backend, fault_rate=0, latency=None, **kwargs):
        super().__init__(image, artifacts, groups=(0, 1), **kwargs)
        self.bundle, self.backend = bundle, backend
        self.diskio_started = False
        self.fault_rate = fault_rate
        self.latency = latency

    def start(self):
        Cluster.start(self)
        self.client(self.bundle, "seed", "diskio::hardware::seed")
        data = node_data(self, 1)
        address = str(ipaddress.ip_address(self.addresses[0]) + 1)
        self.addresses.append(self.launch_diskio(1, data, address, latency=self.latency))
        self.diskio_started = True
        self.ready()
        (self.artifacts / "diskio-endpoints.json").write_text(json.dumps({
            "ip": address, "rpc_port": 7200, "instance_id": 999,
            "disk_group_id": 100, "backend": self.backend, "data": str(data)}, indent=2))
        return self

    def launch_diskio(self, index, data, address, fault_rate=None, latency=None, running=True):
        fault_rate = self.fault_rate if fault_rate is None else fault_rate
        return start_node(self, index, data, (
            "server", "diskio", "--port", "7200", "--bind", address,
            "--dummy-disk", self.backend, "--kv-seeds", self.origins[0],
            "--instance-id", "999", "--rack-id", "1", "--node-id", "10",
            "--dg-id", "100", "--sync-interval-ms", "200", "--auto-discover-disks",
            "--log-dir", "/opt/crowdb/data/log/diskio",
            "--fault-error-rate", str(fault_rate),
            *(("--fault-latency", latency) if latency else ())), address, running=running)
    def ready(self):
        if not self.diskio_started:
            return Cluster.ready(self)
        def reachable():
            state = json.loads(self.command("inspect", self.servers[-1]))[0]["State"]
            if not state["Running"]:
                raise AssertionError("Diskio exited: " + self.command("logs", self.servers[-1]))
            with socket.create_connection((self.addresses[-1], 7200), timeout=1):
                return True
        wait(reachable, bool, "host-to-discovered Diskio RPC listener")


class DiskioSemanticCluster(DiskioDiscoveryCluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, bundle, "mem", latency="200:200", **kwargs)

    def start(self):
        super().start()
        base = ipaddress.ip_address(self.addresses[0])
        for index, fault in ((2, 0), (3, 1)):
            data = node_data(self, index)
            address = str(base + index)
            self.addresses.append(self.launch_diskio(index, data, address, fault_rate=fault, running=False))
        return self

    def control(self, body):
        result = super().control(body)
        if body["action"] == "start" and body["node"] > 0:
            index = body["node"]
            def reachable():
                state = json.loads(self.command("inspect", self.servers[index]))[0]["State"]
                if not state["Running"]:
                    raise AssertionError("Diskio exited: " + self.command("logs", self.servers[index]))
                with socket.create_connection((self.addresses[index], 7200), timeout=1):
                    return True
            wait(reachable, bool, "replacement Diskio RPC listener")
        return result
