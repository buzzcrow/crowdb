# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""External engines share the normal release profile and retained node data."""

import json
import ipaddress
from managed_fixture import ManagedCluster


class EcosystemCluster(ManagedCluster):
    def __init__(self, image, artifacts, **kwargs):
        super().__init__(image, artifacts, services=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), **kwargs)

    def ready(self):
        super().ready()
        node = json.loads(self.command("inspect", self.servers[0]))[0]
        published = {port: bindings for port, bindings in node["NetworkSettings"]["Ports"].items() if bindings}
        if published:
            raise AssertionError(f"ecosystem bridge fixture published host ports: {published}")

    def restart(self):
        name = self.servers[0]
        self.command("stop", "--time", "15", name)
        self.command("rm", name)
        self.servers.clear()
        self.containers.remove(name)
        address = self.launch(self.data_paths[0], self.artifacts / "profile.toml")
        if address != self.addresses[0]:
            raise AssertionError("ecosystem node lost its fixed address on recreation")
        self.ready()

    def engine_environment(self):
        network = json.loads(self.command("network", "inspect", self.run_id))[0]
        subnet = ipaddress.ip_network(network["IPAM"]["Config"][0]["Subnet"])
        name = self.run_id + "-trino"
        self.containers.append(name)
        return {"CROWDB_ECOSYSTEM_NETWORK":self.run_id,
                "CROWDB_ECOSYSTEM_TRINO_IP":str(subnet.network_address + 3)}, name
