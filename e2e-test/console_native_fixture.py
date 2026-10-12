# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three normal manual release nodes with public SSH admission and fixed ports."""

import json
import urllib.request
import urllib.error
from urllib.parse import urlparse
from allocation import LABEL, create_network, node_data, start_node
from fixture import Cluster, HTTP, wait
from console_shell_fixture import ShellBrowserCluster
from console_native_observation import services, process


class NativeConsoleCluster(ShellBrowserCluster):
    def __init__(self, image, artifacts, bundle, *, disk_gib=(80, 8, 8), **kwargs):
        if (len(disk_gib) != 3 or any(type(size) is not int or not 1 <= size <= 1024
                                     for size in disk_gib)):
            raise ValueError("native disk capacities require three bounded GiB values")
        Cluster.__init__(self, image, artifacts, node_count=3, wire_replicas=False,
                         advertise=True, **kwargs)
        self.bundle = bundle
        self.node_ids = []
        self.rejected = False
        self.disk_gib = tuple(disk_gib)

    def api(self, path, body=None, method=None, status=200):
        payload = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://{self.addresses[0]}:9090" + path,
            data=payload, method=method, headers={"Content-Type": "application/json"})
        try:
            with HTTP.open(request, timeout=20) as response:
                content = response.read().decode()
                if response.status != status:
                    raise AssertionError(f"{request.get_method()} {path}: {response.status}: {content}")
                return json.loads(content) if content else None
        except urllib.error.HTTPError as error:
            raise AssertionError(f"{request.get_method()} {path}: {error.code}: {error.read().decode()}") from error

    def start(self):
        subnet = create_network(self)
        host = json.loads(self.command("info", "--format", "{{json .}}"))["ID"]
        if not isinstance(host, str) or not host:
            raise AssertionError("Docker physical host identity is absent")
        for index in range(3):
            data = node_data(self, index)
            disk = data / f"disk-{index + 1}.img"
            with disk.open("wb") as file:
                file.truncate(self.disk_gib[index] * 1024 ** 3)
            name = f"{self.run_id}-prepare{index}"
            self.containers.append(name)
            self.command("run", "--rm", "--name", name, "--label", f"{LABEL}={self.run_id}",
                "--network", "none", "--user", "0", "--entrypoint", "/bin/chown",
                "--cpus", str(self.node_cpus), "--memory", "64m",
                "--mount", f"type=bind,source={data},target=/opt/crowdb/data", self.image,
                "-R", "crowdb:crowdb", "/opt/crowdb/data")
            self.addresses.append(start_node(self, index, data, (), memory="2048m",
                address=str(subnet.network_address + index + 2),
                environment=("CROWDB_STARTUP_MODE=manual", f"CROWDB_PHYSICAL_HOST_ID={host}")))
        self.ready()
        self.api("/api/racks", {"id": 1, "name": "native acceptance"}, status=201)
        candidates = wait(lambda: self.api("/api/node/candidates"),
                          lambda result: len(result["candidates"]) == 3,
                          "three isolated manual node candidates", seconds=10)["candidates"]
        for address in self.addresses:
            candidate = next(entry for entry in candidates if any(
                urlparse(endpoint).hostname == address
                for endpoint in entry["advertisement"]["monitor_endpoints"]))
            record = self.api("/api/node/admit", {
                "discovery_id": candidate["advertisement"]["discovery_id"],
                "rack_id": 1, "ssh_user": "crowdb", "ssh_port": 2222, "ssh_password": "crowdb"})
            self.node_ids.append(record["node_id"])
        self.api("/api/cluster/init", {"nodes": self.node_ids}, status=201)
        wait(lambda: self.api("/api/node/status"), lambda result: result["phase"] == "active",
             "public Group 0 bootstrap", seconds=10)
        wait(lambda: self.api("/api/racks?recursive=1"),
             lambda result: {node["id"] for rack in result["items"] for node in rack["nodes"]
                 if node.get("kv_server", {}).get("health") == "up"} == set(self.node_ids),
             "three live remote KV observations", seconds=10)
        self.origins = [f"http://{address}:10000" for address in self.addresses]
        (self.artifacts / "native-endpoints.json").write_text(json.dumps({
            "nodes": self.node_ids, "addresses": self.addresses, "disk_gib": self.disk_gib, "ports": {
                "web": 9090, "ssh": 2222, "kv": 10000, "diskdb": 7100, "diskio": 7200}}, indent=2))
        return self

    def ready(self):
        for address in self.addresses:
            wait(lambda: HTTP.open(f"http://{address}:9090/healthz", timeout=5).read(),
                 lambda body: body == b"ok", "native release Web listener", seconds=20)

    def control(self, body):
        if body.get("action") == "native-storage-profile":
            return {"disk_gib": self.disk_gib}
        if body.get("action") == "run-native-browser":
            cases = {
                "console::native_browser::multipart_upload_and_physical_navigation":
                    "S3 native multipart upload, HEAD, bounded preview and full round trip",
            }
            name = cases.get(self.case_name)
            if name is None:
                raise ValueError("native browser is not owned by this case")
            self.browser_timeout = 180
            self.browser(name, config="native.config.ts")
            return {"passed": True}
        if body.get("action") == "observe-native-services":
            return services(self, body.get("node"))
        if body.get("action") == "observe-native-process":
            return process(self, body.get("node"), body.get("pid"))
        return super().control(body)
