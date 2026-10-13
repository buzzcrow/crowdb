# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Owned Docker bridge clusters running the packaged KV layer only."""

import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from allocation import LABEL, create_network, node_data, start_node
from client import run_client
from kv_node import arguments as kv_arguments
from control import Control
from kv_topology import ready_nodes, wire_nodes

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def docker(*args, timeout=60):
    result = subprocess.run(["docker", *args], capture_output=True, text=True,
                            check=False, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"docker {args}: {result.stdout}\n{result.stderr}")
    return (result.stdout + result.stderr if args and args[0] == "logs" else result.stdout).strip()


def request(origin, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    try:
        with HTTP.open(urllib.request.Request(origin + path, data=data,
                       headers={"Content-Type": "application/json"}), timeout=5) as response:
            content = response.read()
            return json.loads(content) if content else None
    except urllib.error.HTTPError as error:
        error.add_note(error.read().decode())
        raise


def wait(action, predicate, description, seconds=30):
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        try:
            last = action()
            if predicate(last):
                return last
        except (OSError, ValueError) as error:
            last = str(error)
        time.sleep(0.1)
    raise AssertionError(f"{description}: {last}")


def redact(value):
    text = json.dumps(value, indent=2) if not isinstance(value, str) else value
    return re.sub(r'(?i)(password|secret|token|access_key)(["\s:=]+)[^\s,\"}]+',
                  r'\1\2<redacted>', text)


def persisted_layout(topology):
    return {store["store_id"]: (store["listen_addr"], {
        group["group_id"]: group["local_replica_id"] for group in store.get("groups", [])})
        for store in topology.get("stores", [])}


class Cluster:
    """The runner owns lifecycle; host clients use inspected node IPs."""

    def __init__(self, image, artifacts, command=docker, data_root=None, node_count=3,
                 store_ids=None, wire_replicas=True, stores_per_node=None, groups=(1,), groups_per_node=None,
                 node_args=None, advertise=False, preserve_network=False):
        if node_count < 1:
            raise ValueError("a cluster needs at least one node")
        self.run_id = "crowdb-e2e-" + uuid.uuid4().hex
        self.image, self.command = image, command
        self.artifacts = Path(artifacts) / self.run_id
        self.artifacts.mkdir(parents=True)
        self.containers, self.origins, self.addresses = [], [], []
        self.servers = []
        self.node_count = node_count
        self.node_args = node_args or ((),) * node_count
        if len(self.node_args) != node_count:
            raise ValueError("startup arguments must match the node count")
        self.advertise = advertise
        self.preserve_network = preserve_network
        store_ids = tuple(range(node_count)) if store_ids is None else tuple(store_ids)
        self.stores_per_node = tuple(tuple(stores) for stores in stores_per_node) if stores_per_node else tuple(
            (store,) for store in store_ids)
        self.groups = tuple(groups)
        self.groups_per_node = tuple(tuple(groups) for groups in groups_per_node) if groups_per_node else (
            self.groups,) * node_count
        if len(self.stores_per_node) != node_count or any(
                len(stores) != len(set(stores)) or any(store < 0 for store in stores)
                for stores in self.stores_per_node):
            raise ValueError("each node needs distinct nonnegative store IDs")
        if len(self.groups_per_node) != node_count or any(
                len(groups) != len(set(groups)) or any(group < 0 for group in groups)
                for groups in self.groups_per_node):
            raise ValueError("each node needs distinct nonnegative group IDs")
        if wire_replicas and any(groups != self.groups_per_node[0] for groups in self.groups_per_node):
            raise ValueError("mixed group layouts require explicit test-controlled wiring")
        self.wire_replicas = wire_replicas
        self.data_root = Path(data_root).resolve() / self.run_id if data_root else self.artifacts / "nodes"
        self.data_paths = []
        self.network_created = False
        self.closed = False
        self.timings = {"status": "failed"}
        self.started = None

    def start(self):
        subnet = create_network(self)
        for index in range(self.node_count):
            data = node_data(self, index)
            address = str(subnet.network_address + index + 2) if subnet else None
            self.addresses.append(start_node(self, index, data, kv_arguments(self, index, address), address,
                                             preserve_network=self.preserve_network))
            self.origins.append(f"http://{self.addresses[-1]}:7000")
        self.ready()
        (self.artifacts / "endpoints.json").write_text(json.dumps({"nodes": [
            {"name": name, "ip": self.addresses[index], "management_port": 7000,
             "rpc_ports": list(range(7001, 7012)), "stores": self.stores_per_node[index], "groups": self.groups_per_node[index],
             "replica_id": index + 1, "data": str(self.data_paths[index])}
            for index, name in enumerate(self.servers)]}, indent=2))
        if not self.wire_replicas:
            return self
        wire_nodes(self.origins, self.addresses, self.stores_per_node, self.groups, request)
        return self

    def ready(self):
        ready_nodes(self.origins, self.stores_per_node, self.groups_per_node, request, wait)

    def crash_restart(self):
        started = time.monotonic()
        try:
            expected = [request(origin, "/topology") for origin in self.origins]
            for name in self.servers:
                self.command("kill", "--signal", "KILL", name)
            for name in self.servers:
                self.command("start", name)
            self.refresh_endpoints()
            for origin, before in zip(self.origins, expected, strict=True):
                wait(lambda origin=origin: request(origin, "/topology"),
                     lambda after, before=before: persisted_layout(after) == persisted_layout(before),
                     f"restored store/group/listener identity at {origin}")
        finally:
            self.timings["restart"] = time.monotonic() - started

    def refresh_endpoints(self):
        origins = []
        for index, name in enumerate(self.servers):
            info = json.loads(self.command("inspect", name))[0]
            address = info["NetworkSettings"]["Networks"][self.run_id]["IPAddress"]
            if address != self.addresses[index]:
                raise AssertionError("persisted peer RPC address changed during restart")
            origins.append(f"http://{address}:7000")
        self.origins = origins

    def client(self, bundle, phase, test_name="e2e_three_node_cluster_kv_put_batch_delete"):
        try:
            with Control(self):
                run_client(bundle, self.addresses, self.run_id, phase, self.artifacts, test_name,
                           timeout=getattr(self, "client_timeout", 120))
        except BaseException as error:
            (self.artifacts / f"client-{phase}.error").write_text(redact(str(error)))
            raise

    def control(self, body):
        action, node = body.get("action"), body.get("node")
        if action not in ("stop", "start", "kill", "pause", "unpause", "status"):
            raise ValueError("unsupported fixture action")
        if type(node) is not int or not 0 <= node < len(self.servers):
            raise ValueError("node is outside the owned fixture")
        name = self.servers[node]
        info = json.loads(self.command("inspect", name))[0]
        if info["Config"]["Labels"].get(LABEL) != self.run_id:
            raise ValueError("node ownership label changed")
        if action == "status":
            return info["State"]
        arguments = ("stop", "--time", "10", name) if action == "stop" else (action, name)
        self.command(*arguments)
        return json.loads(self.command("inspect", name))[0]["State"]

    def diagnostics(self):
        started = time.monotonic()
        for index, origin in enumerate(self.origins):
            try:
                (self.artifacts / f"topology-{index}.json").write_text(redact(request(origin, "/topology")))
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                (self.artifacts / f"topology-{index}.error").write_text(redact(str(error)))
        for name in self.containers:
            for operation in ("logs", "inspect", "top"):
                if operation == "top" and name not in self.servers:
                    continue
                try:
                    output = self.command(operation, name, "-eo", "pid,nlwp,comm") if operation == "top" else self.command(operation, name)
                    (self.artifacts / f"{name}-{operation}.log").write_text(redact(output))
                    if operation == "logs" and name in self.servers:
                        index = self.servers.index(name)
                        if index < len(self.data_paths) and (self.data_paths[index] / "log" / "container.log").exists():
                            (self.data_paths[index] / "log" / "container.log").write_text(redact(output))
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                    (self.artifacts / f"{name}-{operation}.error").write_text(redact(str(error)))
        self.timings["collect"] = time.monotonic() - started
        self.write_timings()

    def write_timings(self):
        (self.artifacts / "timing.json").write_text(json.dumps(self.timings, indent=2))

    def close(self):
        if self.closed:
            return
        errors = []
        started = time.monotonic()
        for kind, names in (("container", reversed(self.containers)),
                            ("network", [self.run_id] if self.network_created else [])):
            for name in names:
                try:
                    listing = ("-a",) if kind == "container" else ()
                    found = self.command(kind, "ls", *listing, "--filter", f"label={LABEL}={self.run_id}",
                                         "--filter", f"name={name}", "--format", "{{.Names}}" if kind == "container" else "{{.Name}}")
                    if name in found.splitlines():
                        args = ("-f", name) if kind == "container" else (name,)
                        self.command(kind, "rm", *args)
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                    errors.append(str(error))
        self.closed = not errors
        if errors:
            self.timings["status"] = "failed"
        self.timings["stop"] = time.monotonic() - started
        if self.started is not None:
            self.timings["total"] = time.monotonic() - self.started
        self.write_timings()
        if errors:
            raise RuntimeError("owned cleanup failed: " + "\n".join(errors))

    def __enter__(self):
        started = time.monotonic()
        self.started = started
        try:
            result = self.start()
            self.timings["start"] = time.monotonic() - started
            return result
        except BaseException:
            self.timings["start"] = time.monotonic() - started
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, _kind, error, _traceback):
        from failure_logs import print_failure
        if self.closed:
            self.timings["status"] = "pass" if error is None else "failed"
            self.write_timings()
            return
        collection_error = None
        try:
            self.diagnostics()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as diagnostic_error:
            collection_error = diagnostic_error
            if error is not None:
                error.add_note(str(diagnostic_error))
        if error is not None or collection_error is not None:
            print_failure(self.artifacts, self.data_paths, getattr(self, "case_name", self.run_id),
                          error if error is not None else collection_error, redact)
        try:
            self.timings["status"] = "pass" if error is None and collection_error is None else "failed"
            self.close()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as cleanup_error:
            if error is None:
                raise
            error.add_note(str(cleanup_error))
        if error is None and collection_error is not None:
            raise collection_error
