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

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
LABEL = "org.crowdb.e2e.run"


def docker(*args, timeout=60):
    result = subprocess.run(["docker", *args], capture_output=True, text=True,
                            check=False, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"docker {args}: {result.stdout}\n{result.stderr}")
    return result.stdout.strip()


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


class Cluster:
    """The runner owns lifecycle; RPC clients live on this cluster's network."""

    def __init__(self, image, artifacts, command=docker):
        self.run_id = "crowdb-e2e-" + uuid.uuid4().hex
        self.image, self.command = image, command
        self.artifacts = Path(artifacts) / self.run_id
        self.artifacts.mkdir(parents=True)
        self.containers, self.volumes, self.origins, self.addresses = [], [], [], []
        self.servers = []
        self.network_created = False
        self.closed = False

    def start(self):
        # Record allocations before calls: a timed-out daemon response can have
        # created the resource. Cleanup still checks its ownership label.
        self.network_created = True
        self.command("network", "create", "--label", f"{LABEL}={self.run_id}", self.run_id)
        for index in range(3):
            name, volume = f"{self.run_id}-node{index}", f"{self.run_id}-data{index}"
            self.volumes.append(volume)
            self.command("volume", "create", "--label", f"{LABEL}={self.run_id}", volume)
            self.containers.append(name)
            self.servers.append(name)
            self.command("run", "-d", "--name", name, "--label", f"{LABEL}={self.run_id}",
                         "--network", self.run_id, "--network-alias", f"node{index}",
                         "--cpus", "0.5", "--memory", "512m", "--pids-limit", "256",
                         "--mount", f"type=volume,source={volume},target=/opt/crowdb/data",
                         "-p", "127.0.0.1::7000", "--entrypoint", "crowdb-kv-server", self.image,
                         "--root", "/opt/crowdb/data", "--management-port", "7000",
                         "--ports", "7001..7011", "--stores", str(index), "--groups", "1",
                         "--replica", str(index + 1), "--kv-backend", "block",
                         "--wal-backend", "block-device", "--election-profile", "e2e", "--log")
            info = json.loads(self.command("inspect", name))[0]
            self.addresses.append(info["NetworkSettings"]["Networks"][self.run_id]["IPAddress"])
            binding = info["NetworkSettings"]["Ports"]["7000/tcp"][0]
            if binding["HostIp"] != "127.0.0.1":
                raise AssertionError("management port is not bound to loopback")
            self.origins.append("http://127.0.0.1:" + binding["HostPort"])
        self.ready()
        combined = {"stores": []}
        for index, origin in enumerate(self.origins):
            topology = request(origin, "/topology")
            store = topology["stores"][0]
            assert store["store_id"] == index, topology
            assert store["groups"][0]["group_id"] == 1, topology
            # Wildcard listeners are not peer addresses. The C++ transport
            # consumes numeric IPs; discover those on this owned bridge.
            store["listen_addr"] = self.addresses[index] + ":" + store["listen_addr"].rsplit(":", 1)[1]
            combined["stores"].append(store)
        for index, origin in enumerate(self.origins):
            request(origin, f"/stores/{index}/groups/1/remotes/batch", combined)
        return self

    def ready(self):
        for origin in self.origins:
            wait(lambda origin=origin: request(origin, "/topology"),
                 lambda value: len(value["stores"]) == 1 and len(value["stores"][0]["groups"]) == 1,
                 "KV restored store/group readiness")

    def crash_restart(self):
        for name in self.servers:
            self.command("kill", "--signal", "KILL", name)
        for name in self.servers:
            self.command("start", name)
        self.refresh_endpoints()
        self.ready()

    def refresh_endpoints(self):
        origins = []
        for index, name in enumerate(self.servers):
            info = json.loads(self.command("inspect", name))[0]
            address = info["NetworkSettings"]["Networks"][self.run_id]["IPAddress"]
            if address != self.addresses[index]:
                raise AssertionError("persisted peer RPC address changed during restart")
            binding = info["NetworkSettings"]["Ports"]["7000/tcp"][0]
            if binding["HostIp"] != "127.0.0.1":
                raise AssertionError("management port is not bound to loopback")
            origins.append("http://127.0.0.1:" + binding["HostPort"])
        self.origins = origins

    def client(self, bundle, phase):
        name = self.run_id + "-client-" + phase
        self.containers.append(name)
        try:
            output = self.command("run", "--name", name, "--label", f"{LABEL}={self.run_id}",
                                  "--network", self.run_id, "--cpus", "0.5", "--memory", "512m",
                                  "--pids-limit", "256", "--mount", f"type=bind,source={bundle},target=/e2e,readonly",
                                  "-e", "LD_LIBRARY_PATH=/e2e/lib:/opt/crowdb/lib",
                                  "-e", "CROWDB_E2E_RPC=" + json.dumps(self.addresses),
                                  "-e", f"CROWDB_E2E_TOKEN={self.run_id}", "-e", f"CROWDB_E2E_PHASE={phase}",
                                  "--entrypoint", "/e2e/kv-test", self.image,
                                  "--ignored", "--exact", "e2e_three_node_cluster_kv_put_batch_delete", "--nocapture",
                                  timeout=120)
            (self.artifacts / f"client-{phase}.log").write_text(redact(output))
            print(output, flush=True)
        except BaseException as error:
            (self.artifacts / f"client-{phase}.error").write_text(redact(str(error)))
            raise

    def diagnostics(self):
        for index, origin in enumerate(self.origins):
            try:
                (self.artifacts / f"topology-{index}.json").write_text(redact(request(origin, "/topology")))
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                (self.artifacts / f"topology-{index}.error").write_text(redact(str(error)))
        for name in self.containers:
            for operation in ("logs", "inspect"):
                try:
                    output = self.command(operation, name)
                    (self.artifacts / f"{name}-{operation}.log").write_text(redact(output))
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                    (self.artifacts / f"{name}-{operation}.error").write_text(redact(str(error)))

    def close(self):
        if self.closed:
            return
        errors = []
        for kind, names in (("container", self.containers), ("volume", self.volumes),
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
            raise RuntimeError("owned cleanup failed: " + "\n".join(errors))

    def __enter__(self):
        try:
            return self.start()
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, _kind, error, _traceback):
        if self.closed:
            return
        collection_error = None
        try:
            self.diagnostics()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as diagnostic_error:
            collection_error = diagnostic_error
            if error is not None:
                error.add_note(str(diagnostic_error))
        try:
            self.close()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as cleanup_error:
            if error is None:
                raise
            error.add_note(str(cleanup_error))
        if error is None and collection_error is not None:
            raise collection_error
