#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Real system-Docker bridge, SSH admission and shared Group 0 acceptance."""
import json
import ipaddress
import hashlib
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid

IMAGE = os.environ.get("CROWDB_CONTAINER_IMAGE", "crowdb-iceberg-single-node:dev")
PREFIX = "crowdb-node-test-" + uuid.uuid4().hex[:8]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def docker(*args):
    result = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=60)
    if result.returncode:
        displayed = list(args)
        if "--json" in displayed:
            displayed[displayed.index("--json") + 1] = "<node-control request>"
        raise RuntimeError(f"docker {' '.join(displayed)}\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def request(origin, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    try:
        reply = HTTP.open(urllib.request.Request(origin + path, data=data, headers={"Content-Type": "application/json"}), timeout=45)
    except urllib.error.HTTPError as error:
        error.add_note("Server response: " + error.read().decode())
        raise
    return json.load(reply)


def wait(action, predicate, description, budget=10):
    deadline = time.monotonic() + budget
    last = None
    while time.monotonic() < deadline:
        try:
            last = action()
            if predicate(last):
                return last
        except (OSError, urllib.error.HTTPError) as error:
            last = str(error)
        time.sleep(0.1)
    raise AssertionError(f"{description}: {last}")


def scenario(filename):
    spec = spec_from_file_location(filename, Path(__file__).with_name(filename))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def verify_incomplete_preparation(name, origin):
    identity = request(origin, "/api/node/candidates")["local"]["advertisement"]["discovery_id"]
    intent = {"version": 1, "members": [999], "racks": [{"id": 1, "name": "interrupted"}],
              "nodes": [{"id": 999, "rack_id": 1, "host": "127.0.0.1", "ssh_port": 2222,
                         "ssh_user": "crowdb", "ssh_credential_ref": "id_ed25519"}],
              "servers": [{"id": "kv-999", "node_id": 999, "management_url": "http://127.0.0.1:10000",
                           "rpc_url": "127.0.0.1:10100"}]}
    nodes = [{"discovery_id": identity, "node_id": 999, "physical_host_id": "one-docker-host",
              "rack_id": 1, "host": "127.0.0.1", "ssh_port": 2222, "ssh_user": "crowdb",
              "operation_id": str(uuid.uuid4()), "confirmed": False, "cancelled": False}]
    digest = hashlib.sha256(json.dumps([intent, nodes], separators=(",", ":")).encode()).hexdigest()
    bootstrap = {"cluster_id": str(uuid.uuid4()), "operation_id": str(uuid.uuid4()),
                 "configuration_digest": digest}
    command = {"command": "start_kv", "node_id": 999, "bootstrap": bootstrap,
               "manifest": {"identity": bootstrap, "intent": intent, "nodes": nodes},
               "credentials": {"environment": "invalid"}}
    try:
        docker("exec", "--user", "crowdb", name, "crowdb-monitor", "control", "--json", json.dumps(command))
    except RuntimeError as error:
        assert "credential" in str(error).lower(), error
    else:
        raise AssertionError("invalid credentials must reject preparation")
    accepted = json.loads(docker("exec", name, "cat", "/opt/crowdb/data/accepted-node.json"))
    assert accepted["prepared"] is False, accepted
    docker("restart", name)
    mapping = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["9090/tcp"][0]
    origin = "http://127.0.0.1:" + mapping["HostPort"]
    wait(lambda: request(origin, "/api/node/candidates"), lambda result: bool(result["local"]), "UI after incomplete preparation restart")
    # The reserved identity remains, but a failed preparation cannot start a replica.
    assert docker("exec", name, "sh", "-c", "test ! -d /opt/crowdb/data/kv/node-999") == ""
    cleanup = {"command": "cleanup", "bootstrap": bootstrap, "confirm_delete_system_store": True}
    docker("exec", "--user", "crowdb", name, "crowdb-monitor", "control", "--json", json.dumps(cleanup))
    wait(lambda: request(origin, "/api/node/status"), lambda result: result["phase"] == "unbound_draft", "explicit preparation cleanup")
    print("Incomplete preparation survives restart without starting KV", flush=True)
    return origin


def main():
    names = []
    volumes = []
    origins = []
    network_created = False
    try:
        networks = docker("network", "ls", "--quiet").splitlines()
        occupied = [ipaddress.ip_network(config["Subnet"]) for network in json.loads(docker("network", "inspect", *networks))
                    for config in (network["IPAM"]["Config"] or []) if config.get("Subnet")]
        subnets = [ipaddress.ip_network(f"172.30.{index}.0/24") for index in range(256)]
        subnet = next(candidate for candidate in subnets if not any(candidate.overlaps(network) for network in occupied if network.version == 4))
        docker("network", "create", "--subnet", str(subnet), PREFIX)
        network_created = True
        for index in range(3):
            name = f"{PREFIX}-{index}"
            names.append(name)
            volume = f"{name}-data"
            volumes.append(volume)
            docker("run", "-d", "--name", name, "--network", PREFIX, "--cpus", "2", "--memory", "1g",
                   "--mount", f"type=volume,source={volume},target=/opt/crowdb/data",
                   "-e", "CROWDB_STARTUP_MODE=manual", "-e", "CROWDB_PHYSICAL_HOST_ID=one-docker-host",
                   "-p", "127.0.0.1::9090", IMAGE)
            mapping = json.loads(docker("inspect", name))[0]["NetworkSettings"]["Ports"]["9090/tcp"][0]
            origins.append("http://127.0.0.1:" + mapping["HostPort"])
        identities, records, clusters = scenario("node-bootstrap.py").verify(
            docker, request, wait, verify_incomplete_preparation, PREFIX, names, origins)
        admission_case = scenario("node-admission.py")
        fourth, fourth_origin, joined = admission_case.verify(docker, request, wait, IMAGE, PREFIX, names, volumes, origins, identities, records)
        admission_case.services(docker, request, wait, origins, fourth, joined)
        request(origins[1], "/api/racks", {"id": 2, "name": "shared-rack"})
        assert len(request(origins[0], "/api/racks")) == 2
        # A minority keeps diagnostics but cannot authorize a new SSH management step.
        for name in names[1:3]:
            docker("pause", name)
        try:
            unavailable = wait(lambda: request(origins[0], "/api/node/status"),
                               lambda status: status["phase"] == "authority_unavailable", "minority authority expiry")
            assert unavailable["phase"] == "authority_unavailable", unavailable
            assert unavailable["cluster_id"] == clusters[0]
            stored = request(origins[0], "/api/stores/0/groups/2/kv/put",
                             {"key": "independent-authority", "value": "available", "client_id": 992, "seq": 1})
            assert stored["ok"], stored
            assert request(origins[0], "/api/stores/0/groups/2/kv/get?key=independent-authority")["value_utf8"] == "available"
            print("Data group quorum remains available without Group 0 authority", flush=True)
            try:
                request(origins[0], "/api/racks", {"id": 3, "name": "forbidden-minority"})
                raise AssertionError("Minority accepted a topology mutation")
            except urllib.error.HTTPError as error:
                assert error.code == 502, error
            assert request(origins[0], "/api/node/candidates")["local"]
        finally:
            for name in names[1:3]:
                docker("unpause", name)
        wait(lambda: request(origins[0], "/api/node/status"), lambda result: result["phase"] == "active", "authority recovery")
        scenario("node-recovery.py").verify(docker, request, wait, IMAGE, PREFIX, names, volumes, origins, clusters[0])
        operation_id = request(origins[0], "/api/node/status")["operation_id"]
        docker("stop", "--time", "10", fourth)
        result = request(origins[0], "/api/node/cleanup", {"operation_id": operation_id, "confirm_delete_system_store": True})
        assert result["pending"] == [joined["node_id"]], result
        assert request(origins[0], "/api/node/status")["phase"] == "cleanup_in_progress"
        docker("start", fourth)
        mapping = json.loads(docker("inspect", fourth))[0]["NetworkSettings"]["Ports"]["9090/tcp"][0]
        fourth_origin = "http://127.0.0.1:" + mapping["HostPort"]
        origins[3] = fourth_origin
        wait(lambda: request(fourth_origin, "/api/node/candidates"), lambda result: bool(result["local"]), "unreachable cleanup target returns")
        result = request(origins[0], "/api/node/cleanup", {"operation_id": operation_id, "confirm_delete_system_store": True})
        assert result["pending"] == [], result
        for origin in origins:
            wait(lambda: request(origin, "/api/node/status"), lambda result: result["phase"] == "unbound_draft", "cleanup releases UI binding")
        print("Shared rack mutations, minority gating and explicit cleanup passed", flush=True)

    except Exception:
        for name in names:
            try:
                print(docker("logs", name), flush=True)
                print(docker("exec", name, "sh", "-c", "cat /opt/crowdb/data/log/web/* /opt/crowdb/data/log/kv/* 2>/dev/null"), flush=True)
            except RuntimeError as diagnostic:
                print(diagnostic, flush=True)
        raise
    finally:
        for name in names:
            docker("rm", "-f", name)
        for volume in volumes:
            docker("volume", "rm", volume)
        if network_created:
            docker("network", "rm", PREFIX)


if __name__ == "__main__":
    main()
