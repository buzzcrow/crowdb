# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three isolated Linux hosts, each running a native-network containerd node."""

import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def docker(*arguments):
    return subprocess.check_output(["docker", *arguments], text=True, timeout=60).strip()


def request(origin, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    try:
        with HTTP.open(urllib.request.Request(origin + path, data=data, headers={"Content-Type": "application/json"}), timeout=45) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        error.add_note(error.read().decode())
        raise


def wait(action, predicate, description, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = action()
            if predicate(last):
                return last
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            last = str(error)
        time.sleep(0.25)
    raise AssertionError(f"{description}: {last}")


def control(host, operation):
    return docker("exec", host, "/bin/bash", "-euc",
                  'pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime '
                  'python container/oci-image/control.py --runtime containerd --snapshotter native --address "$(cat /tmp/crowdb-containerd-address)" "$1" crowdb-node',
                  "_", operation)


def main():
    prefix = "crowdb-containerd-cluster-" + uuid.uuid4().hex[:8]
    pixi = docker("version", "--format", "{{.Server.Version}}")
    print("Disposable-host provider Docker " + pixi, flush=True)
    pixi = Path(subprocess.check_output(["which", "pixi"], text=True).strip()).resolve()
    hosts, origins = [], []
    docker("network", "create", prefix)
    try:
        for index in range(3):
            host = prefix + "-" + str(index)
            hosts.append(host)
            docker("run", "-d", "--privileged", "--cgroupns", "host", "--network", prefix, "--name", host,
                   "--mount", f"type=bind,source={ROOT},target={ROOT},readonly",
                   "--mount", f"type=bind,source={pixi},target=/usr/local/bin/pixi,readonly",
                   "--mount", "type=bind,source=/etc/ssl/certs,target=/etc/ssl/certs,readonly",
                   "--workdir", str(ROOT), "ubuntu:24.04", "/bin/bash", "-euc",
                   '! command -v docker; test ! -e /var/run/docker.sock; '
                   'pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime '
                   'python container/oci-image/tests/containerd-host.py target/crowdb.oci.tar --serve')
            address = json.loads(docker("inspect", host))[0]["NetworkSettings"]["Networks"][prefix]["IPAddress"]
            origins.append("http://" + address + ":9090")
        snapshots = [wait(lambda origin=origin: request(origin, "/api/node/candidates"),
                          lambda value: bool(value["local"]), "containerd host discovery") for origin in origins]
        identities = {snapshot["local"]["advertisement"]["discovery_id"] for snapshot in snapshots}
        assert len(identities) == 3
        wait(lambda: request(origins[0], "/api/node/candidates"),
             lambda value: len(value["candidates"]) == 3, "cross-host discovery")
        request(origins[0], "/api/racks", {"id": 1, "name": "containerd-cluster"})
        members = [request(origins[0], "/api/node/admit", {"discovery_id": identity, "rack_id": 1,
                    "ssh_user": "crowdb", "ssh_port": 2222, "ssh_password": "containerd-test"})["node_id"]
                   for identity in sorted(identities)]
        request(origins[0], "/api/cluster/init", {"nodes": members})
        states = [wait(lambda origin=origin: request(origin, "/api/node/status"),
                       lambda value: value["phase"] == "active", "shared Group 0") for origin in origins]
        assert len({state["cluster_id"] for state in states}) == 1
        control(hosts[2], "liveness")
        control(hosts[2], "stop")
        topology = wait(lambda: request(origins[0], "/api/racks?recursive=1"),
                        lambda value: len(value["items"][0]["nodes"]) == 3,
                        "surviving Group 0 authority refresh", seconds=10)
        assert len(topology["items"][0]["nodes"]) == 3, topology
        request(origins[0], "/api/racks", {"id": 2, "name": "quorum-write"})
        control(hosts[2], "restart")
        restored = wait(lambda: request(origins[2], "/api/node/status"),
                        lambda value: value["phase"] == "active", "member recovery")
        assert restored["cluster_id"] == states[2]["cluster_id"]
        control(hosts[2], "liveness")
        assert any(rack["id"] == 2 for rack in request(origins[2], "/api/racks"))
        for host in hosts:
            control(host, "readiness")
        print("containerd three-host discovery, SSH admission, Group 0 quorum and member recovery passed", flush=True)
    finally:
        for host in hosts:
            try:
                print(control(host, "logs"), flush=True)
                print(docker("logs", host), flush=True)
            finally:
                docker("rm", "-f", host)
        docker("network", "rm", prefix)


if __name__ == "__main__":
    main()
