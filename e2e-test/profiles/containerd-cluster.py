# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three isolated Linux hosts, each running a native-network containerd node."""

import json
import os
import signal
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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
    prefix = os.environ.get("CROWDB_CONTAINERD_PREFIX", "crowdb-containerd-cluster-" + uuid.uuid4().hex[:8])
    archive = Path(os.environ.get("CROWDB_CONTAINERD_ARCHIVE", str(ROOT / "target/crowdb.oci.tar"))).resolve()
    data_root = Path(os.environ["CROWDB_CONTAINERD_DATA"])
    pixi = docker("version", "--format", "{{.Server.Version}}")
    print("Disposable-host provider Docker " + pixi, flush=True)
    pixi = Path(subprocess.check_output(["which", "pixi"], text=True).strip()).resolve()
    hosts, origins = [], []
    docker("network", "create", "--label", f"org.crowdb.e2e.run={prefix}", prefix)
    try:
        for index in range(3):
            host = prefix + "-" + str(index)
            hosts.append(host)
            data = data_root / f"host{index}"
            data.mkdir(parents=True)
            docker("run", "-d", "--label", f"org.crowdb.e2e.run={prefix}",
                   "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "1536m", "--privileged", "--cgroupns", "host", "--network", prefix, "--name", host,
                   "--mount", f"type=bind,source={ROOT},target={ROOT},readonly",
                   "--mount", f"type=bind,source={data},target=/opt/crowdb-e2e/data",
                   "--mount", f"type=bind,source={archive},target=/opt/crowdb-e2e/archive.tar,readonly",
                   "--mount", f"type=bind,source={archive}.json,target=/opt/crowdb-e2e/archive.tar.json,readonly",
                   "--env", "CROWDB_CONTAINERD_DATA=/opt/crowdb-e2e/data",
                   "--env", "CROWDB_CONTAINERD_NODE_CPUS=" + os.environ.get("CROWDB_CONTAINERD_NODE_CPUS", "2"),
                   "--mount", f"type=bind,source={pixi},target=/usr/local/bin/pixi,readonly",
                   "--mount", "type=bind,source=/etc/ssl/certs,target=/etc/ssl/certs,readonly",
                   "--workdir", str(ROOT), os.environ["CROWDB_CONTAINERD_HOST_IMAGE"], "/bin/bash", "-euc",
                   '! command -v docker; test ! -e /var/run/docker.sock; '
                   'pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime '
                   'python e2e-test/profiles/containerd-host.py /opt/crowdb-e2e/archive.tar --serve')
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
        errors = []
        for host in hosts:
            try:
                print(control(host, "logs"), flush=True)
                print(docker("logs", host), flush=True)
            except Exception as error:
                errors.append(str(error))
            finally:
                try:
                    docker("rm", "-f", host)
                except Exception as error:
                    errors.append(str(error))
        try:
            docker("network", "rm", prefix)
        except Exception as error:
            errors.append(str(error))
        if errors:
            raise RuntimeError("containerd diagnostics/cleanup failed: " + "; ".join(errors))


if __name__ == "__main__":
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt("owned containerd cluster interrupted")
    signal.signal(signal.SIGTERM, interrupt)
    main()
