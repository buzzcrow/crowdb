# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Real containerd host-network startup, bootstrap and persistence acceptance."""

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from artifact import receipt
from builder import tool
from runtime import Runtime


def request(path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request("http://127.0.0.1:9090" + path, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def wait(action, predicate, description, seconds=120):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = action()
            if predicate(last):
                return last
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
            last = str(error)
        time.sleep(0.25)
    raise AssertionError(f"{description} failed: {last}")


def scenario(runtime, image, data, secret, mode):
    command = [sys.executable, str(Path(__file__).resolve().parents[1] / "run-node.py"),
               "--runtime", "containerd", "--address", runtime.command[2], "--snapshotter", "native", "--image", image,
               "--name", "crowdb-node", "--network", "host", "--data-root", str(data),
               "--physical-host-id", "containerd-acceptance-host", "--interface", "eth0",
               "--cpus", "2", "--memory-mib", "1024", "--password-file", str(secret), "--startup-mode", mode]
    subprocess.run(command, check=True, timeout=90)
    try:
        if mode == "manual":
            snapshot = wait(lambda: request("/api/node/candidates"), lambda s: bool(s["local"]), "local discovery")
            discovery_id = snapshot["local"]["advertisement"]["discovery_id"]
            request("/api/racks", {"id": 1, "name": "containerd-rack"})
            node = request("/api/node/admit", {"discovery_id": discovery_id, "rack_id": 1,
                                              "ssh_user": "crowdb", "ssh_port": 2222, "ssh_password": "containerd-test"})
            request("/api/cluster/init", {"nodes": [node["node_id"]]})
            state = wait(lambda: request("/api/node/status"), lambda s: s["phase"] == "active", "Group 0 bootstrap")
            cluster = state["cluster_id"]
            wait(lambda: runtime.invoke("exec", "crowdb-node", "crowdb-monitor", "readiness"), lambda _: True, "manual readiness")
        else:
            wait(lambda: runtime.invoke("exec", "crowdb-node", "crowdb-monitor", "readiness"), lambda _: True, "automatic readiness", 240)
            authority = request("/api/authority")
            assert authority["available"] and authority["source"] == "group0", authority
            try:
                request("/api/racks", {"id": 9, "name": "forbidden"})
            except urllib.error.HTTPError as error:
                assert error.code == 403, error
            else:
                raise AssertionError("single-node mode accepted a management mutation")
        key = runtime.invoke("exec", "crowdb-node", "cat", "/opt/crowdb/data/ssh/id_ed25519.pub")
        runtime.invoke("stop", "--time", "30", "crowdb-node")
        runtime.invoke("rm", "crowdb-node")
        subprocess.run(command, check=True, timeout=90)
        if mode == "manual":
            restored = wait(lambda: request("/api/node/status"), lambda s: s["phase"] == "active", "persisted cluster")
            assert restored["cluster_id"] == cluster
            assert request("/api/node/candidates")["local"]["advertisement"]["discovery_id"] == discovery_id
        else:
            wait(lambda: runtime.invoke("exec", "crowdb-node", "crowdb-monitor", "readiness"), lambda _: True, "persisted single-node readiness", 240)
        assert runtime.invoke("exec", "crowdb-node", "cat", "/opt/crowdb/data/ssh/id_ed25519.pub") == key
        runtime.verify("crowdb-node", 2, 1024, True)
        print(f"containerd {mode}: host network, resource limits, initialization and persisted identity passed", flush=True)
    finally:
        try:
            print(runtime.invoke("logs", "crowdb-node"), flush=True)
            runtime.invoke("rm", "-f", "crowdb-node")
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"containerd diagnostic/cleanup: {error}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--serve", action="store_true", help="serve one manual node for isolated-host cluster acceptance")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("this isolated host acceptance requires explicit root privileges")
    verified = receipt(args.archive)
    with tempfile.TemporaryDirectory(prefix="crowdb-containerd-") as directory:
        root = Path(directory)
        socket = str(root / "containerd.sock")
        config = root / "config.toml"
        config.write_text('version = 4\ndisabled_plugins = ["io.containerd.cri.v1.images", "io.containerd.cri.v1.runtime", "io.containerd.grpc.v1.cri", "io.containerd.podsandbox.controller.v1.podsandbox"]\n')
        command = [tool("containerd"), "--config", str(config), "--root", str(root / "content"),
                   "--state", str(root / "state"), "--address", socket]
        with (root / "daemon.log").open("w+") as log:
            daemon = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            runtime = Runtime("containerd", socket, snapshotter="native")
            try:
                wait(lambda: runtime.invoke("info"), lambda _: True, "containerd readiness", 30)
                subprocess.run([tool("ctr"), "--address", socket, "--namespace", "crowdb", "images", "import",
                                "--local", "--snapshotter", "native", "--platform", "linux/amd64", "--base-name", "docker.io/crowdb/crowdb-acceptance", "--digests", str(args.archive.resolve())], check=True)
                image = "docker.io/crowdb/crowdb-acceptance@" + verified["image_digest"]
                if args.serve:
                    data = root / "node"
                    data.mkdir()
                    os.chown(data, 10001, 10001)
                    secret = root / "password"
                    secret.write_text("containerd-test\n")
                    secret.chmod(0o600)
                    Path("/tmp/crowdb-containerd-address").write_text(socket)
                    subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] / "run-node.py"),
                                    "--runtime", "containerd", "--address", socket, "--snapshotter", "native",
                                    "--image", image, "--name", "crowdb-node", "--network", "host",
                                    "--data-root", str(data), "--physical-host-id", os.uname().nodename,
                                    "--interface", "eth0", "--cpus", "2", "--memory-mib", "1024",
                                    "--password-file", str(secret)], check=True, timeout=90)
                    while daemon.poll() is None:
                        time.sleep(1)
                    raise RuntimeError("containerd exited during cluster acceptance")
                for mode in ("manual", "single"):
                    data = root / mode
                    data.mkdir()
                    os.chown(data, 10001, 10001)
                    secret = root / "password"
                    secret.write_text("containerd-test\n")
                    secret.chmod(0o600)
                    scenario(runtime, image, data, secret, mode)
                print("Verified OCI digest: " + verified["image_digest"], flush=True)
            finally:
                os.killpg(daemon.pid, signal.SIGTERM)
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(daemon.pid, signal.SIGKILL)
                    daemon.wait(timeout=5)
                log.seek(0)
                print(log.read(), end="", flush=True)


if __name__ == "__main__":
    main()
