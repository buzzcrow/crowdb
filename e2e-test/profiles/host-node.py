#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Same-image host networking, explicit credentials/resources and persistent recovery."""
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import uuid
import signal
from contextlib import nullcontext
from node import docker, request, wait, IMAGE
from setup_choices import diskless_setup
from node_resources import verify_reaping, verify_stopped
from docker_media import DockerMedia, NodeRuntime
from supplied_media import provision as provision_media, remove_and_rejoin, wait_for_descriptors


def main():
    name = os.environ.get("CROWDB_E2E_PROFILE_NAME", "crowdb-host-test-" + uuid.uuid4().hex[:8])
    with (nullcontext(os.environ["CROWDB_E2E_PROFILE_DATA"]) if "CROWDB_E2E_PROFILE_DATA" in os.environ
          else tempfile.TemporaryDirectory(prefix="crowdb-host-node-")) as directory:
        root = Path(directory)
        secret = root / "password"
        secret.write_text("host-ssh-acceptance")
        secret.chmod(0o600)
        data = root / "node"
        docker("run", "--rm", "--network", "none", "--entrypoint", "/bin/sh", "--mount",
               f"type=bind,source={root},target=/test", IMAGE, "-c",
               'mkdir /test/node; chown "10001:$1" /test/node; chmod 0770 /test/node', "sh", str(os.getgid()))
        image = json.loads(docker("image", "inspect", IMAGE))[0]["Id"]
        command = [sys.executable, "container/single-node-container/run-node.py", "--image", image,
                   "--name", name, "--network", "host", "--data-root", str(data), "--physical-host-id", "docker-acceptance-host",
                   "--interface", "lo", "--cpu-shares", "1024", "--worker-threads", "1", "--memory-mib", "1024", "--password-file", str(secret)]
        media = None
        runtime = NodeRuntime(name)
        api = lambda path, body=None: request("http://127.0.0.1:9090", path, body)
        observe = lambda action, predicate, description, seconds=10: wait(action, predicate, description, budget=seconds)
        def launch():
            # Only the owned fixture operator writes launcher receipts. SSH must
            # subsequently see a parent directory without group write permission.
            docker("exec", media.name, "chmod", "0770", "/owned/node")
            try:
                subprocess.run(command, check=True, timeout=60)
            finally:
                docker("exec", media.name, "chmod", "0755", "/owned/node")

        try:
            media = DockerMedia(root, name, image)
            command.extend(media.arguments())
            oversized = command.copy()
            oversized[oversized.index("--memory-mib") + 1] = str(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // (1024 * 1024) + 1)
            rejected = subprocess.run(oversized, text=True, capture_output=True, timeout=60)
            assert rejected.returncode != 0 and "boundaries must fit the host" in rejected.stderr, rejected.stderr
            launch()
            origin = "http://127.0.0.1:9090"
            snapshot = wait(lambda: request(origin, "/api/node/candidates"), lambda result: bool(result["local"]), "host-network discovery")
            owner = docker("exec", name, "stat", "-c", "%u:%a", "/opt/crowdb/data/disks")
            assert owner.split(":")[0] == "10001", f"supplied-media parent owner/mode: {owner}"
            docker("exec", "--user", "10001:10001", name, "test", "-w", "/opt/crowdb/data/disks")
            verify_reaping(docker, name, observe)
            discovery_id = snapshot["local"]["advertisement"]["discovery_id"]
            assert snapshot["local"]["hardware"]["memory_bytes"] <= 1024 * 1024 * 1024
            assert snapshot["local"]["hardware"]["logical_cpus"] > 0
            request(origin, "/api/racks", {"id":1,"name":"host-rack"})
            record = request(origin, "/api/node/admit", {"discovery_id":discovery_id,"rack_id":1,
                             "ssh_user":"crowdb","ssh_port":2222,"ssh_password":"host-ssh-acceptance", "setup":diskless_setup()})
            request(origin, "/api/cluster/init", {"nodes":[record["node_id"]]})
            active = wait(lambda: request(origin, "/api/node/status"), lambda state: state["phase"] == "active", "Group 0 bootstrap")
            assert active["phase"] == "active", active
            media_state = provision_media(media, data, record["node_id"], active["cluster_id"], api, observe)
            remove_and_rejoin(media, media_state, runtime, api, observe)
            key = docker("exec", name, "cat", "/opt/crowdb/data/ssh/id_ed25519.pub")
            docker("stop", "--time", "30", name)
            verify_stopped(docker, name)
            docker("rm", name)
            launch()
            recovered = wait(lambda: request(origin, "/api/node/status"), lambda result: result["phase"] == "active", "host persistent recovery")
            assert recovered["cluster_id"] == active["cluster_id"]
            assert request(origin, "/api/node/candidates")["local"]["advertisement"]["discovery_id"] == discovery_id
            assert docker("exec", name, "cat", "/opt/crowdb/data/ssh/id_ed25519.pub") == key
            wait_for_descriptors(media, media_state, runtime, api, observe)
            media.probe(media_state, "verify")
            assert not media.exclusive_available()
            media.verify_tails()
            cleanup = request(origin, "/api/node/cleanup", {"operation_id":active["operation_id"],"confirm_delete_system_store":True})
            assert cleanup["pending"] == [], cleanup
            print("Host networking, explicit credentials/resources and same-image persistent recovery passed", flush=True)
        except Exception:
            artifact = Path(".crowdb-runtime/artifacts") / name
            artifact.mkdir(parents=True, exist_ok=True)
            print(docker("logs", name), flush=True)
            logs = data / "log"
            if logs.exists():
                shutil.copytree(logs, artifact / "logs")
            try:
                topology = docker("exec", name, "curl", "-fsS", "http://127.0.0.1:10000/topology")
                (artifact / "topology.json").write_text(topology)
                print(topology, flush=True)
            except RuntimeError as error:
                print(f"Host topology diagnostic unavailable: {error}", flush=True)
            print(f"Host failure logs retained at {artifact.resolve()}", flush=True)
            raise
        finally:
            docker("rm", "-f", name)
            if media:
                media.close()
            if "CROWDB_E2E_PROFILE_DATA" not in os.environ:
                docker("run", "--rm", "--network", "none", "--entrypoint", "/bin/chmod", "--mount",
                       f"type=bind,source={root},target=/test", IMAGE, "-R", "0777", "/test/node")


if __name__ == "__main__":
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt("owned host profile interrupted")
    signal.signal(signal.SIGTERM, interrupt)
    main()
