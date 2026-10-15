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
               f"type=bind,source={root},target=/test", IMAGE, "-c", "mkdir /test/node; chown 10001:10001 /test/node")
        image = json.loads(docker("image", "inspect", IMAGE))[0]["Id"]
        command = [sys.executable, "container/single-node-container/run-node.py", "--image", image,
                   "--name", name, "--network", "host", "--data-root", str(data), "--physical-host-id", os.uname().nodename,
                   "--interface", "lo", "--cpu-shares", "1024", "--worker-threads", "1", "--memory-mib", "1024", "--password-file", str(secret)]
        try:
            oversized = command.copy()
            oversized[oversized.index("--memory-mib") + 1] = str(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // (1024 * 1024) + 1)
            rejected = subprocess.run(oversized, text=True, capture_output=True, timeout=60)
            assert rejected.returncode != 0 and "boundaries must fit the host" in rejected.stderr, rejected.stderr
            subprocess.run(command, check=True, timeout=60)
            origin = "http://127.0.0.1:9090"
            snapshot = wait(lambda: request(origin, "/api/node/candidates"), lambda result: bool(result["local"]), "host-network discovery")
            discovery_id = snapshot["local"]["advertisement"]["discovery_id"]
            assert snapshot["local"]["hardware"]["memory_bytes"] <= 1024 * 1024 * 1024
            assert snapshot["local"]["hardware"]["logical_cpus"] > 0
            request(origin, "/api/racks", {"id":1,"name":"host-rack"})
            record = request(origin, "/api/node/admit", {"discovery_id":discovery_id,"rack_id":1,
                             "ssh_user":"crowdb","ssh_port":2222,"ssh_password":"host-ssh-acceptance", "setup":diskless_setup()})
            request(origin, "/api/cluster/init", {"nodes":[record["node_id"]]})
            active = request(origin, "/api/node/status")
            assert active["phase"] == "active", active
            key = docker("exec", name, "cat", "/opt/crowdb/data/ssh/id_ed25519.pub")
            docker("stop", "--time", "30", name)
            docker("rm", name)
            subprocess.run(command, check=True, timeout=60)
            recovered = wait(lambda: request(origin, "/api/node/status"), lambda result: result["phase"] == "active", "host persistent recovery")
            assert recovered["cluster_id"] == active["cluster_id"]
            assert request(origin, "/api/node/candidates")["local"]["advertisement"]["discovery_id"] == discovery_id
            assert docker("exec", name, "cat", "/opt/crowdb/data/ssh/id_ed25519.pub") == key
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
            topology = docker("exec", name, "curl", "-fsS", "http://127.0.0.1:10000/topology")
            (artifact / "topology.json").write_text(topology)
            print(topology, flush=True)
            print(f"Host failure logs retained at {artifact.resolve()}", flush=True)
            raise
        finally:
            docker("rm", "-f", name)
            if "CROWDB_E2E_PROFILE_DATA" not in os.environ:
                docker("run", "--rm", "--network", "none", "--entrypoint", "/bin/chmod", "--mount",
                       f"type=bind,source={root},target=/test", IMAGE, "-R", "0777", "/test/node")


if __name__ == "__main__":
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt("owned host profile interrupted")
    signal.signal(signal.SIGTERM, interrupt)
    main()
