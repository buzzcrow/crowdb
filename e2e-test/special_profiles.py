# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit host deployment profile, outside the concurrent bridge domain matrix."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from fixture import docker, redact
from image_identity import verify_server_sources
from client import bundle_client

ROOT = Path(__file__).resolve().parents[1]


def cleanup_owned_host(name, data, image):
    names = docker("container", "ls", "-a", "--filter", f"name={name}", "--format", "{{.Names}}")
    if name not in names.splitlines():
        return
    node = json.loads(docker("inspect", name))[0]
    mounts = node.get("Mounts", [])
    if node["Image"] != image or not any(mount.get("Destination") == "/opt/crowdb/data"
            and mount.get("Source") == str(data / "node") for mount in mounts):
        raise ValueError("host cleanup refuses changed image or data ownership")
    docker("rm", "-f", name)


def cleanup_owned_media(name, data, image):
    helper = name + "-media"
    names = docker("container", "ls", "-a", "--filter", f"name={helper}", "--format", "{{.Names}}")
    if helper not in names.splitlines():
        return
    current = json.loads(docker("inspect", helper))[0]
    if (current["Image"] != image or current["Config"].get("Labels", {}).get("org.crowdb.e2e.run") != name
            or not any(mount.get("Destination") == "/owned" and mount.get("Source") == str(data)
                       for mount in current.get("Mounts", []))):
        raise ValueError("media cleanup refuses changed image or data ownership")
    docker("rm", "-f", helper)


def run_host(args, parser):
    if args.layer != "all" or args.scope != "all" or args.case or args.require_isolation:
        parser.error("host profile has one deployment acceptance; domain selectors do not apply")
    if args.concurrency != 1:
        parser.error("host fixed ports require --concurrency 1 and an exclusive host")
    if args.cpus < 1.5 or args.memory_mib < 1536:
        parser.error("host profile requires a shared 1.5-CPU scheduling allowance and 1536 MiB including the node and host client")
    if args.list:
        print(json.dumps([{"scope":"deployment.host", "case":"host_network_admission_and_recovery", "nodes":1}]))
        return
    if sys.platform != "linux":
        parser.error("host deployment acceptance requires Linux")
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux" or info["NCPU"] < 1.5 or info["MemTotal"] < 1536 * 1048576:
        parser.error("host deployment requires a Linux daemon with 1.5 CPU / 1536 MiB capacity")
    identity = json.loads(docker("image", "inspect", args.image))[0]
    labels = identity["Config"].get("Labels") or {}
    try:
        source = verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"))
    except ValueError as error:
        parser.error(str(error))
    run_root = args.artifacts / ("run-" + uuid.uuid4().hex)
    run_root.mkdir(parents=True)
    data = (args.data_root / run_root.name / "host" if args.data_root else ROOT / ".crowdb-runtime/test-data/container-e2e" / run_root.name / "host").resolve()
    data.mkdir(parents=True, mode=0o700)
    (run_root / "identity.json").write_text(json.dumps({"profile":"host", "id":identity["Id"],
        "labels":labels, "sources":source, "concurrency":1, "cpus":1.5, "memory_mib":1536,
        "host_data":str(data), "container_data":"/opt/crowdb/data",
        "core_policy":Path("/proc/sys/kernel/core_pattern").read_text().strip()}, indent=2))
    env = os.environ.copy()
    client = bundle_client(run_root / "media-client")
    name = "crowdb-e2e-host-" + run_root.name.removeprefix("run-")
    env.update(CROWDB_CONTAINER_IMAGE=identity["Id"], CROWDB_E2E_PROFILE_DATA=str(data),
               CROWDB_E2E_PROFILE_NAME=name, CROWDB_MEDIA_CLIENT=str(client),
               CROWDB_MEDIA_HOST_ID="docker-acceptance-host")
    started = time.monotonic()
    status, code = "failed", None
    print(f"Host deployment profile: exact image {identity['Id']}; artifacts {run_root}", flush=True)
    child = subprocess.Popen([sys.executable, str(ROOT / "e2e-test/profiles/host-node.py")],
        cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        stdout, _ = child.communicate()
        code = child.returncode
        output = redact(stdout)
        (run_root / "client-host.log").write_text(output)
        print(output, end="", flush=True)
        if code:
            raise RuntimeError("host deployment acceptance failed; inspect retained client log and node data")
        status = "pass"
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                stdout, _ = child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                stdout, _ = child.communicate(timeout=5)
            (run_root / "client-host.log").write_text(redact(stdout))
        try:
            cleanup_owned_host(name, data, identity["Id"])
            cleanup_owned_media(name, data, identity["Id"])
        except Exception:
            status = "failed"
            raise
        finally:
            (run_root / "results.json").write_text(json.dumps({"profile":"host", "status":status,
                "exit_code":code, "wall_seconds":time.monotonic()-started, "passed":int(status=="pass"),
                "failed":int(status!="pass"), "skipped":0}, indent=2))
