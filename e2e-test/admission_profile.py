# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Manual release nodes and host browsers exercise public SSH admission."""

import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import time
import uuid
from fixture import docker, redact
from image_identity import verify_server_sources

ROOT = Path(__file__).resolve().parents[1]


def cleanup(prefix, data=None, image=None):
    errors = []
    for kind in ("container", "network"):
        try:
            names = docker(kind, "ls", *(("-a",) if kind == "container" else ()),
                "--filter", f"label=org.crowdb.e2e.run={prefix}",
                "--format", "{{.Names}}" if kind == "container" else "{{.Name}}")
        except Exception as error:
            errors.append(str(error))
            continue
        for name in names.splitlines():
            try:
                if kind == "network":
                    if name != prefix:
                        raise ValueError("refusing foreign labeled network")
                else:
                    suffix = name.removeprefix(prefix + "-")
                    if suffix not in (*tuple(str(index) for index in range(5)), "prepare"):
                        raise ValueError("refusing foreign labeled container")
                    if data is not None:
                        node = json.loads(docker("inspect", name))[0]
                        sources = [str(data / str(index)) for index in range(5)] if suffix == "prepare" else [str(data / suffix)]
                        if node["Image"] != image or not any(mount.get("Source") in sources
                                and mount.get("Destination") == "/opt/crowdb/data" for mount in node["Mounts"]):
                            raise ValueError("refusing changed image or data ownership")
                docker(kind, "rm", *(("-f",) if kind == "container" else ()), name)
            except Exception as error:
                errors.append(str(error))
    if errors:
        raise RuntimeError("bridge admission cleanup failed: " + "; ".join(errors))


def run_profile(args, parser):
    cpus, memory = 5 * args.node_cpus + 1, 6144
    if args.layer != "all" or args.scope != "all" or args.case or args.require_isolation:
        parser.error("bridge admission is an explicit deployment profile; domain selectors do not apply")
    if args.node_cpus <= 0 or args.concurrency != 1 or args.cpus < cpus or args.memory_mib < memory:
        parser.error(f"bridge admission requires concurrency 1, {cpus:g} CPU / {memory} MiB for five nodes and host browser")
    if args.list:
        print(json.dumps([{"scope":"deployment.admission", "case":"ssh_bootstrap_admission_and_recovery", "nodes":5}]))
        return
    client = ROOT / "e2e-test/clients/console-ui"
    installed = client / "node_modules/@playwright/test/package.json"
    if not installed.is_file() or json.loads(installed.read_text())["version"] != json.loads(
            (client / "package.json").read_text())["devDependencies"]["@playwright/test"]:
        parser.error("bridge admission requires locked host browser dependencies; run pixi run npm ci --ignore-scripts in e2e-test/clients/console-ui")
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux" or info["NCPU"] < cpus or info["MemTotal"] < memory * 1048576:
        parser.error(f"bridge admission requires a Linux daemon with {cpus:g} CPU / {memory} MiB")
    identity = json.loads(docker("image", "inspect", args.image))[0]
    labels = identity["Config"].get("Labels") or {}
    source = verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"))
    run_root = args.artifacts / ("run-" + uuid.uuid4().hex)
    run_root.mkdir(parents=True)
    data = (args.data_root / run_root.name / "admission" if args.data_root else
            ROOT / ".crowdb-runtime/test-data/container-e2e" / run_root.name / "admission").resolve()
    data.mkdir(parents=True, mode=0o700)
    prefix = "crowdb-e2e-admission-" + run_root.name.removeprefix("run-")
    (run_root / "identity.json").write_text(json.dumps({"profile":"bridge-admission", "id":identity["Id"],
        "labels":labels, "sources":source, "cpus":cpus, "memory_mib":memory,
        "node_cpus":args.node_cpus, "host_data":str(data), "container_data":"/opt/crowdb/data",
        "ports":{"web":9090,"ssh":2222},
        "core_policy":Path("/proc/sys/kernel/core_pattern").read_text().strip()}, indent=2))
    env = os.environ.copy()
    env.update(CROWDB_CONTAINER_IMAGE=identity["Id"], CROWDB_E2E_PROFILE_DATA=str(data),
               CROWDB_E2E_PROFILE_NAME=prefix, CROWDB_E2E_NODE_CPUS=str(args.node_cpus))
    print(f"Bridge admission: exact image {identity['Id']}; artifacts {run_root}", flush=True)
    started, status, code = time.monotonic(), "failed", None
    child = subprocess.Popen([sys.executable, str(ROOT / "e2e-test/profiles/node-containers.py")],
        cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        output, _ = child.communicate()
        code = child.returncode
        output = redact(output)
        (run_root / "client-admission.log").write_text(output)
        print(output, end="", flush=True)
        if code:
            raise RuntimeError("bridge admission acceptance failed; see retained diagnostics")
        status = "pass"
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                output, _ = child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                output, _ = child.communicate(timeout=5)
            (run_root / "client-admission.log").write_text(redact(output))
        try:
            cleanup(prefix, data, identity["Id"])
        except Exception:
            status = "failed"
            raise
        finally:
            (run_root / "results.json").write_text(json.dumps({"profile":"bridge-admission", "status":status,
                "exit_code":code,"wall_seconds":time.monotonic()-started,"passed":int(status=="pass"),
                "failed":int(status!="pass"),"skipped":0},indent=2))
