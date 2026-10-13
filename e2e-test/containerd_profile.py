# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Three isolated Linux hosts share one verified formal OCI node artifact."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from fixture import docker, redact
from image_identity import verify_server_sources

ROOT = Path(__file__).resolve().parents[1]


def cleanup(prefix):
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
            if not name.startswith(prefix):
                errors.append("containerd cleanup refuses unexpected labeled resource")
                continue
            try:
                docker(kind, "rm", *(("-f",) if kind == "container" else ()), name)
            except Exception as error:
                errors.append(str(error))
    if errors:
        raise RuntimeError("containerd cleanup failed: " + "; ".join(errors))


def run_profile(args, parser):
    cluster = args.profile == "containerd-cluster"
    needed_cpu = 2 if cluster else 1.5
    needed_memory = 5120 if cluster else 2048
    if args.layer != "all" or args.scope != "all" or args.case or args.require_isolation:
        parser.error("containerd cluster is an explicit deployment profile; domain selectors do not apply")
    if args.node_cpus <= 0 or args.concurrency != 1 or args.cpus < needed_cpu or args.memory_mib < needed_memory:
        parser.error(f"{args.profile} requires --concurrency 1, {needed_cpu:g} CPU and {needed_memory} MiB including Linux hosts")
    if args.list:
        print(json.dumps([{"scope":"deployment.containerd", "case":"three_host_quorum_and_recovery" if cluster else "manual_and_single_host_recovery", "nodes":3 if cluster else 1}]))
        return
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux" or info["NCPU"] < needed_cpu or info["MemTotal"] < needed_memory * 1048576:
        parser.error(f"containerd profile requires a Linux Docker host with {needed_cpu:g} CPU / {needed_memory} MiB")
    spec = importlib.util.spec_from_file_location("crowdb_oci_artifact", ROOT / "container/oci-image/artifact.py")
    artifact = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(artifact)
    archive = args.archive.resolve()
    verified = artifact.receipt(archive)
    source = verify_server_sources(ROOT, verified["revision"])
    run_root = args.artifacts / ("run-" + uuid.uuid4().hex)
    run_root.mkdir(parents=True)
    data = (args.data_root / run_root.name / "containerd" if args.data_root else
            ROOT / ".crowdb-runtime/test-data/container-e2e" / run_root.name / "containerd").resolve()
    data.mkdir(parents=True, mode=0o700)
    prefix = "crowdb-e2e-containerd-" + run_root.name.removeprefix("run-")
    (run_root / "identity.json").write_text(json.dumps({"profile":args.profile, "artifact":verified,
        "sources":source,"concurrency":1,"cpus":needed_cpu,"memory_mib":needed_memory,
        "node_cpus":args.node_cpus,"host_data":str(data)}, indent=2))
    env = os.environ.copy()
    env.update(CROWDB_CONTAINERD_DATA=str(data), CROWDB_CONTAINERD_PREFIX=prefix,
               CROWDB_CONTAINERD_ARCHIVE=str(archive), CROWDB_CONTAINERD_HOST_IMAGE=verified["base_image"],
               CROWDB_CONTAINERD_NODE_CPUS=str(args.node_cpus))
    started = time.monotonic()
    status, code = "failed", None
    print(f"Containerd {args.profile} profile: {verified['image_digest']}; artifacts {run_root}", flush=True)
    script = "containerd-cluster.py" if cluster else "containerd-single-host.py"
    child = subprocess.Popen([sys.executable,str(ROOT / "e2e-test/profiles" / script)],
        cwd=ROOT,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    try:
        output, _ = child.communicate()
        code = child.returncode
        output = redact(output)
        (run_root / "client-containerd.log").write_text(output)
        print(output,end="",flush=True)
        if code:
            raise RuntimeError("containerd cluster acceptance failed; see retained diagnostics")
        status = "pass"
    finally:
        if child.poll() is None:
            child.terminate()
            try:
                output, _ = child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                output, _ = child.communicate(timeout=5)
            (run_root / "client-containerd.log").write_text(redact(output))
        try:
            cleanup(prefix)
        except Exception:
            status = "failed"
            raise
        finally:
            (run_root / "results.json").write_text(json.dumps({"profile":args.profile, "status":status,
                "wall_seconds":time.monotonic()-started,"exit_code":code,"passed":int(status=="pass"),
                "failed":int(status!="pass"),"skipped":0},indent=2))
