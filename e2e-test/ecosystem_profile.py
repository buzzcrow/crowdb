# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit manual external-engine profiles consume one formal server image."""

import hashlib
import json
from pathlib import Path
import shutil
import time
import uuid
from fixture import docker
from image_identity import verify_server_sources
from ecosystem_fixture import EcosystemCluster
from clients.ecosystem.acceptance import verify
from clients.ecosystem.trino_client import IMAGE as TRINO_IMAGE

ROOT = Path(__file__).resolve().parents[1]


def run_profile(args, parser):
    all_engines = args.profile == "ecosystem"
    cpus, memory = 3 + int(all_engines), 10240 if all_engines else 6144
    if args.layer != "all" or args.scope != "all" or args.case or args.require_isolation:
        parser.error("ecosystem is an explicit manual profile; domain selectors do not apply")
    if args.node_cpus <= 0 or args.concurrency != 1 or args.cpus < cpus or args.memory_mib < memory:
        parser.error(f"{args.profile} requires concurrency 1, {cpus:g} CPU / {memory} MiB including host engines")
    if args.list:
        print(json.dumps([{"scope":"iceberg.manual.ecosystem", "case":"all_engines" if all_engines else "python_duckdb", "nodes":1}]))
        return
    environment = ROOT / ".pixi/envs/iceberg-ecosystem"
    if not (environment / "bin/python").is_file() or not (environment / "bin/java").is_file():
        parser.error("ecosystem requires pixi install --locked -e iceberg-ecosystem and its pinned client tools")
    if all_engines:
        try:
            docker("image", "inspect", TRINO_IMAGE)
        except RuntimeError:
            parser.error("ecosystem requires its pinned Trino client image; run pixi run docker pull " + TRINO_IMAGE)
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux" or info["NCPU"] < cpus or info["MemTotal"] < memory * 1048576:
        parser.error(f"ecosystem requires a Linux daemon with {cpus:g} CPU / {memory} MiB")
    image = json.loads(docker("image", "inspect", args.image))[0]
    labels = image["Config"].get("Labels") or {}
    sources = verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"))
    run_root = args.artifacts / ("run-" + uuid.uuid4().hex)
    run_root.mkdir(parents=True)
    data = (args.data_root / run_root.name / "ecosystem" if args.data_root else
            ROOT / ".crowdb-runtime/test-data/container-e2e" / run_root.name / "ecosystem").resolve()
    data.mkdir(parents=True, mode=0o700)
    cluster = EcosystemCluster(image["Id"], data / "runtime", data_root=data / "nodes")
    cluster.node_cpus = args.node_cpus
    identity = {"profile":args.profile,"id":image["Id"],"labels":labels,"sources":sources,
        "host_data":str(data),"concurrency":1,"cpus":cpus,"memory_mib":memory,
        "node_cpus":args.node_cpus,"client_environment":str(environment),
        "client_lock_sha256":hashlib.sha256((ROOT / "pixi.lock").read_bytes()).hexdigest(),
        "trino_image":TRINO_IMAGE if all_engines else None}
    (run_root / "identity.json").write_text(json.dumps(identity,indent=2))
    started, status = time.monotonic(), "failed"
    print(f"Manual {args.profile}: exact image {image['Id']}; artifacts {run_root}", flush=True)
    try:
        with cluster:
            identity.update(network=cluster.run_id, addresses=cluster.addresses, node_data=[str(path) for path in cluster.data_paths])
            (run_root / "identity.json").write_text(json.dumps(identity,indent=2))
            verify(cluster,data,all_engines)
        status = "pass"
    finally:
        if (data / "results").is_dir():
            shutil.copytree(data / "results",run_root / "clients",dirs_exist_ok=True)
        (run_root / "results.json").write_text(json.dumps({"profile":args.profile,"status":status,
            "wall_seconds":time.monotonic()-started,"passed":int(status=="pass"),"failed":int(status!="pass"),"skipped":0},indent=2))
