# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Acknowledged object and multipart recovery across each release service."""

import time
from pathlib import Path

from fixture import request, wait


SERVICE_NODES = {"group0": (0, 1, 2), "access": (12,), "chunkdb": (4,),
                 "diskdb": (3,), "diskio": tuple(range(5, 11)), "chunk-kv": (11,)}


def run_recovery(cluster, service):
    script = Path(__file__).parent / "clients/s3/restart.py"
    cluster.run_sdk("prepare", script, ["prepare"])
    nodes = SERVICE_NODES[service]
    started = time.monotonic()
    for node in nodes:
        cluster.control({"action": "kill", "node": node})
    if service == "access":
        cluster.run_sdk("verify-secondary", script, ["verify"], frontend=13)
    for node in nodes:
        cluster.control({"action": "start", "node": node})
    if service == "group0":
        for origin in cluster.origins[:3]:
            wait(lambda origin=origin: request(origin, "/topology"), bool, "restored Group-0")
    elif service == "access":
        cluster.ready_access(12)
    elif service == "chunkdb":
        cluster.ready_chunkdb()
    elif service == "chunk-kv":
        cluster.ready_chunk_kv()
    elif service == "diskdb":
        wait(lambda: request(f"http://{cluster.addresses[3]}:7101", "/ready"),
             lambda _: True, "recovered Diskdb readiness")
    elif service == "diskio":
        cluster.client(cluster.bundle, "storage-ready-recovered", "chunkdb::io::ready")
    cluster.timings["restart"] = time.monotonic() - started
    phase = "verify" if service == "access" else "verify-after-" + service + "-restart"
    cluster.run_sdk(phase, script, [phase], frontend=13 if service != "access" else 12)
    cluster.run_sdk("cleanup", script, ["cleanup"], frontend=13)
