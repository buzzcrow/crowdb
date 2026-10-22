#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Real Chunk-KV RPC/native-tree corpus through the shared native/OCI launcher."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools/benchmark"))
from rpc_bench_process import Runtime, start  # noqa: E402

CASE = "real_chunk_kv_service_preserves_large_controls_and_typed_results"


def build(pixi, output):
    command = [pixi, "run", "--", "cargo", "test", "-j32", "-p", "crowdb-chunk-kv-server",
               "--test", "rdma_rpc_test", "--no-run", "--message-format=json"]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    (output / "build.jsonl").write_text(result.stdout)
    print(result.stderr, end="", flush=True)
    executable = None
    for line in result.stdout.splitlines():
        item = json.loads(line)
        if item.get("reason") == "compiler-message":
            print(item["message"].get("rendered", ""), end="", flush=True)
        if item.get("target", {}).get("name") == "rdma_rpc_test" and item.get("executable"):
            executable = Path(item["executable"])
    result.check_returncode()
    if executable is None:
        raise RuntimeError("Cargo did not identify the Chunk-KV fixture executable")
    return executable


def wait_json(path, process, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before {path.name}: {process.returncode}")
        try:
            return json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.01)
    raise RuntimeError(f"deadline waiting for {path}")


def check(result, server, profile):
    if result["errors"] or result["verification_errors"] or result["payload_bytes"] <= 3 << 20:
        raise RuntimeError("invalid corpus or unexpected errors")
    if result["expected_typed_errors"] != 2 or result["expected_transport_errors"] != int(profile == "disconnect"):
        raise RuntimeError("incorrect expected error counts")
    snapshots = [result["transport"][group]["transport"] for group in ("metadata", "aggregate")]
    if profile == "tcp":
        if any(item["rdma"] is not None for item in [server, *snapshots]):
            raise RuntimeError("explicit TCP unexpectedly attempted RDMA")
    elif profile == "fallback":
        if any(item["rdma"]["fallback_count"] != 1 for item in snapshots):
            raise RuntimeError("each fixed class must fall back exactly once before payload")
    else:
        qps = []
        for group, capacity in (("metadata", 65536), ("aggregate", 262144)):
            info = result["connections"][group]["info"]
            if info["effective_data_capacity"] != capacity or info["effective_sq_depth"] < 2 or info["effective_rq_depth"] < 2:
                raise RuntimeError("actual QP does not match the selected class or queue window")
            qps.append(info["qpair_number"])
        if len(set(qps)) != 2:
            raise RuntimeError("metadata and aggregate groups must use separate QPs")
        for item in [server, *snapshots]:
            rdma = item["rdma"]
            for field in ("fallback_count", "protocol_errors", "receive_post_errors", "control_stream_timeouts"):
                if rdma[field] != 0:
                    raise RuntimeError(f"unexpected RDMA {field}={rdma[field]}")
            if profile != "disconnect" and (rdma["cq_errors"] or rdma["send_errors"]):
                raise RuntimeError("unexpected CQ/SEND errors")
            if rdma["control_fragments_received"] == 0 or rdma["control_fragments_sent"] == 0:
                raise RuntimeError("large controls did not traverse RDMA fragments")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("rdma", "tcp", "fallback", "disconnect"), required=True)
    parser.add_argument("--topology", choices=("native", "containers"), default="native")
    parser.add_argument("--address", default="10.200.1.1")
    parser.add_argument("--device", default="rocep35s0f0")
    parser.add_argument("--image", default="crowdb-node:dev")
    args = parser.parse_args()
    pixi = shutil.which("pixi")
    if not pixi:
        raise RuntimeError("pixi is required")
    output = (Path(os.environ.get("CROWDB_RUNTIME_ROOT", ROOT / ".crowdb-runtime"))
              / "artifacts/rdma-chunk-kv" / uuid.uuid4().hex).resolve()
    output.mkdir(parents=True)
    binary = build(pixi, output)
    if args.topology == "containers":
        args.image = subprocess.check_output([pixi, "run", "--", "docker", "image", "inspect",
                                              args.image, "--format", "{{.Id}}"], text=True).strip()
    address = "127.0.0.1" if args.profile == "tcp" else args.address
    with socket.socket() as reservation:
        reservation.bind((address, 0))
        port = reservation.getsockname()[1]
    env = {key: value for key, value in os.environ.items() if not key.startswith("CROWDB_RDMA_")}
    env.update(CROWDB_RUNTIME_ROOT=str(output / "runtime"), CROWDB_RDMA_DEVICE=args.device,
               CROWDB_RDMA_CHUNK_KV_ENDPOINT=f"{address}:{port}", CROWDB_RDMA_CHUNK_KV_OUTPUT=str(output),
               CROWDB_RDMA_CHUNK_KV_PROFILE=args.profile)
    topology = "containers-tcp" if args.topology == "containers" and args.profile == "tcp" else args.topology
    runtime = Runtime(ROOT, output, pixi, topology, args.image)
    manifest = {"profile": args.profile, "topology": args.topology, "physical_hosts": 1,
                "image": args.image if args.topology == "containers" else None,
                "source": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "scope": "production RPC/service/native tree/WAL; injected catalog, serving authority and memory ChunkStream backing",
                "errors": None, "expected_transport_errors": int(args.profile == "disconnect"), "verification_errors": None}
    processes = {}
    try:
        for side in ("server", "client"):
            side_env = dict(env, CROWDB_RDMA_CHUNK_KV_ROLE=side)
            with (output / f"{side}.log").open("w") as log:
                processes[side] = start(runtime.command([binary, "--ignored", "--exact", CASE, "--nocapture"],
                                                        side_env, side), side_env, log, ROOT)
            if side == "server":
                manifest["startup"] = wait_json(output / "ready.json", processes[side])
                if args.topology == "containers":
                    info = json.loads(subprocess.check_output([pixi, "run", "--", "docker", "inspect",
                                                               runtime.names[side]], text=True))[0]
                    manifest["server_container"] = {"image": info["Image"], "user": info["Config"]["User"],
                        "network": info["HostConfig"]["NetworkMode"], "devices": info["HostConfig"]["Devices"]}
                    if info["Config"]["User"].split(":")[0] in ("", "0", "root"):
                        raise RuntimeError("server container must be non-root")
        if args.profile == "disconnect":
            manifest["partial_response_gate"] = wait_json(output / "fault-ready.json", processes["client"])
            manifest["partial_receive_gate"] = wait_json(output / "client-partial-ready.json", processes["client"])
            (output / "stop").write_text("disconnect after one RDMA response fragment\n")
        if processes["client"].wait(timeout=30) != 0:
            raise RuntimeError("Chunk-KV client failed")
        (output / "stop").write_text("corpus complete\n")
        if processes["server"].wait(timeout=10) != 0:
            raise RuntimeError("Chunk-KV server failed")
        result = json.loads((output / "client.json").read_text())
        server = json.loads((output / "server-final.json").read_text())
        check(result, server, args.profile)
        if args.topology == "containers" and result["effective_uid"] == "0":
            raise RuntimeError("client container must be non-root")
        result["server_transport"] = server
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        manifest.update(errors=0, verification_errors=0)
    except Exception as error:
        manifest.update(verification_errors=1, failure=str(error))
        raise
    finally:
        for side, process in processes.items():
            runtime.stop(process, side)
            print((output / f"{side}.log").read_text(), end="", flush=True)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Artifacts: {output}", flush=True)


if __name__ == "__main__":
    main()
