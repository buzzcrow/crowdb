#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""HTTP/ChunkIO to a separate O_DIRECT DiskIO process, native or two OCI containers."""

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


def build(pixi, output):
    subprocess.run([pixi, "run", "--", "cmake", "--build", "app/crowdb-diskio/build",
                    "-j32", "--target", "crowdb-diskio"], cwd=ROOT, check=True)
    result = subprocess.run([pixi, "run", "--", "cargo", "test", "-p", "crowdb-access-server",
                             "--test", "rdma_datapath_test", "--no-run", "--message-format=json"],
                            cwd=ROOT, capture_output=True, text=True)
    (output / "build.jsonl").write_text(result.stdout)
    print(result.stderr, end="", flush=True)
    executable = None
    for line in result.stdout.splitlines():
        item = json.loads(line)
        if item.get("reason") == "compiler-message":
            print(item["message"].get("rendered", ""), end="", flush=True)
        if item.get("target", {}).get("name") == "rdma_datapath_test" and item.get("executable"):
            executable = Path(item["executable"])
    result.check_returncode()
    if executable is None:
        raise RuntimeError("Cargo did not identify the datapath test executable")
    return executable


def status(log, marker):
    for line in log.read_text().splitlines():
        if line.startswith(marker):
            return json.loads(line.removeprefix(marker))
    return None


def ready(server, log, rdma):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError(f"DiskIO exited before readiness: {server.returncode}")
        observed = status(log, "crowdb-diskio transport ready: ")
        if observed is not None:
            if observed["allow_tcp_fallback"]:
                raise RuntimeError("datapath fixture must disable TCP fallback")
            if rdma and not (observed["rdma"]["ready"] and observed["rdma"]["listen_active"]):
                raise RuntimeError("DiskIO did not publish an active RDMA listener")
            if not rdma and observed["rdma"] is not None:
                raise RuntimeError("TCP-only DiskIO unexpectedly enabled RDMA")
            return observed
        time.sleep(0.02)
    raise RuntimeError("DiskIO readiness deadline expired")


def checked_result(output, transport, final, fault):
    result = json.loads((output / "client.json").read_text())
    if result["errors"] != int(fault) or result["verification_errors"] != 0 or result["payload_bytes"] <= 3 << 20:
        raise RuntimeError("datapath corpus or error count is invalid")
    if transport == "rdma":
        rdma = final["rdma"]
        for field in ("copied_payload_bytes", "fallback_count", "cq_errors", "protocol_errors",
                      "send_errors", "receive_post_errors"):
            if rdma[field] != 0:
                raise RuntimeError(f"DiskIO {field}={rdma[field]}")
        if rdma["direct_payload_bytes"] < (1 if fault else result["payload_bytes"]):
            raise RuntimeError("DiskIO download did not use direct RDMA payloads")
    elif final["rdma"] is not None:
        raise RuntimeError("TCP-only DiskIO attempted RDMA")
    result["server_transport"] = final
    result["disk_engine"] = next((line.removeprefix("crowdb-diskio engine: ")
                                  for line in (output / "server.log").read_text().splitlines()
                                  if line.startswith("crowdb-diskio engine: ")), None)
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("tcp", "rdma"), required=True)
    parser.add_argument("--topology", choices=("native", "containers"), default="native")
    parser.add_argument("--address", default=os.environ.get("CROWDB_RDMA_SOURCE_ADDR", "10.200.1.1"))
    parser.add_argument("--device", default=os.environ.get("CROWDB_RDMA_DEVICE", "rocep35s0f0"))
    parser.add_argument("--image", default="crowdb-node:dev")
    parser.add_argument("--fault-disconnect", action="store_true")
    args = parser.parse_args()
    pixi = shutil.which("pixi")
    if pixi is None:
        raise RuntimeError("pixi is required")
    root = Path(os.environ.get("CROWDB_RUNTIME_ROOT", ROOT / ".crowdb-runtime"))
    output = (root / "artifacts/rdma-datapath" / uuid.uuid4().hex).resolve()
    output.mkdir(parents=True)
    client_binary = build(pixi, output)
    server_binary = ROOT / "app/crowdb-diskio/build/crowdb-diskio"
    if args.topology == "containers":
        args.image = subprocess.check_output([pixi, "run", "--", "docker", "image", "inspect",
                                              args.image, "--format", "{{.Id}}"], text=True).strip()
    rdma = args.transport == "rdma"
    address = args.address if rdma else "127.0.0.1"
    with socket.socket() as reservation:
        reservation.bind((address, 0))
        port = reservation.getsockname()[1]
    config = ["[server]", f'bind_address = "{address}"', f"listen_port = {port}", "rpc_workers = 1",
              "o_direct = true", "[engine]", "sq_entries = 32", "[metrics]", "interval_secs = 0"]
    for high in (1, 2):
        disk = output / f"disk-{high}.data"
        with disk.open("wb") as stream:
            stream.truncate(64 << 20)
        config += ["[[disk]]", f'id = "{high}:0"', f'path = "{disk}"', f"zone_capacity = {64 << 20}"]
    config_path = output / "diskio.toml"
    config_path.write_text("\n".join(config) + "\n")
    env = {key: value for key, value in os.environ.items() if not key.startswith("CROWDB_RDMA_")}
    env.update(CROWDB_RUNTIME_ROOT=str(output), CROWDB_RDMA_ENABLED=str(int(rdma)),
               CROWDB_RDMA_ALLOW_TCP_FALLBACK="0", CROWDB_RDMA_DEVICE=args.device,
               CROWDB_RDMA_SOURCE_ADDR=address, CROWDB_RDMA_CQ_COUNT="1", CROWDB_RDMA_CQ_DEPTH="16",
               CROWDB_RDMA_WIRE_LAYOUT="1", CROWDB_RDMA_PORT="1",
               CROWDB_RDMA_SQ_DEPTH="4", CROWDB_RDMA_RQ_DEPTH="4", CROWDB_RDMA_POSTED_RECEIVE_TARGET="4",
               CROWDB_RDMA_SEND_ARENA_BYTES=str(4 << 20), CROWDB_RDMA_RECEIVE_ARENA_BYTES=str(4 * ((1 << 20) + 1024)),
               CROWDB_RDMA_AGGREGATION_ENABLED="0", CROWDB_RDMA_DATAPATH_ENDPOINT=f"{address}:{port}",
               CROWDB_RDMA_DATAPATH_TRANSPORT=args.transport, CROWDB_RDMA_DATAPATH_RESULT=str(output / "client.json"),
               CROWDB_RDMA_DATAPATH_FAULT=str(int(args.fault_disconnect)),
               CROWDB_RDMA_DATAPATH_PAUSED=str(output / "download-paused"),
               CROWDB_RDMA_DATAPATH_RESUME=str(output / "download-resume"),
               CROWDB_RDMA_DATAPATH_TOPOLOGY=args.topology + ("_hca_local_loop" if rdma else "_tcp_loopback"))
    topology = args.topology if rdma or args.topology == "native" else "containers-tcp"
    runtime = Runtime(ROOT, output, pixi, topology, args.image)
    manifest = {"transport": args.transport, "topology": env["CROWDB_RDMA_DATAPATH_TOPOLOGY"],
                "image": args.image if args.topology == "containers" else None,
                "source": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
                "binaries": {str(binary): hashlib.sha256(binary.read_bytes()).hexdigest()
                             for binary in (server_binary, client_binary)}, "errors": None,
                "expected_errors": int(args.fault_disconnect), "verification_errors": None}
    server = client = None
    client_log_printed = False
    server_log, client_log = output / "server.log", output / "client.log"
    try:
        with server_log.open("w") as stream:
            server = start(runtime.command([server_binary, "--config", config_path], env, "server"), env, stream, ROOT)
        manifest["server_startup"] = ready(server, server_log, rdma)
        if args.topology == "containers":
            info = json.loads(subprocess.check_output([pixi, "run", "--", "docker", "inspect",
                                                       runtime.names["server"]], text=True))[0]
            manifest["server_container"] = {
                "image": info["Image"], "user": info["Config"]["User"],
                "network": info["HostConfig"]["NetworkMode"], "devices": info["HostConfig"]["Devices"],
            }
            if info["Config"]["User"].split(":")[0] in ("", "0", "root"):
                raise RuntimeError("DiskIO container must run as non-root")
        with client_log.open("w") as stream:
            client = start(runtime.command([client_binary, "--ignored", "--exact",
                           "http_chunkio_round_trip_uses_selected_transport_and_exact_framed_bytes", "--nocapture"],
                           env, "client"), env, stream, ROOT)
        if args.fault_disconnect:
            deadline = time.monotonic() + 10
            while not (output / "download-paused").exists():
                if client.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("download did not reach its verified partial-result gate")
                time.sleep(0.01)
            runtime.stop(server, "server")
            (output / "download-resume").write_text("peer stopped\n")
        code = client.wait(timeout=45)
        print(client_log.read_text(), end="", flush=True)
        client_log_printed = True
        if code != 0:
            raise RuntimeError(f"datapath client failed: {code}")
        if args.topology == "containers" and json.loads((output / "client.json").read_text())["effective_uid"] == 0:
            raise RuntimeError("HTTP/ChunkIO container must run as non-root")
        runtime.stop(server, "server")
        final = status(server_log, "crowdb-diskio transport final: ")
        if final is None:
            raise RuntimeError("DiskIO did not retain its final transport counters")
        checked_result(output, args.transport, final, args.fault_disconnect)
        manifest["errors"] = int(args.fault_disconnect)
        manifest["verification_errors"] = 0
    except Exception as error:
        manifest["verification_errors"] = 1
        manifest["failure"] = str(error)
        raise
    finally:
        for process, side in ((client, "client"), (server, "server")):
            if process is not None:
                runtime.stop(process, side)
        if not client_log_printed and client_log.exists():
            print(client_log.read_text(), end="", flush=True)
        if server_log.exists():
            print(server_log.read_text(), end="", flush=True)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Artifacts: {output}", flush=True)


if __name__ == "__main__":
    main()
