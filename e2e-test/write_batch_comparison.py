# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Bounded point/batch writes over an owned production Chunk-KV fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import uuid

from chunk_kv_fixture import ChunkKvCluster
from client import bundle_client
from fixture import docker
from image_identity import verify_server_sources

ROOT = Path(__file__).resolve().parents[1]
TARGET = "dataset_routed_write_batch_comparison_test"
CASE = "compare_bounded_immutable_point_and_routed_batch_writes"


def interrupted(_signal, _frame):
    raise KeyboardInterrupt("Write comparison interrupted")


def build_client(output):
    command = ["cargo", "test", "--release", "--locked", "-p", "crowdb-sdk-native",
               "--test", TARGET, "--no-run", "--message-format=json"]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=600)
    (output / "build.log").write_text(result.stdout + result.stderr)
    print(result.stderr, end="", flush=True)
    if result.returncode:
        print(result.stdout, end="", flush=True)
        result.check_returncode()
    binaries = [item["executable"] for line in result.stdout.splitlines() if line.startswith("{")
                and (item := json.loads(line)).get("target", {}).get("name") == TARGET
                and item.get("executable")]
    if len(binaries) != 1:
        raise RuntimeError("missing or ambiguous release comparison executable")
    return binaries[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="crowdb-node:dev")
    parser.add_argument("--client-cpu", default="2")
    parser.add_argument("--server-cpus", default="4-15")
    parser.add_argument("--metadata-memory", default="2g")
    args = parser.parse_args()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, interrupted)
    output = ROOT / ".crowdb-runtime/artifacts/write-batch-comparison" / uuid.uuid4().hex
    output.mkdir(parents=True)
    print(f"Artifacts: {output}", flush=True)
    image = json.loads(docker("image", "inspect", args.image))[0]
    labels = image["Config"].get("Labels") or {}
    verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"),
                          labels.get("org.crowdb.source.tree") or None)
    binary = build_client(output)
    bundle = bundle_client(output / "client")
    os.environ["CROWDB_RDMA_ENABLED"] = "0"

    def pinned_command(*parts, **kwargs):
        if parts and parts[0] == "run":
            parts = list(parts)
            name = parts[parts.index("--name") + 1]
            if name.endswith(("-node0", "-node1", "-node2")):
                parts[parts.index("--memory") + 1] = args.metadata_memory
            parts = (parts[0], "--cpuset-cpus", args.server_cpus, *parts[1:])
        return docker(*parts, **kwargs)

    sources = [Path(__file__).resolve(),
               ROOT / f"lib/crowdb-sdk-native/tests/{TARGET}.rs",
               ROOT / "lib/crowdb-sdk-native/tests/common/routed_write_batch.rs",
               *(ROOT / "lib/crowdb-sdk-native/tests/common/routed_write_batch").glob("*.rs")]
    manifest = {"image": image["Id"], "labels": labels,
                "client_binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
                "client_source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                         for path in sources},
                "source_revision": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "client_cpu": args.client_cpu, "server_cpus": args.server_cpus,
                "metadata_memory": args.metadata_memory,
                "cpu_topology": subprocess.check_output(["lscpu", "-e=CPU,CORE,SOCKET,NODE"], text=True),
                "scope": "same-host TCP, real routed typed writes; no Dataset publication or production TPS claim",
                "status": "failed"}
    try:
        with ChunkKvCluster(image["Id"], output, bundle, command=pinned_command) as cluster:
            environment = dict(os.environ, CROWDB_MANAGEMENT_SEEDS=",".join(cluster.origins[:3]),
                               CROWDB_WRITE_BATCH_OUTPUT=str(output / "comparison.json"),
                               NO_PROXY="*", no_proxy="*")
            manifest.update(seeds=cluster.origins[:3], endpoints=cluster.addresses,
                            fixture=str(cluster.artifacts))
            command = ["taskset", "-c", args.client_cpu, binary, CASE, "--ignored", "--exact",
                       "--nocapture", "--test-threads=1"]
            manifest["command"] = command
            log = output / "comparison.log"
            try:
                with log.open("w") as stream:
                    result = subprocess.run(command, env=environment, cwd=ROOT, text=True,
                                            stdout=stream, stderr=subprocess.STDOUT, timeout=600)
            finally:
                print(log.read_text(), end="", flush=True)
            result.check_returncode()
            report = json.loads((output / "comparison.json").read_text())
            if not report["valid"] or "test result: ok. 1 passed; 0 failed;" not in log.read_text():
                raise RuntimeError("comparison did not complete exactly one valid passing case")
            manifest["status"] = "passed"
    finally:
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Bounded point/batch comparison complete", flush=True)


if __name__ == "__main__":
    main()
