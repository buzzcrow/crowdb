#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native Dataset against owned real services and one local HCA; no remote nodes."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "e2e-test"))
sys.path.insert(0, str(ROOT / "tools/sdk"))
from client import bundle_client
from fixture import docker
from image_identity import verify_server_sources
from native_package import package
from native_s3_hca import NativeS3HcaCluster


def interrupted(_signal, _frame):
    raise KeyboardInterrupt("Dataset local HCA acceptance interrupted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="10.200.1.1")
    parser.add_argument("--device", default="rocep35s0f0")
    parser.add_argument("--image", default="crowdb-node:dev")
    args = parser.parse_args()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, interrupted)
    output = ROOT / ".crowdb-runtime/artifacts/native-dataset-hca" / uuid.uuid4().hex
    output.mkdir(parents=True)
    print(f"Artifacts: {output}", flush=True)
    image = json.loads(docker("image", "inspect", args.image))[0]
    labels = image["Config"].get("Labels") or {}
    verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"),
                          labels.get("org.crowdb.source.tree") or None)
    os.environ["CROWDB_RDMA_ENABLED"] = "0"
    python = package(output / "python")
    bundle = bundle_client(output / "client")
    with NativeS3HcaCluster(image["Id"], output, bundle, address=args.address, device=args.device) as cluster:
        environment = dict(os.environ, CROWDB_MANAGEMENT_SEEDS=",".join(cluster.origins[:3]),
                           CROWDB_RDMA_SOURCE_ADDR=args.address, CROWDB_RDMA_DEVICE=args.device,
                           CROWDB_DATASET_CONSUMER_ROOT=str(output), NO_PROXY="*", no_proxy="*")
        for phase in ("write", "verify"):
            result = subprocess.run([str(python), str(ROOT / "lib/crowdb-sdk-native-python/tests/hca_consumer.py")],
                env=dict(environment, CROWDB_DATASET_PHASE=phase), text=True, capture_output=True, timeout=600)
            log = result.stdout + result.stderr
            output.joinpath(f"{phase}.log").write_text(log)
            print(log, end="", flush=True)
            result.check_returncode()
            if phase == "write":
                cluster.crash_restart()
    print("Native Dataset local HCA acceptance passed", flush=True)


if __name__ == "__main__":
    main()
