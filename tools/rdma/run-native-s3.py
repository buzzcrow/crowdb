#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native and OCI S3 consumers against twelve real services using a shared HCA."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "e2e-test"))
from client import bundle_client
from fixture import docker
from image_identity import verify_server_sources
from native_s3_hca import NativeS3HcaCluster


def interrupted(_signal, _frame):
    raise KeyboardInterrupt("shared HCA S3 acceptance interrupted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default=os.environ.get("CROWDB_RDMA_SOURCE_ADDR", "10.200.1.1"))
    parser.add_argument("--device", default=os.environ.get("CROWDB_RDMA_DEVICE", "rocep35s0f0"))
    parser.add_argument("--image", default="crowdb-node:dev")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    output = ROOT / ".crowdb-runtime/artifacts/native-s3-hca" / uuid.uuid4().hex
    output.mkdir(parents=True)
    print(f"Artifacts: {output}", flush=True)
    image = json.loads(docker("image", "inspect", args.image))[0]
    labels = image["Config"].get("Labels") or {}
    verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"),
                          labels.get("org.crowdb.source.tree") or None)
    os.environ.update(CROWDB_NATIVE_S3_RDMA="1", CROWDB_RDMA_SOURCE_ADDR=args.address,
                      CROWDB_RDMA_DEVICE=args.device, CROWDB_RDMA_ENABLED="0")
    bundle = output / "client"
    bundle_client(bundle)
    cluster = NativeS3HcaCluster(image["Id"], output, bundle, address=args.address, device=args.device)
    case = "access_s3::native::complete_corpus_survives_storage_restart_without_access_or_credentials"
    with cluster:
        cluster.client(bundle, "write", case)
        cluster.crash_restart()
        cluster.client(bundle, "verify", case)
    print("Native and OCI shared-HCA S3 acceptance passed", flush=True)


if __name__ == "__main__":
    main()
