# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Run the currently migrated container layers with bounded resource ownership."""

import argparse
import concurrent.futures
import hashlib
import json
import signal
import subprocess
import threading
import uuid
from pathlib import Path

from client import bundle_client
from fixture import Cluster, docker

ROOT = Path(__file__).resolve().parents[2]


def capacity(concurrency, cpus, memory_mib):
    if concurrency < 1 or cpus < 2 or memory_mib < 2048:
        raise ValueError("KV needs at least one 2-CPU / 2048-MiB fixture slot")
    return min(concurrency, int(cpus // 2), memory_mib // 2048)


def interrupt(_signal, _frame):
    raise KeyboardInterrupt("container E2E interrupted")


def run_pair(image, artifacts, bundle, count):
    ready = threading.Barrier(count, timeout=180)
    retired, failed = threading.Event(), threading.Event()

    def scenario(index):
        try:
            with Cluster(image, artifacts) as cluster:
                cluster.client(bundle, "write")
                ready.wait()
                if index == 0:
                    cluster.crash_restart()
                    cluster.client(bundle, "verify")
                    cluster.diagnostics()
                    cluster.close()
                    retired.set()
                else:
                    if not retired.wait(180) or failed.is_set():
                        raise AssertionError("peer fixture failed before survivor isolation check")
                    cluster.ready()
                    cluster.client(bundle, "verify")
        except BaseException:
            failed.set()
            retired.set()
            ready.abort()
            raise

    # At most two fixtures constitute the initial isolation acceptance. The
    # budget is reserved for each node plus its client sidecar, even while idle.
    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(scenario, index) for index in range(count)]
        try:
            for future in futures:
                future.result()
        except BaseException:
            failed.set()
            retired.set()
            ready.abort()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=("kv", "all"), default="kv")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--require-isolation", action="store_true")
    parser.add_argument("--cpus", type=float, default=4)
    parser.add_argument("--memory-mib", type=int, default=4096)
    parser.add_argument("--image", default="crowdb-node:dev")
    parser.add_argument("--artifacts", type=Path, default=ROOT / ".crowdb-runtime/artifacts/container-e2e")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, interrupt)
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux":
        parser.error("the first container E2E profile requires a Linux daemon")
    slots = capacity(args.concurrency, min(args.cpus, info["NCPU"]),
                     min(args.memory_mib, info["MemTotal"] // 1048576))
    if args.require_isolation and slots < 2:
        parser.error("concurrent isolation acceptance needs two fixture slots")
    identity = json.loads(docker("image", "inspect", args.image))[0]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    labels = identity["Config"].get("Labels") or {}
    if labels.get("org.opencontainers.image.revision") != revision:
        parser.error("image/source revision differs; rebuild the shared OCI image")
    args.artifacts.mkdir(parents=True, exist_ok=True)
    # Unique artifacts/client bundle avoid concurrent runner collisions.
    run_root = args.artifacts / ("run-" + uuid.uuid4().hex)
    run_root.mkdir()
    (run_root / "identity.json").write_text(json.dumps({"image": args.image, "id": identity["Id"],
        "repo_digests": identity.get("RepoDigests", []), "labels": labels, "client_revision": revision,
        "layer": args.layer, "slots": slots}, indent=2))
    print(f"KV fixture slots: {slots}; exact image {identity['Id']}; artifacts {run_root}", flush=True)
    bundle = bundle_client(run_root / "client")
    with (bundle / "kv-test").open("rb") as executable:
        client_digest = hashlib.file_digest(executable, "sha256").hexdigest()
    source_diff = subprocess.check_output(["git", "diff", "HEAD", "--", "container/crowdb-e2e"])
    (run_root / "client-identity.json").write_text(json.dumps({"sha256": client_digest,
        "revision": revision, "tracked_diff_sha256": hashlib.sha256(source_diff).hexdigest()}, indent=2))
    run_pair(identity["Id"], run_root, bundle, min(slots, 2))
    print("Container KV CRUD, crash recovery and available fixture isolation checks passed", flush=True)


if __name__ == "__main__":
    main()
