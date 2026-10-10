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
import time
import uuid
from pathlib import Path

from client import bundle_client
from fixture import Cluster, docker
from release import check_startup

ROOT = Path(__file__).resolve().parents[1]


def capacity(concurrency, cpus, memory_mib):
    if concurrency < 1 or cpus < 2 or memory_mib < 2048:
        raise ValueError("KV needs at least one 2-CPU / 2048-MiB fixture slot")
    return min(concurrency, int(cpus // 2), memory_mib // 2048)


def interrupt(_signal, _frame):
    raise KeyboardInterrupt("container E2E interrupted")


def run_pair(image, artifacts, bundle, count, data_root=None):
    ready = threading.Barrier(count, timeout=180)
    retired, failed = threading.Event(), threading.Event()

    def scenario(index):
        try:
            with Cluster(image, artifacts, data_root=data_root) as cluster:
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
    # budget is reserved for each node plus its host client, even while idle.
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
    parser.add_argument("--data-root", type=Path, help="host parent for independent per-run/node data mounts")
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
    started = time.monotonic()
    bundle = bundle_client(run_root / "client")
    timings = {"client_build": time.monotonic() - started}
    with (bundle / "kv-test").open("rb") as executable:
        client_digest = hashlib.file_digest(executable, "sha256").hexdigest()
    client_source = hashlib.sha256()
    for path in sorted((ROOT / "e2e-test").rglob("*")):
        if path.is_file() and path.suffix in (".rs", ".py", ".toml"):
            client_source.update(str(path.relative_to(ROOT)).encode())
            client_source.update(path.read_bytes())
    (run_root / "client-identity.json").write_text(json.dumps({"sha256": client_digest,
        "revision": revision, "source_sha256": client_source.hexdigest()}, indent=2))
    started = time.monotonic()
    run_pair(identity["Id"], run_root, bundle, min(slots, 2), args.data_root)
    with Cluster(identity["Id"], run_root, data_root=args.data_root, node_count=1) as cluster:
        cluster.client(bundle, "management", "kv::management::management_lifecycle")
    check_startup(identity["Id"], run_root)
    timings["scenarios_wall"] = time.monotonic() - started
    (run_root / "timing.json").write_text(json.dumps(timings, indent=2))
    print("Container KV CRUD, crash recovery and available fixture isolation checks passed", flush=True)


if __name__ == "__main__":
    main()
