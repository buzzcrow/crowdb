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
from scenarios import ALL_CASES, case_nodes, run_cases
from image_identity import verify_server_sources
from report import write_report

ROOT = Path(__file__).resolve().parents[1]


def capacity(concurrency, cpus, memory_mib, nodes=3):
    fixture_cpus, fixture_memory = (nodes + 1) * 0.5, (nodes + 1) * 512
    if concurrency < 1 or nodes < 1 or cpus < fixture_cpus or memory_mib < fixture_memory:
        raise ValueError(f"KV needs at least one {fixture_cpus:g}-CPU / {fixture_memory}-MiB fixture slot")
    return min(concurrency, int(cpus // fixture_cpus), memory_mib // fixture_memory)


def interrupt(_signal, _frame):
    raise KeyboardInterrupt("container E2E interrupted")


def run_pair(image, artifacts, bundle, count, data_root=None):
    ready = threading.Barrier(count, timeout=180)
    retired, failed = threading.Event(), threading.Event()

    def scenario(index):
        try:
            cluster = Cluster(image, artifacts, data_root=data_root)
            (cluster.artifacts / "case.json").write_text(json.dumps({
                "scope": "kv.recovery" if index == 0 else "kv.isolation",
                "case": "e2e_three_node_cluster_kv_put_batch_delete"}, indent=2))
            with cluster:
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
    parser.add_argument("--layer", choices=("kv", "diskdb", "diskio", "deployment", "all"), default="all")
    parser.add_argument("--scope", choices=("all", "kv.management", "kv.membership", "kv.routing", "kv.recovery", "kv.isolation", "kv.group0", "kv.client", "kv.metadata", "diskdb.allocation", "diskdb.ownership", "diskdb.reuse", "diskdb.recovery", "deployment.selection", "diskio.durability", "diskio.direct", "diskio.io.mem", "diskio.io.null", "diskio.discovery", "diskio.errors", "diskio.faults", "diskio.semantic"),
                        default="all")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--case", help="fully qualified named flow within the selected scope")
    parser.add_argument("--list", action="store_true", help="list implemented named flows without starting servers")
    parser.add_argument("--require-isolation", action="store_true")
    parser.add_argument("--cpus", type=float, default=4)
    parser.add_argument("--memory-mib", type=int, default=4096)
    parser.add_argument("--image", default="crowdb-node:dev")
    parser.add_argument("--data-root", type=Path, help="host parent for independent per-run/node data mounts")
    parser.add_argument("--artifacts", type=Path, default=ROOT / ".crowdb-runtime/artifacts/container-e2e")
    args = parser.parse_args()
    selected = [case for case in ALL_CASES if (args.layer == "all" or case.scope.startswith(args.layer + ".")) and (args.scope == "all" or case.scope == args.scope)
                and (not args.case or case.name == args.case)]
    if not selected and not (args.layer in ("kv", "all") and args.scope in ("kv.recovery", "kv.isolation")):
        parser.error("the selected case is not implemented in this scope")
    if args.case and args.require_isolation:
        parser.error("a single case cannot claim concurrent isolation acceptance")
    if args.require_isolation and (args.layer not in ("kv", "all") or args.scope not in ("all", "kv.recovery", "kv.isolation")):
        parser.error("isolation acceptance requires all, kv.recovery or kv.isolation scope")
    if args.list:
        print(json.dumps([{"scope": case.scope, "case": case.name,
                           "nodes": case_nodes(case)} for case in selected], indent=2))
        return
    signal.signal(signal.SIGTERM, interrupt)
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux":
        parser.error("the first container E2E profile requires a Linux daemon")
    slots = capacity(args.concurrency, min(args.cpus, info["NCPU"]),
                     min(args.memory_mib, info["MemTotal"] // 1048576))
    largest = max((case_nodes(case) for case in selected), default=3)
    case_slots = capacity(args.concurrency, min(args.cpus, info["NCPU"]),
                          min(args.memory_mib, info["MemTotal"] // 1048576), largest)
    if (args.require_isolation or args.scope == "kv.isolation") and slots < 2:
        parser.error("concurrent isolation acceptance needs two fixture slots")
    identity = json.loads(docker("image", "inspect", args.image))[0]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    labels = identity["Config"].get("Labels") or {}
    try:
        source_identity = verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"))
    except ValueError as error:
        parser.error(str(error))
    args.artifacts.mkdir(parents=True, exist_ok=True)
    # Unique artifacts/client bundle avoid concurrent runner collisions.
    run_root = args.artifacts / ("run-" + uuid.uuid4().hex)
    run_root.mkdir()
    (run_root / "identity.json").write_text(json.dumps({"image": args.image, "id": identity["Id"],
        "repo_digests": identity.get("RepoDigests", []), "labels": labels, "client_revision": revision,
        "layer": args.layer, "scope": args.scope, "slots": slots, "case_slots": case_slots,
        "largest_case_nodes": largest, "selected_cases": [case.name for case in selected],
        "source_identity": source_identity}, indent=2))
    print(f"KV isolation slots: {slots}; case slots: {case_slots}; exact image {identity['Id']}; artifacts {run_root}", flush=True)
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
    timings["status"] = "failed"
    try:
        if args.layer in ("kv", "all") and not args.case and args.scope in ("all", "kv.recovery", "kv.isolation"):
            run_pair(identity["Id"], run_root, bundle, min(slots, 2), args.data_root)
        if selected:
            run_cases(identity["Id"], run_root, bundle, case_slots, args.data_root, args.scope, args.case, args.layer)
        if args.layer in ("kv", "deployment", "all") and args.scope == "all" and not args.case:
            check_startup(identity["Id"], run_root)
        timings["status"] = "pass"
    finally:
        timings["scenarios_wall"] = time.monotonic() - started
        write_report(run_root, timings)
    print(f"Container scope {args.scope} passed", flush=True)


if __name__ == "__main__":
    main()
