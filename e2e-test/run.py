# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Run the currently migrated container layers with bounded resource ownership."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path

from client import bundle_cli, bundle_client, bundle_iceberg_rust, bundle_iceberg_java, bundle_iceberg_rck, client_sources
from fixture import Cluster, docker
from release import check_startup
from scenarios import case_nodes, case_resources, run_cases, select_cases
from image_identity import verify_server_sources
from report import write_report
from mixed_isolation import run_mixed

ROOT = Path(__file__).resolve().parents[1]


def capacity(concurrency, cpus, memory_mib, nodes=3, node_cpus=0.5):
    if node_cpus <= 0:
        raise ValueError("node CPU quota must be positive")
    fixture_cpus, fixture_memory = nodes * node_cpus + 0.5, (nodes + 1) * 512
    if concurrency < 1 or nodes < 1 or cpus < fixture_cpus or memory_mib < fixture_memory:
        raise ValueError(f"KV needs at least one {fixture_cpus:g}-CPU / {fixture_memory}-MiB fixture slot")
    return min(concurrency, int(cpus // fixture_cpus), memory_mib // fixture_memory)


def interrupt(_signal, _frame):
    raise KeyboardInterrupt("container E2E interrupted")


def run_pair(image, artifacts, bundle, count, data_root=None, node_cpus=0.5):
    ready = threading.Barrier(count, timeout=180)
    retired, failed = threading.Event(), threading.Event()

    def scenario(index):
        try:
            cluster = Cluster(image, artifacts, data_root=data_root)
            cluster.node_cpus = node_cpus
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
    parser.add_argument("--layer", choices=("kv", "diskdb", "diskio", "chunkdb", "chunk-stream", "chunk-client", "chunk-kv", "s3", "iceberg", "dataset", "console", "deployment", "all"), default="all")
    parser.add_argument("--scope", choices=("all", "dataset.http", "console.managed", "console.native", "console.native.binding", "console.native.s3", "console.native.recovery", "console.native.load", "console.native.browser.s3", "console.browser", "console.browser.shell", "console.browser.registry", "console.browser.kv", "console.browser.canvas", "console.browser.inspector", "console.browser.logical", "console.browser.node", "console.discovery", "console.hardware", "console.logical", "console.bootstrap", "console.cleanup", "console.deployment", "console.cli", "console.cli.container", "console.operations", "console.web", "console.web.disposable", "console.web.recovery", "console.web.access", "console.web.authority", "console.web.slots", "console.web.catalog", "console.web.bootstrap", "console.web.standalone", "console.web.registry", "console.web.lifecycle", "console.web.wal", "console.clients", "iceberg.manual.tpc", "iceberg.benchmark", "iceberg.metadata", "iceberg.namespace", "iceberg.files", "iceberg.files.listing", "iceberg.catalog", "iceberg.gc", "iceberg.sdk", "iceberg.sdk.java", "iceberg.sdk.rust", "iceberg.sdk.rck", "s3.protocol", "s3.recovery", "s3.responses", "s3.benchmark", "s3.sdk", "s3.credentials", "s3.policy", "s3.nodes", "kv.management", "kv.membership", "kv.routing", "kv.recovery", "kv.isolation", "kv.group0", "kv.client", "kv.metadata", "diskdb.allocation", "diskdb.ownership", "diskdb.reuse", "diskdb.recovery", "chunkdb.allocation", "chunkdb.lifecycle", "chunkdb.recovery", "chunkdb.placement", "chunkdb.io", "chunkdb.conversion", "chunkdb.repair", "chunkdb.slots", "chunkdb.handoff", "chunk-kv.protocol", "chunk-kv.recovery", "chunk-client.reader", "chunk-client.large_repair", "chunk-client.small_flow", "chunk-client.benchmark", "chunk-client.large", "chunk-client.small", "chunk-stream.objects", "chunk-stream.recovery", "chunk-stream.errors", "deployment.selection", "deployment.supervision", "deployment.hardware", "deployment.bootstrap", "deployment.balance", "deployment.listeners", "deployment.browser", "deployment.anonymous", "deployment.failures", "deployment.clients", "deployment.storage", "deployment.validation", "deployment.kv", "diskio.durability", "diskio.direct", "diskio.io.mem", "diskio.io.null", "diskio.discovery", "diskio.errors", "diskio.faults", "diskio.semantic"),
                        default="all")
    parser.add_argument("--profile", choices=("bridge", "bridge-admission", "host", "containerd-host", "containerd-cluster", "ecosystem", "ecosystem-python"), default="bridge")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--case", help="fully qualified named flow within the selected scope")
    parser.add_argument("--list", action="store_true", help="list implemented named flows without starting servers")
    parser.add_argument("--require-isolation", action="store_true")
    parser.add_argument("--cpus", type=float, default=4)
    parser.add_argument("--node-cpus", type=float, default=os.environ.get("CROWDB_E2E_NODE_CPUS", "0.5"), help="Docker CPU quota per server; host client reserves 0.5 CPU")
    parser.add_argument("--memory-mib", type=int, default=4096)
    parser.add_argument("--image", default="crowdb-node:dev")
    parser.add_argument("--data-root", type=Path, help="host parent for independent per-run/node data mounts")
    parser.add_argument("--artifacts", type=Path, default=ROOT / ".crowdb-runtime/artifacts/container-e2e")
    parser.add_argument("--archive", type=Path, default=ROOT / "target/crowdb.oci.tar",
                        help="verified OCI archive for the explicit containerd profile")
    args = parser.parse_args()
    if args.profile in ("ecosystem", "ecosystem-python"):
        signal.signal(signal.SIGTERM, interrupt)
        from ecosystem_profile import run_profile
        run_profile(args, parser)
        return
    if args.profile == "bridge-admission":
        signal.signal(signal.SIGTERM, interrupt)
        from admission_profile import run_profile
        run_profile(args, parser)
        return
    if args.profile == "host":
        signal.signal(signal.SIGTERM, interrupt)
        from special_profiles import run_host
        run_host(args, parser)
        return
    if args.profile in ("containerd-host", "containerd-cluster"):
        signal.signal(signal.SIGTERM, interrupt)
        from containerd_profile import run_profile
        run_profile(args, parser)
        return
    selected = select_cases(args.layer, args.scope, args.case)
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
    if args.scope == "iceberg.manual.tpc":
        manifest = ROOT / "e2e-test/clients/tpc-loader/pixi.toml"
        python = manifest.parent / ".pixi/envs/default/bin/python"
        if not python.is_file():
            parser.error("manual TPC requires the locked client environment; run pixi install --manifest-path " + str(manifest))
        os.environ["CROWDB_E2E_TPC_MANIFEST"] = str(manifest)
    signal.signal(signal.SIGTERM, interrupt)
    info = json.loads(docker("info", "--format", "{{json .}}"))
    if info["OSType"] != "linux":
        parser.error("the first container E2E profile requires a Linux daemon")
    slots = capacity(args.concurrency, min(args.cpus, info["NCPU"]),
                     min(args.memory_mib, info["MemTotal"] // 1048576), node_cpus=args.node_cpus)
    largest = max((case_nodes(case) for case in selected), default=3)
    resources = [case_resources(case, args.node_cpus) for case in selected] or [(2, 2048)]
    needed_cpu, needed_memory = max(cost[0] for cost in resources), max(cost[1] for cost in resources)
    available_cpu = min(args.cpus, info["NCPU"])
    available_memory = min(args.memory_mib, info["MemTotal"] // 1048576)
    if available_cpu < needed_cpu or available_memory < needed_memory:
        parser.error(f"selected cases require at least {needed_cpu:g} CPU / {needed_memory} MiB")
    case_slots = min(args.concurrency, int(available_cpu // needed_cpu), available_memory // needed_memory)
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
        "largest_case_nodes": largest, "case_cpu_reservation": needed_cpu,
        "case_memory_reservation_mib": needed_memory, "selected_cases": [case.name for case in selected],
        "node_cpus": args.node_cpus,
        "source_identity": source_identity}, indent=2))
    print(f"KV isolation slots: {slots}; case slots: {case_slots}; exact image {identity['Id']}; artifacts {run_root}", flush=True)
    started = time.monotonic()
    bundle = bundle_client(run_root / "client")
    timings = {"client_build": time.monotonic() - started}
    if any(case.name.startswith("access_iceberg::rust_") for case in selected):
        started = time.monotonic()
        bundle_iceberg_rust(bundle)
        timings["iceberg_rust_build"] = time.monotonic() - started
    if any("::java_sdk::" in case.name for case in selected):
        started = time.monotonic()
        bundle_iceberg_java(bundle)
        timings["iceberg_java_build"] = time.monotonic() - started
    if any(case.scope == "iceberg.sdk.rck" for case in selected):
        started = time.monotonic()
        bundle_iceberg_rck(bundle)
        timings["iceberg_rck_source"] = time.monotonic() - started
    if any(case.scope.startswith("console.cli") for case in selected):
        started = time.monotonic()
        bundle_cli(bundle)
        timings["cli_build"] = time.monotonic() - started
    with (bundle / "domain-test").open("rb") as executable:
        client_digest = hashlib.file_digest(executable, "sha256").hexdigest()
    client_source = hashlib.sha256()
    for path in client_sources(ROOT / "e2e-test"):
        if path.suffix in (".rs", ".py", ".toml"):
            client_source.update(str(path.relative_to(ROOT)).encode())
            client_source.update(path.read_bytes())
    for path in client_sources(bundle / "clients"):
        client_source.update(str(path.relative_to(bundle)).encode())
        client_source.update(path.read_bytes())
    executable_digests = {}
    for name in ("domain-test", "crowdb-cli", "iceberg-rust-client"):
        path = bundle / name
        if path.is_file():
            with path.open("rb") as executable:
                executable_digests[name] = hashlib.file_digest(executable, "sha256").hexdigest()
    (run_root / "client-identity.json").write_text(json.dumps({"sha256": client_digest,
        "revision": revision, "source_sha256": client_source.hexdigest(), "executables": executable_digests}, indent=2))
    started = time.monotonic()
    timings["status"] = "failed"
    try:
        if args.layer in ("kv", "all") and not args.case and args.scope in ("all", "kv.recovery", "kv.isolation"):
            run_pair(identity["Id"], run_root, bundle, min(slots, 2), args.data_root, args.node_cpus)
            if slots >= 2:
                run_mixed(identity["Id"], run_root, bundle, args.data_root, args.node_cpus)
        if selected:
            run_cases(identity["Id"], run_root, bundle, case_slots, args.data_root, args.scope, args.case, args.layer, args.node_cpus)
        if args.layer in ("kv", "deployment", "all") and args.scope == "all" and not args.case:
            check_startup(identity["Id"], run_root)
        timings["status"] = "pass"
    finally:
        timings["scenarios_wall"] = time.monotonic() - started
        write_report(run_root, timings)
    print(f"Container scope {args.scope} passed", flush=True)


if __name__ == "__main__":
    main()
