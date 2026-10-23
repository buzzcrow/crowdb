# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Pinned Dataset workload over an owned real-service fixture."""

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
from access_dataset_fixture import AccessDatasetCluster
from client import bundle_client
from fixture import docker
from image_identity import verify_server_sources
from native_package import package


def interrupted(_signal, _frame):
    raise KeyboardInterrupt("Dataset workload interrupted")


def run(command, **kwargs):
    print("Workload:", " ".join(map(str, command)), flush=True)
    subprocess.run(command, check=True, timeout=600, **kwargs)


def server_usage(containers):
    """Read owned cgroup v2 counters; retain CPU and peak memory separately."""
    result = {}
    for container in containers:
        inspected = json.loads(docker("inspect", container))[0]
        state = inspected["State"]
        pid = state["Pid"]
        if not state["Running"]:
            result[container] = {"state": state}
            continue
        group = Path(f"/proc/{pid}/cgroup").read_text().strip().split("::", 1)[1]
        root = Path("/sys/fs/cgroup") / group.lstrip("/")
        result[container] = {"cpu": (root / "cpu.stat").read_text(),
                             "memory_peak": (root / "memory.peak").read_text().strip()}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="crowdb-node:dev")
    parser.add_argument("--client-cpu", default="2")
    parser.add_argument("--server-cpus", default="4-15")
    parser.add_argument("--metadata-memory", default="2g")
    args = parser.parse_args()
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, interrupted)
    output = ROOT / ".crowdb-runtime/artifacts/dataset-workload" / uuid.uuid4().hex
    output.mkdir(parents=True)
    print(f"Artifacts: {output}", flush=True)
    image = json.loads(docker("image", "inspect", args.image))[0]
    labels = image["Config"].get("Labels") or {}
    verify_server_sources(ROOT, labels.get("org.opencontainers.image.revision"),
                          labels.get("org.crowdb.source.tree") or None)
    run(["cargo", "build", "--release", "--locked", "--manifest-path", str(ROOT / "sdk/Cargo.toml"),
         "-p", "crowdb-sdk-dataset", "--example", "workload"])
    run(["cargo", "build", "--release", "--locked", "-p", "crowdb-sdk-native", "--example", "dataset_workload"])
    native_python = package(output / "native-python")
    os.environ["CROWDB_RDMA_ENABLED"] = "0"
    bundle = bundle_client(output / "client")

    def pinned_command(*parts, **kwargs):
        if parts and parts[0] == "run":
            parts = list(parts)
            name = parts[parts.index("--name") + 1]
            if name.endswith(("-node0", "-node1", "-node2")):
                parts[parts.index("--memory") + 1] = args.metadata_memory
            parts = (parts[0], "--cpuset-cpus", args.server_cpus, *parts[1:])
        return docker(*parts, **kwargs)

    with AccessDatasetCluster(image["Id"], output, bundle, command=pinned_command) as cluster:
        environment = dict(os.environ, CROWDB_DATASET_ENDPOINT=f"http://{cluster.addresses[12]}:7503",
                           CROWDB_DATASET_NAME="workload", CROWDB_MANAGEMENT_SEEDS=",".join(cluster.origins[:3]), CROWDB_DATASET_READ_TOKEN="token",
                           CROWDB_DATASET_CONSUMER_ROOT=str(output), NO_PROXY="*", no_proxy="*")
        manifest = {"image": image["Id"], "labels": labels, "client_cpu": args.client_cpu,
                    "metadata_memory": args.metadata_memory,
                    "server_cpus": args.server_cpus, "clock_ticks": os.sysconf("SC_CLK_TCK"),
                    "cpu_topology": subprocess.check_output(["lscpu", "-e=CPU,CORE,SOCKET,NODE"], text=True),
                    "seeds": cluster.origins[:3], "endpoints": cluster.addresses,
                    "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                    "scope": "same-host pinned CPU functional workload, not isolated production hardware",
                    "before": server_usage(cluster.servers)}
        tests = ROOT / "lib/crowdb-sdk-native-python/tests"
        commands = [
            ("http", [ROOT / "sdk/target/release/examples/workload"]),
            ("native-rust", [ROOT / "target/release/examples/dataset_workload"]),
            ("native-python", [native_python, tests / "workload.py"]),
            ("native-parity", [native_python, tests / "real_consumer.py"]),
        ]
        try:
            for name, command in commands:
                before = server_usage(cluster.servers)
                with (output / f"{name}.log").open("w") as log:
                    try:
                        run(["taskset", "-c", args.client_cpu, *map(str, command)], env=environment,
                            stdout=log, stderr=subprocess.STDOUT)
                    finally:
                        print((output / f"{name}.log").read_text(), end="", flush=True)
                        manifest[name] = {"before": before, "after": server_usage(cluster.servers)}
            for node in (12, 11):
                cluster.control({"action": "kill", "node": node})
            for node in (11, 12):
                cluster.control({"action": "start", "node": node})
            run([str(native_python), str(tests / "real_consumer.py")],
                env=dict(environment, CROWDB_DATASET_PHASE="verify"))
        finally:
            manifest["after"] = server_usage(cluster.servers)
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print("Dataset HTTP/native comparison and restart acceptance complete", flush=True)


if __name__ == "__main__":
    main()
