#!/usr/bin/env python3
"""Check workspace package assignments and CI test-task reachability."""

import json
import subprocess
import sys
from pathlib import Path


COMPONENT_PACKAGES = {
    "test-cpp": {"crowdb-tree-ffi", "crowdb-rpc-ffi", "crowdb-rpc-test"},
    "test-core": {
        "crowdb-common", "crowdb-test-harness", "crowdb-protocol",
        "crowdb-paxos-kv", "crowdb-paxos-kv-client",
    },
    "test-storage": {
        "crowdb-chunkdb-client", "crowdb-chunk-kv", "crowdb-chunk-kv-client",
        "crowdb-chunk-kv-server", "crowdb-chunk-stream", "crowdb-paxos-kv-server",
        "crowdb-diskdb", "crowdb-diskdb-client", "crowdb-chunkdb",
        "crowdb-chunk-client", "crowdb-diskio-client",
    },
    "test-access": {
        "crowdb-access-multipart", "crowdb-access-s3", "crowdb-access-iceberg",
        "crowdb-access-server", "crowdb-access-dataset", "crowdb-monitor",
        "crowdb-sdk-native", "crowdb-sdk-native-python",
    },
    "test-console": {"crowdb-console-shared", "crowdb-cli", "crowdb-web"},
}

SUPPORT_PACKAGES = {
    "e2e-test": "Host acceptance client, built and executed by test-container-e2e",
}


def workspace_packages() -> set[str]:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)
    return {package["name"] for package in metadata["packages"]}


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    packages = workspace_packages()
    assignments: dict[str, str] = {}
    for task, task_packages in COMPONENT_PACKAGES.items():
        for package in task_packages:
            previous = assignments.setdefault(package, task)
            if previous != task:
                raise SystemExit(f"package {package} is assigned to both {previous} and {task}")

    missing = sorted(packages - set(assignments) - set(SUPPORT_PACKAGES))
    unknown_support = sorted(set(SUPPORT_PACKAGES) - packages)
    if missing:
        print("Rust workspace packages missing from CI test tasks:")
        for package in missing:
            print(f"  {package}")
        print("Add the package to COMPONENT_PACKAGES in tools/ci-checks/check-ci-test-tasks.py")
        return 1
    if unknown_support:
        print("Support-package allowlist contains packages not in the workspace:")
        for package in unknown_support:
            print(f"  {package}")
        return 1

    sys.path.insert(0, str(root / "tools/ci-checks"))
    from task_graph import TaskGraph

    graph = TaskGraph(root)
    pixi_tasks = {
        name: graph.command((environment, name))
        for environment, name in graph.tasks
        if environment == "default"
    }
    missing_tasks = [task for task in COMPONENT_PACKAGES if task not in pixi_tasks]
    if missing_tasks:
        print("CI component map references missing Pixi tasks:")
        for task in missing_tasks:
            print(f"  {task}")
        return 1
    workflows = root / ".github/workflows"
    from e2e_coverage import check_coverage, expand_matrix

    workflow = (workflows / "ci.yml").read_text()
    try:
        owners = check_coverage(workflow, graph)
        sys.path.insert(0, str(root / "e2e-test"))
        from scenarios import ALL_CASES, select_cases

        selected = {domain: select_cases(layer=domain) for domain in owners}
        for case in ALL_CASES:
            matches = [domain for domain, cases in selected.items() if case in cases]
            if len(matches) != 1:
                raise ValueError(f"named flow {case.scope}::{case.name} has CI domains {matches}")
    except ValueError as error:
        print(error)
        return 1
    reachable = graph.reachable(expand_matrix(workflow))
    required = {("default", task) for task in COMPONENT_PACKAGES}
    required.update({
        ("default", "test-container-e2e"),
        ("default", "test-container-e2e-fixture"),
        ("default", "test-console-ui"),
        ("s3-e2e", "test-boto3-copy-error"),
        ("default", "test-access-sdk"),
        ("default", "test-dataset-sdk"),
        ("default", "test-dataset-sdk-python"),
        ("default", "sdk-fmt-check"),
        ("default", "sdk-lint"),
        ("default", "check-dataset-sdk-boundary"),
    })
    # Component protocol peers and complete container domains have distinct owners.
    unreachable = sorted(required - reachable)
    if unreachable:
        print("Test tasks not reachable from regular CI:")
        for environment, task in unreachable:
            print(f"  {environment}: {task}")
        return 1

    manual_tasks = {
        "docker-preview.yml": {("default", "test-single-node-container")},
    }
    for workflow, tasks in manual_tasks.items():
        manual_reachable = graph.reachable((workflows / workflow).read_text())
        missing = sorted(tasks - manual_reachable)
        if missing:
            print(f"Manual workflow {workflow} does not reach its test tasks:")
            for environment, task in missing:
                print(f"  {environment}: {task}")
            return 1
        unexpected = sorted(tasks & reachable)
        if unexpected:
            print("Manual-only test tasks also reachable from regular CI:")
            for environment, task in unexpected:
                print(f"  {environment}: {task}")
            return 1

    print(f"CI component tasks verified for {len(packages)} workspace packages")
    print(f"Complete container domains verified: {owners}")
    print(f"Named container flows verified: {len(ALL_CASES)}")
    for package in sorted(assignments):
        print(f"  {package}: {assignments[package]}")
    for package, reason in sorted(SUPPORT_PACKAGES.items()):
        print(f"  {package}: support ({reason})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
