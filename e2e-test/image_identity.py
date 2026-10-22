# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Allow client-only revisions to reuse an image with identical server sources."""

import re
import os
import subprocess
import tempfile
import tomllib
from pathlib import Path

# Conservatively include manifests, dependencies, build tooling and runtime
# assets. Component tests are host clients and do not enter the release image.
SERVER_PATHS = (
    "app", "lib", "container", "tools", "cmake", ".cargo",
    "Cargo.toml", "Cargo.lock", "pixi.toml", "pixi.lock", "CMakeLists.txt",
    ":(glob,exclude)**/tests/**",
    # The UI compiler includes only src; browser fixtures, selectors and docs
    # are host clients. UI sources, compiler/Vite configuration and build tasks
    # remain guarded release inputs.
    ":(glob,exclude)app/crowdb-web/ui/e2e/**",
    ":(exclude)tools/pixi-tasks/test-console-managed-native.py",
    # The staging script does not package this operator documentation.
    ":(exclude)container/single-node-container/README.md",
    # Host development/cleanup commands are not read or packaged by staging.
    ":(exclude)container/single-node-container/dev-container.sh",
    ":(exclude)tools/pixi-tasks/clean-build.sh",
    ":(exclude)container/oci-image/README.md",
    # Host launch resource flags do not enter the release artifact (stage.sh).
    ":(exclude)container/oci-image/runtime.py",
    ":(exclude)container/oci-image/run-node.py",
    # These host CLI/cleanup modules are embedded only in crowdb-cli. stage.sh
    # builds the node services and never reads or packages these Python files.
    ":(exclude)container/oci-image/host_inputs.py",
    ":(exclude)container/oci-image/host_runtime.py",
    ":(exclude)container/oci-image/host_lifecycle.py",
    ":(exclude)container/oci-image/host_cleanup.py",
    ":(exclude)container/oci-image/host-clean.py",
    ":(exclude)container/oci-image/prepare-ci-host.py",
    # Host SDK client delegates are not read by staging or the OCI builder.
    ":(exclude)tools/pixi-tasks/test.py",
    ":(exclude)tools/test-metrics/measure.sh",
    ":(exclude)tools/test-metrics/measure.py",
    ":(exclude)tools/README.md",
    ":(exclude)tools/pixi-tasks/test-cpp.sh",
    ":(exclude)tools/pixi-tasks/test-boto3-copy-error.sh",
    ":(exclude)tools/pixi-tasks/test-s3-sdks.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-sdk.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-listing-client.sh",
    ":(exclude)tools/ci-checks/task_graph.py",
    ":(exclude)tools/ci-checks/e2e_coverage.py",
    ":(exclude)tools/pixi-tasks/test-storage.sh",
    ":(exclude)tools/pixi-tasks/test-suite.sh",
    ":(exclude)tools/pixi-tasks/test-core.sh",
    ":(exclude)tools/pixi-tasks/test-access.sh",
    ":(exclude)tools/pixi-tasks/test-console.sh",
    ":(exclude)tools/pixi-tasks/test-console-ui.sh",
    ":(exclude)tools/pixi-tasks/test-s3-e2e.sh",
    ":(exclude)tools/pixi-tasks/test-s3-client.sh",
    ":(exclude)tools/pixi-tasks/test-s3-sdk.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-native.sh",
    # Retired native host preparation never entered the release staging path.
    ":(exclude)tools/pixi-tasks/prepare-iceberg.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-e2e.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-listing-native.sh",
    ":(exclude)tools/pixi-tasks/test-pyiceberg-e2e.sh",
    ":(exclude)tools/pixi-tasks/test-rust-iceberg-e2e.sh",
    ":(exclude)tools/pixi-tasks/test-java-iceberg-fileio-e2e.sh",
    ":(exclude)tools/pixi-tasks/test-java-iceberg-e2e.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-sdk-component.sh",
    ":(exclude)tools/pixi-tasks/test-iceberg-rck.sh",
    # The retired host layout inspector was never part of release staging.
    ":(exclude)lib/crowdb-chunkdb-client/examples/single_node_chunk_layout.rs",
    # Version verification reads host SDK manifests and is never staged.
    ":(exclude)tools/ci-checks/check-version.py",
    # CI task reachability inspects host test delegates, never release inputs.
    ":(exclude)tools/ci-checks/check-ci-test-tasks.py",
    # Independent SDK packaging builds host test consumers only; release staging
    # neither reads this helper nor packages its output.
    ":(exclude)tools/sdk/package.py",
    # This host-only RPC benchmark is not a dependency or staged server binary.
    ":(exclude)app/crowdb-cli/src/commands/bench/rpc.rs",
    ":(exclude)app/crowdb-cli/src/commands/bench/result.rs",
    ":(exclude)app/crowdb-cli/src/commands/bench/kv/read.rs",
    ":(exclude)app/crowdb-cli/src/commands/bench/kv/scan.rs",
    ":(exclude)app/crowdb-cli/src/commands/bench/kv/write.rs",
    # This workload driver has only CLI consumers; server consumer changes
    # remain guarded. Its test hook does not enter a release build.
    ":(exclude)lib/crowdb-console-shared/src/ops/s3_bench.rs",
    # Host CLI lifecycle is not built or packaged by release staging.
    ":(exclude)app/crowdb-cli/src/commands.rs",
    ":(exclude)app/crowdb-cli/src/commands/s3.rs",
    ":(glob,exclude)app/crowdb-cli/src/commands/s3/**",
    ":(exclude)app/crowdb-cli/src/commands/cluster.rs",
    ":(glob,exclude)app/crowdb-cli/src/commands/cluster/**",
    ":(exclude)app/crowdb-cli/src/main.rs",
    ":(exclude)app/crowdb-cli/src/commands/kv.rs",
    ":(exclude)app/crowdb-cli/src/commands/kv/server.rs",
    ":(glob,exclude)app/crowdb-cli/src/commands/kv/server/**",
    ":(exclude)app/crowdb-cli/src/commands/launch.rs",
    # Audited host-only crate: every consumer uses a dev-dependency. A new
    # runtime/build dependency changes its owning manifest and rejects reuse.
    ":(exclude)lib/crowdb-test-harness/Cargo.toml",
    ":(exclude)lib/crowdb-test-harness/src/lib.rs",
    ":(exclude)lib/crowdb-test-harness/src/test_dirs.rs",
    ":(exclude)lib/crowdb-test-harness/src/cluster.rs",
    ":(exclude)lib/crowdb-test-harness/src/hardware.rs",
    ":(exclude)lib/crowdb-test-harness/src/logging.rs",
    ":(exclude)lib/crowdb-test-harness/src/diskio.rs",
    ":(exclude)lib/crowdb-test-harness/src/diskdb.rs",
    ":(exclude)lib/crowdb-test-harness/src/chunkdb.rs",
    ":(exclude)lib/crowdb-test-harness/src/chunk_kv.rs",
)



def production_pixi_manifest(manifest):
    # Explicit host task allowlist: release build tasks and dependencies
    # remain guarded. stage.sh never reads these test commands.
    host_tasks = {
        "test", "test-core", "test-storage", "test-access", "test-console",
        "test-console-ui", "test-cpp", "test-suite", "test-access-sdk",
        "test-s3-java-sdk", "test-s3-js-sdk", "test-s3-go-sdk", "test-s3-sdks",
        *("test-e2e-" + domain for domain in (
            "kv", "diskdb", "diskio", "chunkdb", "chunk-stream", "chunk-client",
            "chunk-kv", "s3", "iceberg", "dataset", "console", "deployment")),
    }
    feature_tasks = {
        "s3-e2e": {"test-boto3-e2e", "test-boto3-copy-error", "test-aws-cli-e2e",
                   "test-aws-cli-concurrent", "test-rclone-e2e"},
        "iceberg-e2e": {"test-iceberg-listing-client", "test-iceberg-listing-native",
            "test-iceberg-native", "test-java-iceberg-e2e", "test-rust-iceberg-e2e",
            "test-iceberg-rck", "test-iceberg-e2e", "test-iceberg-sdk",
            "test-iceberg-sdk-component", "test-java-iceberg-fileio-e2e", "test-pyiceberg-e2e"},
        "iceberg-ecosystem": {"test-iceberg-container-ecosystem"},
    }
    for task in host_tasks:
        manifest.get("tasks", {}).pop(task, None)
    for feature, tasks in feature_tasks.items():
        table = manifest.get("feature", {}).get(feature, {}).get("tasks", {})
        for task in tasks:
            table.pop(task, None)
        if not table:
            manifest.get("feature", {}).get(feature, {}).pop("tasks", None)
        if not manifest.get("feature", {}).get(feature, {}):
            manifest.get("feature", {}).pop(feature, None)
    if not manifest.get("feature", {}):
        manifest.pop("feature", None)
    return manifest


def production_manifest(manifest):
    result = dict(manifest)
    result.pop("test", None)
    result.pop("dev-dependencies", None)
    if "example" in result:
        examples = [entry for entry in result["example"] if not (
            entry.get("path", "").startswith("tests/common/")
            and entry.get("required-features") == ["test-util"])]
        if examples:
            result["example"] = examples
        else:
            result.pop("example")
    if result.get("package", {}).get("name") == "crowdb-console-shared":
        features = dict(result.get("features", {}))
        if features.get("test-util") == []:
            features.pop("test-util")
        if features:
            result["features"] = features
        else:
            result.pop("features", None)
    return result


def lock_matches(before, after, manifests):
    def packages(lock):
        return {(p["name"], p["version"], p.get("source")): p
                for p in lock["package"] if p["name"] not in ("e2e-test", "crowdb-test-harness")}
    old, new = packages(before), packages(after)
    if old.keys() != new.keys():
        return False
    for key, previous in old.items():
        current = new[key]
        if previous == current:
            continue
        if {k: v for k, v in previous.items() if k != "dependencies"} != {
                k: v for k, v in current.items() if k != "dependencies"}:
            return False
        pair = manifests.get(key[0])
        if not pair or production_manifest(pair[0]) != production_manifest(pair[1]):
            return False
        dev, production = set(), set()
        for manifest in pair:
            for alias, specification in manifest.get("dev-dependencies", {}).items():
                dev.add(specification.get("package", alias) if isinstance(specification, dict) else alias)
            sections = [manifest, *manifest.get("target", {}).values()]
            for sections_for_target in sections:
                for section in ("dependencies", "build-dependencies"):
                    for alias, specification in sections_for_target.get(section, {}).items():
                        production.add(specification.get("package", alias) if isinstance(specification, dict) else alias)
        dev -= production
        changed = set(previous.get("dependencies", ())) ^ set(current.get("dependencies", ()))
        if any(dependency.split()[0] not in dev for dependency in changed):
            return False
    return {k: v for k, v in before.items() if k != "package"} == {
        k: v for k, v in after.items() if k != "package"}


def verify_server_sources(root, revision, source_tree=None):
    with tempfile.TemporaryDirectory(prefix="crowdb-source-check-") as directory:
        return _verify_with_index(root, revision, source_tree, Path(directory) / "index")


def _verify_with_index(root, revision, source_tree, index):
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("release image needs a full source revision label")

    environment = dict(os.environ, GIT_INDEX_FILE=str(index), GIT_OPTIONAL_LOCKS="0")

    def git(*arguments):
        return subprocess.check_output(["git", "-C", str(root), *arguments],
                                       env=environment, text=True).strip()

    try:
        git("rev-parse", "--verify", revision + "^{commit}")
        source = revision
        comparison = [source]
        if source_tree is not None:
            if not isinstance(source_tree, str) or not re.fullmatch(r"[0-9a-f]{40}", source_tree):
                raise ValueError("release image source tree label is invalid")
            if git("rev-parse", "--verify", source_tree + "^{tree}") != source_tree:
                raise ValueError("release image source tree label must identify a tree")
            source = source_tree
        # A private baseline index includes dirty inputs staged into the image
        # tree, even if the operator never added them to their real index.
        # Comparing its working files writes neither that index nor Git objects.
        git("read-tree", source)
        comparison = [source]
        changed = git("diff", "--name-only", *comparison, "--", *SERVER_PATHS)
        untracked = git("ls-files", "--others", "--exclude-standard", "--", *SERVER_PATHS)
        added = set(git("diff", "--name-only", "--diff-filter=A", *comparison, "--", *SERVER_PATHS).splitlines())
    except subprocess.CalledProcessError as error:
        raise ValueError("image source revision cannot be verified locally; rebuild the shared OCI image") from error
    manifests = {}
    for path in changed.splitlines():
        if path not in added and path.endswith("Cargo.toml") and Path(root, path).is_file():
            pair = (tomllib.loads(git("show", f"{source}:{path}")),
                    tomllib.loads(Path(root, path).read_text()))
            name = pair[0].get("package", {}).get("name")
            if name:
                manifests[name] = pair
    runtime_changes = []
    for path in changed.splitlines():
        if path in added:
            runtime_changes.append(path)
            continue
        current = Path(root, path)
        if path == "pixi.toml" and current.is_file():
            before = tomllib.loads(git("show", f"{source}:{path}"))
            after = tomllib.loads(current.read_text())
            before = production_pixi_manifest(before)
            after = production_pixi_manifest(after)
            if before == after:
                continue
        if path == "Cargo.lock" and current.is_file():
            before = tomllib.loads(git("show", f"{source}:{path}"))
            after = tomllib.loads(current.read_text())
            if lock_matches(before, after, manifests):
                continue
        if path.endswith("Cargo.toml") and current.is_file():
            before = tomllib.loads(git("show", f"{source}:{path}"))
            after = tomllib.loads(current.read_text())
            if production_manifest(before) == production_manifest(after):
                continue
        runtime_changes.append(path)
    changed = "\n".join(runtime_changes)
    if changed or untracked:
        raise ValueError("release server sources differ from the image; rebuild the shared OCI image:\n" +
                         "\n".join(filter(None, [changed, untracked])))
    return {"image_revision": revision, "image_source_tree": source_tree,
            "client_revision": git("rev-parse", "HEAD"),
            "server_sources_match": True}
