# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Build and stage the locked Rust RPC client, never a host server."""

import json
import io
import os
import shutil
import subprocess
import tarfile
import time
from pathlib import Path

CLIENT_BUILD_DIRS = ("target", "node_modules", "dist", "__pycache__", "sdk-check", ".pixi")


def client_sources(root):
    """Enumerate client inputs without traversing installed dependency trees."""
    for directory, children, files in os.walk(root):
        children[:] = sorted(name for name in children if name not in CLIENT_BUILD_DIRS)
        for name in sorted(files):
            path = Path(directory) / name
            if path.is_file():
                yield path


def run_client(bundle, addresses, token, phase, artifacts,
               test_name="e2e_three_node_cluster_kv_put_batch_delete", timeout=120):
    """Run the host client with per-invocation endpoints and require one case."""
    env = os.environ.copy()
    env.update(CROWDB_E2E_RPC=json.dumps(addresses), CROWDB_E2E_TOKEN=token,
               CROWDB_E2E_PHASE=phase, CROWDB_E2E_ARTIFACTS=str(artifacts),
               CROWDB_E2E_CONTROL=str(Path(artifacts) / "control"),
               CROWDB_E2E_CLIENT_ROOT=str(Path(bundle) / "clients"))
    bypass = ",".join(filter(None, [env.get("NO_PROXY", ""), env.get("no_proxy", ""), *addresses]))
    env.update(NO_PROXY=bypass, no_proxy=bypass)
    cli = Path(bundle) / "crowdb-cli"
    if cli.is_file():
        env["CROWDB_E2E_CLI_BIN"] = str(cli)
    sdk = Path(bundle) / "iceberg-rust-client"
    if sdk.is_file():
        env["CROWDB_ICEBERG_RUST_CLIENT_BIN"] = str(sdk)
    env["LD_LIBRARY_PATH"] = str(Path(bundle) / "lib") + ":" + env.get("LD_LIBRARY_PATH", "")
    started = time.monotonic()
    try:
        result = subprocess.run([str(Path(bundle) / "domain-test"), "--ignored", "--exact",
            test_name, "--nocapture"],
            env=env, capture_output=True, text=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        stdout, stderr = error.stdout or b"", error.stderr or b""
        output = (stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout)
        output += (stderr.decode(errors="replace") if isinstance(stderr, bytes) else stderr)
        Path(artifacts, f"client-{phase}.log").write_text(output)
        Path(artifacts, f"client-{phase}.json").write_text(json.dumps({
            "phase": phase, "case": test_name, "seconds": time.monotonic() - started,
            "status": "timeout", "execution": "host"}, indent=2))
        print(output, end="", flush=True)
        raise
    output = result.stdout + result.stderr
    Path(artifacts, f"client-{phase}.log").write_text(output)
    Path(artifacts, f"client-{phase}.json").write_text(json.dumps({
        "phase": phase, "case": test_name, "seconds": time.monotonic() - started,
        "exit_code": result.returncode, "execution": "host"}, indent=2))
    print(output, end="", flush=True)
    if result.returncode or "test result: ok. 1 passed; 0 failed;" not in output:
        raise RuntimeError(f"host client {phase} did not complete exactly one passing case")


def bundle_client(output):
    result = subprocess.run(["cargo", "test", "--locked", "-p", "e2e-test", "--test", "domain_test",
                             "--no-run", "--message-format=json"], capture_output=True, text=True,
                            check=False, timeout=600)
    print(result.stderr, end="", flush=True)
    if result.returncode:
        print(result.stdout, end="", flush=True)
        raise RuntimeError("container RPC client compilation failed")
    binaries = [item["executable"] for line in result.stdout.splitlines() if line.startswith("{")
                and (item := json.loads(line)).get("target", {}).get("name") == "domain_test"
                and item.get("executable")]
    if len(binaries) != 1:
        raise RuntimeError("missing or ambiguous domain test executable")
    output = Path(output).resolve()
    (output / "lib").mkdir(parents=True)
    shutil.copy2(binaries[0], output / "domain-test")
    shutil.copytree(Path(__file__).parent / "clients", output / "clients",
                    ignore=shutil.ignore_patterns(*CLIENT_BUILD_DIRS))
    result = subprocess.run(["ldd", "-r", str(output / "domain-test")], capture_output=True, text=True, check=True)
    if "not found" in result.stdout or "undefined symbol" in result.stdout:
        raise RuntimeError(result.stdout)
    prefix = Path(os.environ["CONDA_PREFIX"]).resolve()
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[1] == "=>":
            source = Path(parts[2]).resolve()
            if source.is_relative_to(prefix):
                shutil.copy2(source, output / "lib" / parts[0])
    return output


def bundle_iceberg_rust(output):
    """Stage the unchanged pinned Apache client separately from server artifacts."""
    root = Path(__file__).resolve().parents[1]
    manifest = Path(output) / "clients/iceberg-rust/Cargo.toml"
    result = subprocess.run(["cargo", "build", "--locked", "--manifest-path", str(manifest),
        "--target-dir", str(root / "target/iceberg-sdk"), "--message-format=json"],
        capture_output=True, text=True, check=False, timeout=600)
    print(result.stderr, end="", flush=True)
    if result.returncode:
        print(result.stdout, end="", flush=True)
        raise RuntimeError("pinned Apache Iceberg Rust client compilation failed")
    binaries = [item["executable"] for line in result.stdout.splitlines() if line.startswith("{")
        and (item := json.loads(line)).get("target", {}).get("name") == "crowdb-iceberg-rust-client-fixture"
        and item.get("executable")]
    if len(binaries) != 1:
        raise RuntimeError("missing or ambiguous pinned Rust SDK executable")
    shutil.copy2(binaries[0], Path(output) / "iceberg-rust-client")


def bundle_iceberg_java(output):
    """Compile pinned host Java SDKs once before independent fixture dispatch."""
    manifest = Path(output) / "clients/iceberg-java/pom.xml"
    result = subprocess.run(["pixi", "run", "-e", "iceberg-e2e", "mvn",
        "--batch-mode", "--no-transfer-progress", "-f", str(manifest),
        "dependency:go-offline", "compile", "exec:help"],
        capture_output=True, text=True, check=False, timeout=600)
    print(result.stdout + result.stderr, end="", flush=True)
    if result.returncode:
        raise RuntimeError("pinned Apache Iceberg Java clients failed to compile")


def bundle_cli(output):
    """Build the host CLI once and pin its executable alongside the test client."""
    result = subprocess.run(["cargo", "build", "--locked", "-p", "crowdb-cli", "--bin", "crowdb-cli",
                             "--message-format=json"], capture_output=True, text=True, check=False, timeout=600)
    print(result.stderr, end="", flush=True)
    if result.returncode:
        print(result.stdout, end="", flush=True)
        raise RuntimeError("host CLI compilation failed")
    binaries = [item["executable"] for line in result.stdout.splitlines() if line.startswith("{")
                and (item := json.loads(line)).get("target", {}).get("name") == "crowdb-cli"
                and item.get("executable")]
    if len(binaries) != 1:
        raise RuntimeError("missing or ambiguous host CLI executable")
    shutil.copy2(binaries[0], Path(output) / "crowdb-cli")


def bundle_iceberg_rck(output):
    """Verify immutable upstream source and isolate Gradle outputs per run."""
    revision = "6976e020b894f6a6777704df2b8c4458cb291ae9"
    root = Path(__file__).resolve().parents[1]
    source = Path(os.environ.get("CROWDB_ICEBERG_RCK_ROOT", root / "target/iceberg-rck"))
    if not source.exists():
        source.mkdir(parents=True)
        subprocess.run(["git", "-C", str(source), "init"], check=True, timeout=60)
        subprocess.run(["git", "-C", str(source), "fetch", "--depth", "1",
                        "https://github.com/apache/iceberg.git", revision], check=True, timeout=600)
        subprocess.run(["git", "-C", str(source), "checkout", "--detach", "FETCH_HEAD"],
                       check=True, timeout=60)
    actual = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"],
                                     text=True, timeout=60).strip()
    dirty = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain",
                                     "--untracked-files=no"], text=True, timeout=60)
    if actual != revision or dirty:
        raise RuntimeError(f"RCK requires clean {revision}; checkout preserved at {source}")
    destination = Path(output) / "clients/iceberg-rck/source"
    archive = subprocess.check_output(["git", "-C", str(source), "archive", revision],
                                      timeout=60)
    destination.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tree:
        tree.extractall(destination, filter="data")
    # The pinned commit is the peeled apache-iceberg-1.11.0 release tag.
    # Upstream source distributions provide this file because git archive
    # omits Git metadata; never derive their version from the caller's repo.
    (destination / "version.txt").write_text("1.11.0\n")
    # Upstream buildInfo copies this source-distribution file instead of
    # trying to discover Git metadata outside the archived source tree.
    (destination / "iceberg-build.properties").write_text(
        f"git.commit.id={revision}\n"
        f"git.commit.id.abbrev={revision[:7]}\n"
        "git.build.version=1.11.0\n"
        "git.closest.tag.name=apache-iceberg-1.11.0\n"
        "git.tags=apache-iceberg-1.11.0\n")
    (destination.parent / "source.json").write_text(json.dumps({
        "upstream": "https://github.com/apache/iceberg.git", "revision": revision,
        "version": "1.11.0"}, indent=2))
