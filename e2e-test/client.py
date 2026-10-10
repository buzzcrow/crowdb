# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Build and stage the locked Rust RPC client, never a host server."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path


def run_client(bundle, addresses, token, phase, artifacts,
               test_name="e2e_three_node_cluster_kv_put_batch_delete"):
    """Run the host client with per-invocation endpoints and require one case."""
    env = os.environ.copy()
    env.update(CROWDB_E2E_RPC=json.dumps(addresses), CROWDB_E2E_TOKEN=token,
               CROWDB_E2E_PHASE=phase)
    env["LD_LIBRARY_PATH"] = str(Path(bundle) / "lib") + ":" + env.get("LD_LIBRARY_PATH", "")
    started = time.monotonic()
    try:
        result = subprocess.run([str(Path(bundle) / "kv-test"), "--ignored", "--exact",
            test_name, "--nocapture"],
            env=env, capture_output=True, text=True, check=False, timeout=120)
    except subprocess.TimeoutExpired as error:
        stdout, stderr = error.stdout or b"", error.stderr or b""
        output = (stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout)
        output += (stderr.decode(errors="replace") if isinstance(stderr, bytes) else stderr)
        Path(artifacts, f"client-{phase}.log").write_text(output)
        Path(artifacts, f"client-{phase}.json").write_text(json.dumps({
            "phase": phase, "seconds": time.monotonic() - started,
            "status": "timeout", "execution": "host"}, indent=2))
        print(output, end="", flush=True)
        raise
    output = result.stdout + result.stderr
    Path(artifacts, f"client-{phase}.log").write_text(output)
    Path(artifacts, f"client-{phase}.json").write_text(json.dumps({
        "phase": phase, "seconds": time.monotonic() - started,
        "exit_code": result.returncode, "execution": "host"}, indent=2))
    print(output, end="", flush=True)
    if result.returncode or "test result: ok. 1 passed; 0 failed;" not in output:
        raise RuntimeError(f"host KV {phase} did not complete exactly one passing case")


def bundle_client(output):
    result = subprocess.run(["cargo", "test", "--locked", "-p", "e2e-test", "--test", "kv_test",
                             "--no-run", "--message-format=json"], capture_output=True, text=True,
                            check=False, timeout=600)
    print(result.stderr, end="", flush=True)
    if result.returncode:
        print(result.stdout, end="", flush=True)
        raise RuntimeError("container RPC client compilation failed")
    binaries = [item["executable"] for line in result.stdout.splitlines() if line.startswith("{")
                and (item := json.loads(line)).get("target", {}).get("name") == "kv_test"
                and item.get("executable")]
    if len(binaries) != 1:
        raise RuntimeError("missing or ambiguous KV test executable")
    output = Path(output).resolve()
    (output / "lib").mkdir(parents=True)
    shutil.copy2(binaries[0], output / "kv-test")
    result = subprocess.run(["ldd", "-r", str(output / "kv-test")], capture_output=True, text=True, check=True)
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
