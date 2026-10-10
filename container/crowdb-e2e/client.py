# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Build and stage the locked Rust RPC client, never a host server."""

import json
import os
import shutil
import subprocess
from pathlib import Path


def bundle_client(output):
    result = subprocess.run(["cargo", "test", "--locked", "-p", "crowdb-e2e", "--test", "kv_test",
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
