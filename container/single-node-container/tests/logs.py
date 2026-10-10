"""Check lifecycle coverage and secret exclusion in disposable container logs."""

import gzip
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(sys.argv[1])
container = sys.argv[2]
secrets = []
for name in ["server.env", "client.env"]:
    private = subprocess.run([
        "docker", "run", "--rm", "--network", "none", "--user", "root",
        "--mount", f"type=bind,source={root},target=/data,readonly",
        "--entrypoint", "/bin/cat",
        os.environ.get("CROWDB_CONTAINER_IMAGE", "crowdb-node:dev"),
        f"/data/secrets/{name}",
    ], capture_output=True, check=True, timeout=30)
    for line in private.stdout.splitlines():
        key, _, value = line.partition(b"=")
        if value and any(word in key for word in [b"TOKEN", b"KEY"]):
            secrets.append(value)

assert secrets, "credential files contain no secret values to check"

def check_secret_free(body):
    assert all(secret not in body for secret in secrets), "secret leaked into diagnostic logs"

events = set()
for path in (root / "log").rglob("*"):
    if not path.is_file():
        continue
    body = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
    check_secret_free(body)
    if path.name.startswith("monitor.log"):
        for line in body.splitlines():
            events.add(json.loads(line)["kind"])
required = {"starting", "ready", "bootstrap_step_started", "bootstrap_step_completed",
            "child_started", "child_stopped", "child_exited", "probe_failed",
            "restarting", "draining", "stopped", "restart_exhausted"}
assert required <= events, f"missing lifecycle events: {required - events}"
logs = subprocess.run(["docker", "logs", container], capture_output=True, check=True, timeout=10)
check_secret_free(logs.stdout + logs.stderr)
print("Container lifecycle events and secret-free logs passed")
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
