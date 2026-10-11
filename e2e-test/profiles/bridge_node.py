# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Owned manual nodes expose fixed listeners at inspected bridge addresses."""

import json
import os
from pathlib import Path
import sys
import subprocess
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from admission_profile import cleanup as cleanup_runtime

IMAGE = os.environ["CROWDB_CONTAINER_IMAGE"]
PREFIX = os.environ["CROWDB_E2E_PROFILE_NAME"]
DATA = Path(os.environ["CROWDB_E2E_PROFILE_DATA"])
CLIENT = Path(__file__).resolve().parents[1] / "clients/console-ui"
BROWSER_OUTPUT = DATA / "browser"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def origin_for(name):
    node = json.loads(docker("inspect", name))[0]
    address = node["NetworkSettings"]["Networks"][PREFIX]["IPAddress"]
    if not address:
        raise ValueError("owned bridge node has no inspected IP")
    return f"http://{address}:9090"


def start_node(name):
    data = DATA / name.removeprefix(PREFIX + "-")
    data.mkdir(mode=0o755)
    docker("run", "--rm", "--name", PREFIX + "-prepare", "--network", "none",
           "--label", f"org.crowdb.e2e.run={PREFIX}", "--user", "0",
           "--cpus", "0.25", "--memory", "32m", "--entrypoint", "/bin/chown",
           "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
           IMAGE, "10001:10001", "/opt/crowdb/data")
    docker("run", "-d", "--name", name, "--network", PREFIX,
           "--label", f"org.crowdb.e2e.run={PREFIX}",
           "--cpus", os.environ["CROWDB_E2E_NODE_CPUS"], "--memory", "1g",
           "--ulimit", "core=-1", "--pids-limit", "256",
           "--mount", f"type=bind,source={data},target=/opt/crowdb/data",
           "-e", "CROWDB_STARTUP_MODE=manual",
           "-e", "CROWDB_PHYSICAL_HOST_ID=one-docker-host", IMAGE)
    return origin_for(name)


def cleanup(prefix):
    cleanup_runtime(prefix, DATA, IMAGE)


def browser(origin, selection, label, **inputs):
    env = {key:value for key,value in os.environ.items() if not key.startswith("CROWDB_NODE_UI_")}
    output = BROWSER_OUTPUT / label
    env.update(inputs, CROWDB_NODE_UI_ORIGIN=origin, CROWDB_WEB_E2E_OUTPUT=str(output))
    subprocess.run(["npx", "playwright", "test", "--config=bridge.config.ts",
                    "e2e/flows/12-bridge-discovery.spec.ts", "--grep", selection],
                   cwd=CLIENT, env=env, check=True, timeout=120)
    report = json.loads((output / "results.json").read_text())
    stats = report["stats"]
    if stats["expected"] != 1 or any(stats[field] for field in ("unexpected", "flaky", "skipped")):
        raise AssertionError(f"required bridge browser flow did not pass exactly once: {stats}")


def docker(*args):
    result = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=60)
    if result.returncode:
        displayed = list(args)
        if "--json" in displayed:
            displayed[displayed.index("--json") + 1] = "<node-control request>"
        raise RuntimeError(f"docker {displayed}: {result.stdout}\n{result.stderr}")
    return (result.stdout + result.stderr if args and args[0] == "logs" else result.stdout).strip()
