# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Opt-in CLI accounting regression, owned by a normal disposable node."""

import json
import os
import subprocess
import time

from fixture import redact


def pending_free(cluster):
    name = "mixed_disk_workload_normalizes_frees_predating_its_baseline"
    env = cluster.environment()
    env.update(CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT=cluster.endpoint(0),
               CROWDB_E2E_CLI_BIN=str(cluster.bundle / "crowdb-cli"),
               LD_LIBRARY_PATH=str(cluster.bundle / "lib") + ":" + os.environ.get("LD_LIBRARY_PATH", ""))
    started = time.monotonic()
    result = subprocess.run([str(cluster.bundle / "cli-workload-test"), "--ignored", "--exact", name,
                             "--nocapture"], env=env, capture_output=True, text=True, timeout=120)
    output = redact(result.stdout + result.stderr)
    (cluster.artifacts / "client-pending-free.log").write_text(output)
    (cluster.artifacts / "client-pending-free.json").write_text(json.dumps({
        "case": cluster.case_name, "phase": "pending-free-accounting", "execution": "host",
        "seconds": time.monotonic() - started, "exit_code": result.returncode}, indent=2))
    print(output, end="", flush=True)
    if result.returncode or "test result: ok. 1 passed; 0 failed;" not in output:
        raise AssertionError("pending-free regression did not execute exactly one passing test")
