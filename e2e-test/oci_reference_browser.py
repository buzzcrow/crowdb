# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Existing rack editor flow against an unchanged reference deployment."""

import json
import os
import re
import shutil
import subprocess
import time

TITLE = "datacenter root wraps racks; Add Rack from datacenter context menu"


def verify(cluster):
    project = cluster.artifacts / "browser-client"
    shutil.copytree(cluster.bundle / "clients/console-ui", project,
                    ignore=shutil.ignore_patterns("node_modules"))
    env = dict(os.environ, NO_PROXY="*", no_proxy="*",
               CROWDB_WEB_E2E_BASE_URL=cluster.endpoint(0),
               CROWDB_WEB_E2E_OUTPUT=str((cluster.artifacts / "browser").resolve()),
               CROWDB_WEB_E2E_REPORT=str((cluster.artifacts / "playwright.json").resolve()))
    execute(cluster, project, env, "browser-prepare", ["npm", "ci", "--ignore-scripts"])
    execute(cluster, project, env, "browser", ["npx", "--no-install", "playwright", "test",
        "--config=playwright.config.ts", "e2e/flows/10-cluster-rack-node.spec.ts", "--grep", re.escape(TITLE)])
    report = json.loads((cluster.artifacts / "playwright.json").read_text())
    stats = report["stats"]
    if stats["expected"] != 1 or any(stats[field] for field in ("unexpected", "skipped", "flaky")):
        raise AssertionError(f"reference browser did not pass exactly one flow: {stats}")


def execute(cluster, project, env, phase, arguments):
    started = time.monotonic()
    result = subprocess.run(["pixi", "run", *arguments], cwd=project, env=env,
                            capture_output=True, text=True, timeout=240)
    output = result.stdout + result.stderr
    (cluster.artifacts / f"client-{phase}.log").write_text(output)
    (cluster.artifacts / f"client-{phase}.json").write_text(json.dumps({
        "case": cluster.case_name, "phase": phase, "execution": "host",
        "seconds": time.monotonic() - started, "exit_code": result.returncode}, indent=2))
    print(output, end="", flush=True)
    if result.returncode:
        raise AssertionError(f"reference {phase} failed")
