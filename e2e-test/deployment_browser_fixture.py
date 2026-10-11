# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Pinned host Playwright checks of the normally configured readonly release UI."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from control import Control
from managed_fixture import ManagedCluster


class DeploymentBrowserCluster(ManagedCluster):
    def __init__(self, image, artifacts, bundle, services, **kwargs):
        super().__init__(image, artifacts, services=services, **kwargs)
        self.bundle = bundle

    def browser(self):
        project = self.artifacts / "browser-client"
        shutil.copytree(self.bundle / "clients/console-ui", project,
                        ignore=shutil.ignore_patterns("node_modules"))
        self.invoke(["pixi", "run", "npm", "ci", "--ignore-scripts"], project,
                    os.environ.copy(), "browser-prepare")
        env = os.environ.copy()
        env.update(CROWDB_E2E_CONTROL=str(self.artifacts / "control"),
                   CROWDB_E2E_KV_PID=str(self.monitor_status()["services"]["kv"]["pid"]),
                   CROWDB_PREVIEW_TEST_ARTIFACTS=str(self.artifacts / "browser"))
        with Control(self):
            self.invoke(["pixi", "run", "node", "release-web.cjs",
                         f"http://{self.address}:9090", "owned-monitor"], project, env, "browser")

    def invoke(self, arguments, project, env, phase):
        started = time.monotonic()
        result = subprocess.run(arguments, cwd=project, env=env, capture_output=True,
                                text=True, timeout=120)
        output = result.stdout + result.stderr
        (self.artifacts / f"client-{phase}.log").write_text(output)
        (self.artifacts / f"client-{phase}.json").write_text(json.dumps({
            "case": self.case_name, "phase": phase, "seconds": time.monotonic() - started,
            "exit_code": result.returncode, "execution": "host"}, indent=2))
        print(output, end="", flush=True)
        if result.returncode or (phase == "browser" and
                "Container managed Web browser acceptance passed" not in output):
            raise RuntimeError(f"host release UI {phase} failed")

    def control(self, body):
        if body.get("action") != "signal-kv":
            return super().control(body)
        super().control({"action": "status", "node": body.get("node")})
        if body.get("signal") not in ("STOP", "CONT"):
            raise ValueError("unsupported browser service signal")
        pid = self.monitor_status()["services"]["kv"]["pid"]
        if type(body.get("pid")) is not int or body["pid"] != pid or pid <= 1:
            raise ValueError("browser service PID changed")
        executable = self.command("exec", self.servers[0], "cat", f"/proc/{pid}/cmdline").split("\0")[0]
        if Path(executable).name != "crowdb-kv-server":
            raise ValueError("browser service executable changed")
        self.command("exec", self.servers[0], "kill", "-" + body["signal"], str(pid))
        return {"pid": pid}
