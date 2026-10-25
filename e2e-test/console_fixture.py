# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host browser clients against the complete normal managed release node."""

import json
import os
import re
import shutil
import subprocess
import time

from control import Control
from managed_fixture import ManagedCluster

SERVICES = ("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")


class ConsoleCluster(ManagedCluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, services=SERVICES, **kwargs)
        self.bundle = bundle

    def observation(self):
        return json.loads(self.command("exec", self.servers[0], "cat",
                                      "/opt/crowdb/run/status/monitor.json"))

    def browser(self, name):
        project = self.artifacts / "browser-client"
        shutil.copytree(self.bundle / "clients/console-ui", project,
                        ignore=shutil.ignore_patterns("node_modules"))
        env = os.environ.copy()
        bypass = ",".join(filter(None, [env.get("NO_PROXY", ""), env.get("no_proxy", ""), self.address]))
        env.update(NO_PROXY=bypass, no_proxy=bypass,
                   CROWDB_WEB_E2E_BASE_URL=f"http://{self.address}:9090",
                   CROWDB_NATIVE_KV_PID=str(self.observation()["services"]["kv"]["pid"]),
                   CROWDB_E2E_CONTROL=str(self.artifacts / "control"),
                   CROWDB_WEB_E2E_OUTPUT=str(self.artifacts / "browser"),
                   CROWDB_WEB_E2E_REPORT=str(self.artifacts / "playwright.json"))
        self.invoke(["pixi", "run", "npm", "ci", "--ignore-scripts"], project, env, "browser-prepare", 120)
        with Control(self):
            self.invoke(["pixi", "run", "npx", "--no-install", "playwright", "test",
                         "--config=playwright.config.ts", "--grep", re.escape(name)], project, env, "browser", 240)
        report = json.loads((self.artifacts / "playwright.json").read_text())
        stats = report["stats"]
        if stats["expected"] != 1 or stats["unexpected"] or stats["skipped"] or stats["flaky"]:
            raise AssertionError(f"browser did not complete exactly one case: {stats}")

    def invoke(self, arguments, project, env, phase, timeout):
        started = time.monotonic()
        result = subprocess.run(arguments, cwd=project, env=env, capture_output=True,
                                text=True, timeout=timeout)
        output = result.stdout + result.stderr
        (self.artifacts / f"client-{phase}.log").write_text(output)
        (self.artifacts / f"client-{phase}.json").write_text(json.dumps({
            "case": self.case_name, "phase": phase, "seconds": time.monotonic() - started,
            "exit_code": result.returncode, "execution": "host"}, indent=2))
        print(output, end="", flush=True)
        if result.returncode:
            raise RuntimeError(f"host browser {phase} failed")

    def control(self, body):
        if body.get("action") != "signal-service":
            return super().control(body)
        # Validate ownership through the same guarded container control path.
        super().control({"action": "status", "node": body.get("node")})
        if body.get("service") != "kv" or body.get("signal") not in ("STOP", "CONT"):
            raise ValueError("unsupported managed service signal")
        pid = self.observation()["services"]["kv"]["pid"]
        if type(body.get("pid")) is not int or body["pid"] != pid or pid <= 1:
            raise ValueError("managed service PID changed")
        self.command("exec", self.servers[0], "/bin/kill", "-" + body["signal"], str(pid))
        return {"pid": pid, "signal": body["signal"]}
