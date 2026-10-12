# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Host shell browser flows against an ordinary release Web container."""

import json
import os
import re
import shutil
import subprocess
import time

from console_standalone_fixture import StandaloneWebCluster


class ShellBrowserCluster(StandaloneWebCluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, **kwargs)
        self.bundle = bundle

    def browser(self, name, *, config="shell.config.ts", extra_env=None):
        project = self.artifacts / "browser-client"
        shutil.copytree(self.bundle / "clients/console-ui", project,
                        ignore=shutil.ignore_patterns("node_modules"))
        env = os.environ.copy()
        address = self.addresses[0]
        bypass = ",".join(filter(None, [env.get("NO_PROXY", ""), env.get("no_proxy", ""), address]))
        env.update(NO_PROXY=bypass, no_proxy=bypass,
                   CROWDB_WEB_E2E_BASE_URL=f"http://{address}:9090",
                   CROWDB_WEB_E2E_OUTPUT=str(self.artifacts / "browser"))
        env.update(extra_env or {})
        self.invoke(["pixi", "run", "npm", "ci", "--ignore-scripts"], project, env, "browser-prepare")
        self.invoke(["pixi", "run", "npx", "--no-install", "playwright", "test",
                     f"--config={config}", "--grep", re.escape(name)], project, env, "browser",
                    timeout=getattr(self, "browser_timeout", 120))
        stats = json.loads((self.artifacts / "browser/results.json").read_text())["stats"]
        if stats["expected"] != 1 or any(stats[key] for key in ("unexpected", "skipped", "flaky")):
            raise AssertionError(f"shell browser did not complete exactly one case: {stats}")

    def invoke(self, arguments, project, env, phase, *, timeout=120):
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
            raise RuntimeError(f"host shell browser {phase} failed")
