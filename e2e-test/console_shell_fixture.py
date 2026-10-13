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
from console_standalone_observation import StandaloneBrowserObservations
from control import Control
from timeout_budget import scaled_timeout


class ShellBrowserCluster(StandaloneWebCluster):
    def __init__(self, image, artifacts, bundle, **kwargs):
        super().__init__(image, artifacts, **kwargs)
        self.bundle = bundle

    def observed_browser(self, name):
        with Control(StandaloneBrowserObservations(self)) as control:
            self.browser(name, extra_env={"CROWDB_NATIVE_OBSERVATION_CONTROL": str(control.directory)})

    def browser(self, name, *, config="shell.config.ts", extra_env=None, expected=1):
        project = self.artifacts / "browser-client"
        shutil.copytree(self.bundle / "clients/console-ui", project,
                        ignore=shutil.ignore_patterns("node_modules"))
        env = os.environ.copy()
        address = self.addresses[0]
        browser_output = (self.artifacts / "browser").resolve()
        bypass = ",".join(filter(None, [env.get("NO_PROXY", ""), env.get("no_proxy", ""), address]))
        env.update(NO_PROXY=bypass, no_proxy=bypass,
                   CROWDB_WEB_E2E_BASE_URL=f"http://{address}:9090",
                   CROWDB_WEB_E2E_OUTPUT=str(browser_output))
        env.update(extra_env or {})
        test_budget = scaled_timeout(max(getattr(self, "browser_timeout", 120),
                                         180 if config == "native.config.ts" else 60))
        env["CROWDB_WEB_E2E_TEST_TIMEOUT_MS"] = str(round(test_budget * 1000))
        self.invoke(["pixi", "run", "npm", "ci", "--ignore-scripts"], project, env, "browser-prepare")
        pattern = ("|".join(re.escape(title) for title in name)
                   if isinstance(name, tuple) else re.escape(name))
        self.invoke(["pixi", "run", "npx", "--no-install", "playwright", "test",
                     f"--config={config}", "--grep", pattern], project, env, "browser",
                    timeout=test_budget + 60)
        stats = json.loads((self.artifacts / "browser/results.json").read_text())["stats"]
        if stats["expected"] != expected or any(stats[key] for key in ("unexpected", "skipped", "flaky")):
            raise AssertionError(f"host browser did not complete exactly {expected} cases: {stats}")

    def invoke(self, arguments, project, env, phase, *, timeout=120):
        started = time.monotonic()
        result = subprocess.run(arguments, cwd=project, env=env, capture_output=True,
                                text=True, timeout=timeout)
        output = result.stdout + result.stderr
        (self.artifacts / f"client-{phase}.log").write_text(output)
        (self.artifacts / f"client-{phase}.json").write_text(json.dumps({
            "case": self.case_name, "phase": phase, "seconds": time.monotonic() - started,
            "exit_code": result.returncode, "execution": "host"}, indent=2))
        if result.returncode:
            print(f"[FAILED CLIENT] case={self.case_name} phase={phase} exit_code={result.returncode}", flush=True)
        print(output, end="", flush=True)
        if result.returncode:
            raise RuntimeError(f"host browser {self.case_name} {phase} failed with exit code {result.returncode}; "
                               f"log: {self.artifacts / f'client-{phase}.log'}\n{output}")
