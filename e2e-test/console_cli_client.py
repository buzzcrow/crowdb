# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit CLI attachment and bounded, redacted host invocation artifacts."""

import json
import os
import subprocess
import time

from fixture import redact


class CliClient:
    def environment(self):
        environment = dict(os.environ, CROWDB_RUNTIME_ROOT=str(self.root), NO_PROXY="*", no_proxy="*")
        for name in ("CROWDB_CLUSTER", "CROWDB_SYSTEM_IP", "CROWDB_SYSTEM_PORT"):
            environment.pop(name, None)
        return environment

    def arguments(self, *arguments):
        return [str(self.bundle / "crowdb-cli"), "--cluster", self.run_id, *arguments]

    def invoke(self, *arguments, endpoint=None, expected_exit=0):
        self.phase_number += 1
        started = time.monotonic()
        command = self.arguments(*arguments) if endpoint is None else [
            str(self.bundle / "crowdb-cli"), "--management-endpoint", endpoint, *arguments]
        result = subprocess.run(command, env=self.environment(),
                                capture_output=True, text=True, timeout=210)
        output = result.stdout + result.stderr
        (self.artifacts / f"client-cli-{self.phase_number}.log").write_text(redact(output))
        (self.artifacts / f"client-cli-{self.phase_number}.json").write_text(json.dumps({
            "case": self.case_name, "phase": ".".join(arguments[:2]), "execution": "host",
            "seconds": time.monotonic() - started, "exit_code": result.returncode}, indent=2))
        if result.returncode != expected_exit:
            print(output, flush=True)
            raise AssertionError(output)
        return result.stdout

