# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit CLI attachment and bounded, redacted host invocation artifacts."""

import json
import os
import subprocess
import time
import urllib.error
import urllib.request

from fixture import redact


class CliClient:
    def diagnostics(self):
        # Export cached counters before teardown, including a failed probe's
        # latency. These reads never issue dependency health probes.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for index, address in enumerate(self.addresses):
            try:
                port = self.access_listeners(index)["s3_port"]
                with opener.open(f"http://{address}:{port}/_crowdb/metrics", timeout=2) as response:
                    metrics = response.read(1024 * 1024 + 1)
                if len(metrics) > 1024 * 1024:
                    raise ValueError("Access metrics exceeded the diagnostic limit")
                (self.artifacts / f"access-metrics-{index}.prom").write_text(redact(metrics.decode()))
            except (OSError, ValueError, urllib.error.HTTPError) as error:
                (self.artifacts / f"access-metrics-{index}.error").write_text(redact(str(error)))
        super().diagnostics()

    def environment(self):
        environment = dict(os.environ, CROWDB_RUNTIME_ROOT=str(self.root), NO_PROXY="*", no_proxy="*")
        for name in ("CROWDB_CLUSTER", "CROWDB_SYSTEM_IP", "CROWDB_SYSTEM_PORT"):
            environment.pop(name, None)
        environment.update(getattr(self, "extra_environment", {}))
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
            "seconds": time.monotonic() - started, "exit_code": result.returncode,
            "expected_exit_code": expected_exit}, indent=2))
        if result.returncode != expected_exit:
            print(output, flush=True)
            raise AssertionError(output)
        return result.stdout
