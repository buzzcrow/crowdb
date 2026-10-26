# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Capture complete client output while reporting the active case periodically."""

import subprocess
import time


def run_client_process(arguments, env, log, timeout, label, heartbeat_seconds=30):
    started = time.monotonic()
    print(f"[CLIENT START] {label} timeout={timeout}s log={log}", flush=True)
    with log.open("w") as output:
        with subprocess.Popen(arguments, env=env, stdout=output, stderr=subprocess.STDOUT,
                              text=True) as process:
            try:
                while True:
                    remaining = timeout - (time.monotonic() - started)
                    try:
                        process.wait(timeout=max(0, min(heartbeat_seconds, remaining)))
                        break
                    except subprocess.TimeoutExpired:
                        elapsed = time.monotonic() - started
                        if elapsed >= timeout:
                            raise subprocess.TimeoutExpired(arguments, timeout) from None
                        print(f"[CLIENT RUNNING] {label} elapsed={elapsed:.1f}s timeout={timeout}s", flush=True)
            except BaseException as error:
                process.kill()
                process.wait()
                output.flush()
                if isinstance(error, subprocess.TimeoutExpired):
                    error.output = log.read_text()
                raise
    elapsed = time.monotonic() - started
    print(f"[CLIENT END] {label} exit_code={process.returncode} elapsed={elapsed:.2f}s", flush=True)
    return subprocess.CompletedProcess(arguments, process.returncode, log.read_text(), "")
