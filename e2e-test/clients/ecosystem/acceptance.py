# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Exact cross-engine data and selected-snapshot handoff survive recreation."""

import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
CLIENT = Path(__file__).resolve().parent


def execute(command, env, output, timeout=900):
    with output.open("w") as log:
        child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=timeout)
            if code:
                raise RuntimeError(f"ecosystem client exited {code}; retained log {output}")
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)


def verify(cluster, data, all_engines):
    private = data / "private"
    private.mkdir(mode=0o700)
    results = data / "results"
    results.mkdir()
    credentials = cluster.command("exec", cluster.servers[0], "crowdb-monitor", "credentials", "show", "--format", "env")
    credential_file = private / "credentials.env"
    credential_file.touch(mode=0o600)
    credential_file.write_text(credentials)
    token = next(line.removeprefix("ICEBERG_TOKEN=") for line in credentials.splitlines() if line.startswith("ICEBERG_TOKEN="))
    if not token:
        raise ValueError("formal node did not provide an Iceberg token")
    env = os.environ.copy()
    engine_env, trino = cluster.engine_environment() if all_engines else ({}, "")
    env.update(engine_env, ICEBERG_TOKEN=token,
               CROWDB_PREVIEW_ICEBERG_URI=f"http://{cluster.addresses[0]}:9092",
               CROWDB_ECOSYSTEM_RESULTS=str(results), CROWDB_ECOSYSTEM_STATE=str(data / "state.json"))
    prefix = ["pixi", "run", "--as-is", "-e", "iceberg-ecosystem"]
    timings = {}

    def client(label, *arguments):
        started = time.monotonic()
        try:
            execute([*prefix, *arguments], env, private / f"{label}.log")
        finally:
            timings[label] = time.monotonic() - started
            execute([*prefix, "python", str(CLIENT / "diagnostics.py"), str(data), label], env, private / "redaction.log", timeout=60)
            (results / "timing.json").write_text(json.dumps(timings, indent=2))
        print(f"Ecosystem {label}: pass ({timings[label]:.3f}s)", flush=True)

    def python(label, phase):
        client(label, "python", str(CLIENT / "python_client.py"), phase)

    def engine(label, name, phase, *inputs):
        client(label, "bash", str(CLIENT / "engine.sh"), name, phase, *inputs)

    try:
        for label, phase in (("python-write","write"),("python-before","verify")):
            python(label, phase)
        client("duckdb", "python", str(CLIENT / "duckdb_client.py"))
        if all_engines:
            engine("spark-mutate", "spark", "mutate")
            engine("flink-mutate", "flink", "mutate")
            python("python-capture", "capture")
            snapshot = str(json.loads((data / "state.json").read_text())["engine_snapshot"])
            python("python-handoff-before", "handoff")
            engine("spark-before", "spark", "verify", snapshot)
            engine("flink-before", "flink", "verify")
            client("trino-before", "python", str(CLIENT / "trino_client.py"), trino, str(private))
        cluster.restart()
        if cluster.command("exec", cluster.servers[0], "crowdb-monitor", "credentials", "show", "--format", "env") != credentials:
            raise AssertionError("ecosystem credentials changed across persisted-data recreation")
        python("python-after", "verify")
        client("duckdb-after", "python", str(CLIENT / "duckdb_client.py"), *(("final",) if all_engines else ()))
        if all_engines:
            python("python-handoff-after", "handoff")
            engine("spark-after", "spark", "verify", snapshot)
            engine("flink-after", "flink", "verify")
            client("trino-after", "python", str(CLIENT / "trino_client.py"), trino, str(private))
    finally:
        for service in cluster.containers:
            try:
                (private / ("trino.log" if service == trino else "container.log")).write_text(cluster.command("logs", service))
            except RuntimeError:
                # A completed Trino client already removed its own engine;
                # existing stage logs retain its diagnostics.
                if service != trino:
                    raise
        execute([*prefix, "python", str(CLIENT / "diagnostics.py"), str(data)], env, private / "redaction-final.log", timeout=60)
