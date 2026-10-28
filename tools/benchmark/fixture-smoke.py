#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Exercise native fixture ownership and ordinary Paxos-KV data commands."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
import uuid

import fixture


def wait_stopped(processes):
    deadline = time.monotonic() + 15
    while any(fixture.process_start(pid) == start for pid, start in processes):
        if time.monotonic() >= deadline:
            raise RuntimeError(f"owned measurement processes survived shutdown: {processes}")
        time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path,
                        default=fixture.ROOT / ".crowdb-runtime/artifacts/measurement-fixture" / uuid.uuid4().hex)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ, CROWDB_REGRESSION_FIXTURE_BIN=str(args.worker.resolve()),
                       REGRESSION_CLI=str(args.client.resolve()))

    def command(state, *arguments, owner=None, succeeds=True):
        argv = [sys.executable, str(fixture.ROOT / "tools/benchmark/fixture.py"), "--state", str(state)]
        if owner is not None:
            argv += ["--owner-pid", str(owner)]
        result = subprocess.run([*argv, *arguments], env=environment, capture_output=True, text=True, timeout=60)
        print(result.stdout, end="", flush=True)
        print(result.stderr, end="", file=sys.stderr, flush=True)
        with (args.artifacts / "commands.log").open("a") as log:
            log.write(f"{arguments}: exit={result.returncode}\n{result.stdout}{result.stderr}")
        if (result.returncode == 0) != succeeds:
            raise RuntimeError(f"measurement command returned {result.returncode}: {arguments}")
        return result.stdout

    def setup(state, owner):
        command(state, "setup", "--", "--kind", "kv", "--nodes", "3", "--kv-backend", "mem-block",
                "--wal-backend", "mem-block", "--metrics-interval", "1", owner=owner)
        value = fixture.descriptor(state)
        servers = tomllib.loads(state.read_text())["server"]
        processes = [(server["pid"], fixture.process_start(server["pid"])) for server in servers]
        assert len(processes) == 3 and all(start for _, start in processes)
        return value, [(value["pid"], value["start"]), *processes]

    # This process is deliberately unrelated to either fixture's receipt.
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    state = args.artifacts / "explicit.toml"
    owner = None
    try:
        value, owned = setup(state, os.getpid())
        request = urllib.request.Request(value["endpoint"] + "/_fixture/control",
            data=b'{"action":"stop"}', headers={"Content-Type": "application/json",
                                                "X-Crowdb-Fixture-Owner": "foreign"})
        try:
            fixture.HTTP.open(request, timeout=5)
        except urllib.error.HTTPError as error:
            assert error.code == 403, error.read().decode()
        else:
            raise AssertionError("foreign control token was accepted")
        command(state, "reset", "--group", "0", succeeds=False)
        command(state, "client", "--", "paxos-kv", "group", "add", "-s", "0", "-g", "1", "-n", "1,2,3")
        command(state, "client", "--", "paxos-kv", "data", "put", "-s", "0", "-g", "1",
                "-k", "fixture-smoke-key", "-v", "fixture-smoke-value")
        fetched = command(state, "client", "--", "paxos-kv", "data", "get", "-s", "0", "-g", "1",
                          "-k", "fixture-smoke-key")
        assert "fixture-smoke-value" in fetched, fetched
        scanned = command(state, "client", "--", "paxos-kv", "data", "scan", "-s", "0", "-g", "1",
                          "--prefix", "fixture-smoke-")
        assert "fixture-smoke-key" in scanned and "fixture-smoke-value" in scanned, scanned
        # A management fixture is not an implicit benchmark deployment.
        # Container load/report coverage belongs to the container benchmark tests.
        command(state, "client", "--", "bench", "paxos-kv", "write", succeeds=False)
        command(state, "client", "--", "bench", "paxos-kv", "invalid", succeeds=False)
        result = json.loads(command(state, "reset", "--group", "1"))
        assert result["new_leader"] and result["wiped_nodes"] == 3, result
        command(state, "stop")
        wait_stopped(owned)
        assert unrelated.poll() is None, "fixture stopped an unrelated process"

        owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
        abandoned = args.artifacts / "owner-exit.toml"
        _, owned = setup(abandoned, owner.pid)
        owner.terminate()
        owner.wait(timeout=5)
        wait_stopped(owned)
        stopped = json.loads(abandoned.with_suffix(".fixture-stopped.json").read_text())
        assert stopped["ok"], stopped
        assert unrelated.poll() is None, "owner-exit cleanup stopped an unrelated process"
        print(f"PASS: native fixture ownership and Paxos-KV data commands; artifacts: {args.artifacts}")
    finally:
        command(state, "stop")
        if owner is not None and owner.poll() is None:
            owner.terminate()
            owner.wait(timeout=5)
        unrelated.terminate()
        unrelated.wait(timeout=5)


if __name__ == "__main__":
    main()
