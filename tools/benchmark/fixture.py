#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit native measurement fixture; never an installed product command."""

import argparse
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.request
import urllib.error

ROOT = Path(__file__).resolve().parents[2]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
BENCHMARKS = {"kv", "diskdb", "chunkdb", "chunkio", "s3"}


class FixtureStopped(ValueError):
    pass


def process_start(pid):
    try:
        if sys.platform.startswith("linux"):
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            return None if fields[0] == "Z" else fields[19]
        value = subprocess.check_output(["ps", "-p", str(pid), "-o", "lstart="], text=True).strip()
        return value or None
    except (OSError, subprocess.SubprocessError):
        return None


def descriptor(state):
    path = state.with_suffix(".fixture.json")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536:
        raise ValueError("fixture descriptor must be a bounded private regular file")
    value = json.loads(path.read_text())
    if process_start(value["pid"]) != value["start"]:
        raise FixtureStopped("measurement fixture is stopped; start an explicit fixture first")
    return value


def control(state, action):
    value = descriptor(state)
    request = urllib.request.Request(value["endpoint"] + "/_fixture/control",
        data=json.dumps(action).encode(), headers={"Content-Type": "application/json",
                                                  "X-Crowdb-Fixture-Owner": value["token"]})
    try:
        with HTTP.open(request, timeout=180) as reply:
            result = json.load(reply)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"fixture control failed {error.code}: " + error.read().decode()) from error
    if result.get("ok") is not True:
        raise ValueError("fixture control did not confirm the requested action")
    return value, result


def setup(args):
    if args.arguments[:1] == ["--"]:
        args.arguments = args.arguments[1:]
    if args.state.with_suffix(".fixture.json").exists():
        try:
            descriptor(args.state)
        except FixtureStopped:
            pass
        else:
            control(args.state, {"action": "setup", "arguments": args.arguments})
            return
    if args.owner_pid is None or process_start(args.owner_pid) is None:
        raise ValueError("fixture setup requires --owner-pid for the live measurement owner")
    binary = Path(os.environ.get("CROWDB_REGRESSION_FIXTURE_BIN",
                                 ROOT / "target/release/examples/regression-fixture"))
    if not binary.is_file():
        raise ValueError("build the test fixture with: pixi run cargo build --release -p "
                         "crowdb-console-shared --example regression-fixture --features test-util")
    args.state.parent.mkdir(parents=True, exist_ok=True)
    if args.state.is_symlink():
        raise ValueError("fixture state cannot be a symlink")
    root = Path(tempfile.mkdtemp(prefix=args.state.stem + "-fixture-", dir=args.state.parent))
    with (root / "controller.log").open("w") as log:
        # The worker requires a fresh root; its diagnostic output stays alongside it.
        empty = root / "owned"
        empty.mkdir(mode=0o700)
        environment = dict(os.environ)
        for setting in ("LD_PRELOAD", "ASAN_OPTIONS"):
            override = environment.pop("CROWDB_FIXTURE_" + setting, None)
            if override is not None:
                environment[setting] = override
        worker = subprocess.Popen([str(binary), "--root", str(empty), "--state", str(args.state),
            "--owner-pid", str(args.owner_pid), "--", *args.arguments], stdout=log, stderr=log,
            start_new_session=True, env=environment)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            raise RuntimeError(f"fixture setup exited {worker.returncode}:\n" + (root / "controller.log").read_text())
        try:
            value = descriptor(args.state)
            if value["pid"] == worker.pid:
                print(f"Measurement fixture ready; artifacts: {root}", file=sys.stderr)
                return
        except (FileNotFoundError, ValueError):
            pass
        time.sleep(0.05)
    worker.terminate()
    worker.wait(timeout=60)
    raise TimeoutError("explicit measurement fixture did not become ready within 180 seconds")


def run(args):
    args.state = args.state.resolve(strict=False) if not args.state.is_symlink() else args.state.absolute()
    if args.action == "setup":
        setup(args)
    elif args.action == "reset":
        _, result = control(args.state, {"action": "reset", "store": args.store, "group": args.group,
                                        "restart_storage": args.restart_storage})
        print(json.dumps(result["result"], indent=2))
    elif args.action == "inspect":
        _, result = control(args.state, {"action": "inspect"})
        print(json.dumps(result["result"], indent=2))
    elif args.action == "stop":
        try:
            value, _ = control(args.state, {"action": "stop"})
        except (FileNotFoundError, FixtureStopped):
            return 0
        deadline = time.monotonic() + 60
        while process_start(value["pid"]) == value["start"]:
            if time.monotonic() >= deadline:
                raise TimeoutError("owned measurement fixture did not stop within 60 seconds")
            time.sleep(0.05)
        stopped = json.loads(args.state.with_suffix(".fixture-stopped.json").read_text())
        if (stopped.get("pid"), stopped.get("start"), stopped.get("ok")) != (value["pid"], value["start"], True):
            raise RuntimeError(f"owned fixture cleanup did not complete: {stopped.get('error')}")
    elif args.action == "client":
        value = descriptor(args.state)
        environment = dict(os.environ, CROWDB_RUNTIME_ROOT=str(args.state.parent / "client-runtime"))
        environment.pop("CROWDB_CLUSTER", None)
        arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
        logs = None
        if arguments[:1] == ["bench"] and len(arguments) > 1 and arguments[1] in BENCHMARKS:
            logs = args.state.parent / "client-runtime/artifacts/cli" / ("bench-" + arguments[1])
            if logs.exists():
                shutil.rmtree(logs)
        cli = os.environ.get("REGRESSION_CLI", str(ROOT / "target/release/crowdb-cli"))
        code = subprocess.call([cli, "--management-endpoint", value["endpoint"], *arguments], env=environment)
        if logs is not None:
            if logs.is_dir():
                # Product CLI owns its latest-run directory. Measurements retain
                # each completed invocation before the next one replaces it.
                retained = args.state.parent / "measurements" / (logs.name + "-" + uuid.uuid4().hex)
                shutil.copytree(logs, retained)
        return code
    elif args.action in ("target", "root"):
        value = descriptor(args.state)
        print(value["endpoint" if args.action == "target" else "root"])
    elif args.action == "seed":
        value = descriptor(args.state)
        with HTTP.open(value["endpoint"] + "/api/cluster/target", timeout=5) as response:
            target = json.load(response)
        seeds = target.get("group0_management_endpoints")
        if not isinstance(seeds, list) or not seeds or not all(isinstance(seed, str) and seed for seed in seeds):
            raise ValueError("live measurement target has no Group-0 endpoints")
        print(seeds[0])
    elif args.action == "report":
        path = args.state.parent / "client-runtime/artifacts/cli/bench-kv/report.json"
        report = json.loads(path.read_text())
        if not all(key in report for key in ("total_ops", "total_errors", "duration_ms", "by_op")):
            raise ValueError("latest benchmark report is incomplete")
        print(json.dumps(report, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--owner-pid", type=int)
    subcommands = parser.add_subparsers(dest="action", required=True)
    subcommands.add_parser("setup").add_argument("arguments", nargs=argparse.REMAINDER)
    subcommands.add_parser("stop")
    subcommands.add_parser("target")
    subcommands.add_parser("root")
    subcommands.add_parser("seed")
    subcommands.add_parser("report")
    subcommands.add_parser("inspect")
    reset = subcommands.add_parser("reset")
    reset.add_argument("--store", type=int, default=0)
    reset.add_argument("--group", type=int, required=True)
    reset.add_argument("--restart-storage", action="store_true")
    subcommands.add_parser("client").add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        return run(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"measurement fixture failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
