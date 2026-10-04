#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Owned host-native managed console acceptance, separate from Docker packaging."""

import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time


REPO = Path(__file__).resolve().parents[2]
UI = REPO / "app/crowdb-web/ui"
SOURCE = REPO / "container/single-node-container"


def allocate(service, count=1, instance=0):
    result = subprocess.run(
        ["pixi", "run", str(REPO / "target/debug/crowdb-cli"), "port-alloc",
         "--owner-pid", str(os.getpid()), "--service", service,
         "--count", str(count), "--instance", str(instance)],
        cwd=REPO, check=True, capture_output=True, text=True, timeout=60,
    )
    return [int(port) for port in result.stdout.split()]


def create_profile(root):
    assignments = {}
    for originals, service, instance in [
        ([10000], "kv-mgmt", 0), ([10100, 10101], "kv-listen", 0),
        ([11000], "diskdb-listen", 0), ([11100], "diskdb-http", 0),
        ([11200], "diskdb-rpc", 0), ([12100], "chunkdb-http", 0),
        ([12200], "chunkdb-rpc", 0), ([13000], "diskio-rpc", 0),
        ([15100], "chunkdb-http", 1), ([15200], "chunkdb-rpc", 1),
        ([9090], "web", 0), ([9091], "access-http", 0),
        ([9092], "access-iceberg-http", 0),
    ]:
        assignments.update(zip(originals, allocate(service, len(originals), instance)))
    pattern = re.compile(r"(?<!\d)(" + "|".join(map(str, assignments)) + r")(?!\d)")

    def render(text):
        return pattern.sub(lambda match: str(assignments[int(match[0])]),
                           text.replace("/opt/crowdb", str(root)))

    for name in ["bin", "etc/templates", "data", "run"]:
        (root / name).mkdir(parents=True, exist_ok=True)
    for binary in ["crowdb-kv-server", "crowdb-diskdb", "crowdb-chunkdb",
                   "crowdb-chunk-kv-server", "crowdb-access-server", "crowdb-monitor", "crowdb-web"]:
        source = REPO / "target/debug" / binary
        if not source.is_file():
            raise RuntimeError(f"Build native service first: {source}")
        (root / "bin" / binary).symlink_to(source)
    diskio = REPO / "app/crowdb-diskio/build/crowdb-diskio"
    if not diskio.is_file():
        raise RuntimeError(f"Build native DiskIO first: {diskio}")
    (root / "bin/crowdb-diskio").symlink_to(diskio)
    (root / "ui").symlink_to(UI / "dist", target_is_directory=True)
    for template in (SOURCE / "templates").glob("*.toml"):
        (root / "etc/templates" / template.name).write_text(render(template.read_text()))
    profile = root / "profile.toml"
    profile.write_text(render((SOURCE / "profile.toml").read_text()))
    return profile, assignments[9090]


def await_ready(process, root):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Monitor exited before readiness: {process.returncode}")
        status = root / "run/status/monitor.json"
        if status.is_file():
            observation = json.loads(status.read_text())
            if observation["phase"] == "ready":
                return
            if observation["phase"] in ["failed", "draining"]:
                raise RuntimeError(f"Monitor entered {observation['phase']} before readiness")
        time.sleep(0.1)
    raise RuntimeError("Managed native services did not become ready within startup budget")


def main():
    runtime = REPO / ".crowdb-runtime/test-data"
    runtime.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="console-managed-", dir=runtime) as temporary:
        root = Path(temporary)
        profile, port = create_profile(root)
        started = time.monotonic()
        failed = True
        with (root / "monitor.log").open("w") as log:
            process = subprocess.Popen(
                ["pixi", "run", str(root / "bin/crowdb-monitor"), "run", "--profile", str(profile)],
                cwd=REPO, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            try:
                await_ready(process, root)
                print(f"[PHASE] managed native readiness: {time.monotonic() - started:.2f}s", flush=True)
                status = json.loads((root / "run/status/monitor.json").read_text())
                env = dict(os.environ, CROWDB_WEB_E2E_BASE_URL=f"http://127.0.0.1:{port}",
                           CROWDB_NATIVE_KV_PID=str(status["services"]["kv"]["pid"]))
                subprocess.run(["pixi", "run", "npx", "playwright", "test",
                                "--config=e2e/managedNative.config.ts"],
                               cwd=UI, env=env, check=True)
                failed = False
            finally:
                cleanup = time.monotonic()
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                # No process from this isolated session may survive teardown.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                print(f"[PHASE] managed native teardown: {time.monotonic() - cleanup:.2f}s", flush=True)
                if failed:
                    artifact = REPO / ".crowdb-runtime/artifacts" / f"console-managed-{os.getpid()}"
                    artifact.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(root / "monitor.log", artifact)
                    if (root / "data/log").is_dir():
                        shutil.copytree(root / "data/log", artifact / "services", dirs_exist_ok=True)
                    print(f"[PHASE] managed failure logs: {artifact}", flush=True)
                    print((root / "monitor.log").read_text(), flush=True)


if __name__ == "__main__":
    main()
