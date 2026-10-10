# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Check release startup boundaries using the same immutable server image."""

import json
import subprocess
import time
import uuid
from pathlib import Path

from fixture import LABEL, docker


def check_startup(image, artifacts):
    root = Path(artifacts) / "release-startup"
    root.mkdir()
    results = []

    def check(case, arguments, expected, mounts=(), environment=()):
        name = "crowdb-e2e-startup-" + uuid.uuid4().hex
        data = root / case
        data.mkdir()
        if case == "managed-root":
            (data / "accepted-node.json").write_text("{}")
        command = ["docker", "run", "--name", name, "--label", f"{LABEL}={name}",
                   "--network", "none", "--cpus", "0.5", "--memory", "512m",
                   "--pids-limit", "256", "--mount",
                   f"type=bind,source={data.resolve()},target=/opt/crowdb/data"]
        for mount in mounts:
            command.extend(["--mount", mount])
        for value in environment:
            command.extend(["--env", value])
        started = time.monotonic()
        try:
            result = subprocess.run(command + [image, *arguments], capture_output=True,
                                    text=True, timeout=30, check=False)
            (root / f"{case}.log").write_text(result.stdout + result.stderr)
            assert result.returncode != 0, f"{case}: invalid startup succeeded"
            assert expected in result.stderr, f"{case}: {result.stdout}\n{result.stderr}"
            results.append({"case": case, "seconds": time.monotonic() - started,
                            "exit_code": result.returncode, "status": "pass"})
        finally:
            # Never remove a resource that a different owner replaced.
            labels = json.loads(docker("inspect", "--format", "{{json .Config.Labels}}", name))
            if labels.get(LABEL) != name:
                raise AssertionError(f"startup container ownership changed: {name}")
            docker("rm", "-f", name)

    check("unknown-service", ["server", "unknown"], "Unknown server service")
    check("managed-root", ["server", "kv"], "requires a separate data root")
    profile = root / "invalid-profile.toml"
    profile.write_text("version = [\n")
    check("invalid-profile", [], "Error: Decode", mounts=(
        f"type=bind,source={profile.resolve()},target=/opt/crowdb/etc/selected.toml,readonly",),
        environment=("CROWDB_PROFILE_PATH=/opt/crowdb/etc/selected.toml",))
    (root / "timing.json").write_text(json.dumps(results, indent=2))
