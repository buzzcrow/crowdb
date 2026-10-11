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
            diagnostic = result.stdout + result.stderr
            assert expected in diagnostic, f"{case}: {diagnostic}"
            results.append({"case": case, "seconds": time.monotonic() - started,
                            "exit_code": result.returncode, "status": "pass"})
        finally:
            # Never remove a resource that a different owner replaced.
            labels = json.loads(docker("inspect", "--format", "{{json .Config.Labels}}", name))
            if labels.get(LABEL) != name:
                raise AssertionError(f"startup container ownership changed: {name}")
            docker("rm", "-f", name)

    check("unknown-service", ["server", "unknown"], "Unknown server service")
    check("chunk-kv-missing-config", ["server", "chunk-kv", "--config", "/opt/crowdb/etc/missing.toml",
          "--log-dir", "/opt/crowdb/data/log/chunk-kv", "--log"], "failed to load configuration")
    check("managed-root", ["server", "kv"], "requires a separate data root")
    profile = root / "invalid-profile.toml"
    profile.write_text("version = [\n")
    check("invalid-profile", [], "Error: Decode", mounts=(
        f"type=bind,source={profile.resolve()},target=/opt/crowdb/etc/selected.toml,readonly",),
        environment=("CROWDB_PROFILE_PATH=/opt/crowdb/etc/selected.toml",))
    source = Path(__file__).resolve().parents[1] / "container/single-node-container/profile.toml"
    for case, body, expected in (
            ("unknown-selected-service", source.read_text().replace('id = "web"', 'id = "unknown"'), "unknown release service"),
            ("missing-selected-dependency", source.read_text().replace('dependencies = ["kv"]', 'dependencies = []'), "release service dependency is absent")):
        profile = root / f"{case}.toml"
        profile.write_text(body)
        check(case, [], expected, mounts=(
            f"type=bind,source={profile.resolve()},target=/opt/crowdb/etc/selected.toml,readonly",),
            environment=("CROWDB_PROFILE_PATH=/opt/crowdb/etc/selected.toml", "CROWDB_STARTUP_MODE=single"))
        for path in ("node-identity", "ssh", "bootstrap", "kv"):
            if (root / case / path).exists():
                raise AssertionError(f"{case}: invalid selection created {path}")
    (root / "timing.json").write_text(json.dumps(results, indent=2))
