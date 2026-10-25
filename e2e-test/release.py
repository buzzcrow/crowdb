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

    def check(case, arguments, expected, mounts=(), environment=(), entrypoint=None):
        name = "crowdb-e2e-startup-" + uuid.uuid4().hex
        data = root / case
        data.mkdir()
        if case == "managed-root":
            (data / "accepted-node.json").write_text("{}")
        command = ["docker", "run", "--name", name, "--label", f"{LABEL}={name}",
                   "--network", "none", "--cpu-shares", "1024", "--env", "TOKIO_WORKER_THREADS=1", "--memory", "512m",
                   "--pids-limit", "256", "--mount",
                   f"type=bind,source={data.resolve()},target=/opt/crowdb/data"]
        for mount in mounts:
            command.extend(["--mount", mount])
        for value in environment:
            command.extend(["--env", value])
        if entrypoint is not None:
            command.extend(["--entrypoint", entrypoint])
        started = time.monotonic()
        try:
            result = subprocess.run(command + [image, *arguments], capture_output=True,
                                    text=True, timeout=30, check=False)
            (root / f"{case}.log").write_text(result.stdout + result.stderr)
            assert result.returncode != 0, f"{case}: invalid startup succeeded"
            diagnostic = result.stdout + result.stderr
            accepted = (expected,) if isinstance(expected, str) else expected
            assert any(text in diagnostic for text in accepted), f"{case}: {diagnostic}"
            results.append({"case": case, "seconds": time.monotonic() - started,
                            "exit_code": result.returncode, "status": "pass"})
        finally:
            # Never remove a resource that a different owner replaced.
            labels = json.loads(docker("inspect", "--format", "{{json .Config.Labels}}", name))
            if labels.get(LABEL) != name:
                raise AssertionError(f"startup container ownership changed: {name}")
            docker("rm", "-f", name)

    check("unknown-service", ["server", "unknown"], "Unknown server service")
    check("iceberg-missing-environment", ["iceberg"], "Error: NotPresent",
          entrypoint="/opt/crowdb/bin/crowdb-access-server")
    for case, writer in (("missing", None), ("short", "short"),
                         ("reader", "r" * 32), ("manager", "m" * 32), ("clearer", "c" * 32)):
        environment = ("CROWDB_MANAGEMENT_SEEDS=127.0.0.1:1",
                       "CROWDB_ICEBERG_READ_TOKEN=" + "r" * 32,
                       "CROWDB_ICEBERG_MANAGE_TOKEN=" + "m" * 32,
                       "CROWDB_ICEBERG_CLEAR_TOKEN=" + "c" * 32)
        if writer is not None:
            environment += ("CROWDB_ICEBERG_WRITE_TOKEN=" + writer,)
        expected = ("CROWDB_ICEBERG_WRITE_TOKEN must be set" if writer is None else
                    "invalid or oversized text field")
        check("iceberg-writer-" + case, ["iceberg", "serve"], expected,
              environment=environment, entrypoint="/opt/crowdb/bin/crowdb-access-server")
    old_config = root / "old-web.toml"
    old_config.write_text("[[rack]]\nid = 1\n")
    check("web-old-mixed-config", ["server", "web", "--config", "/opt/crowdb/etc/old-web.toml"],
          ("version", "unknown field"), mounts=(
              f"type=bind,source={old_config.resolve()},target=/opt/crowdb/etc/old-web.toml,readonly",))
    managed_config = root / "managed-web.toml"
    managed_config.write_text("version = 1\nmode = 'docker'\nbind = '127.0.0.1'\nport = 9090\n"
        "group0_management_seeds = ['http://127.0.0.1:10000']\nui_root = '/opt/crowdb/ui'\n"
        "monitor_status = '/opt/crowdb/run/status/monitor.json'\nlog_dir = '/opt/crowdb/data/log/web'\n"
        "log_max_file_mb = 30\nlog_max_files = 5\n")
    registry = root / "registry.toml"
    registry.write_text("version = 1\n")
    check("web-managed-registry", ["server", "web", "--config", "/opt/crowdb/etc/managed-web.toml",
          "--registry", "/opt/crowdb/etc/registry.toml"], "docker web does not accept --registry", mounts=(
              f"type=bind,source={managed_config.resolve()},target=/opt/crowdb/etc/managed-web.toml,readonly",
              f"type=bind,source={registry.resolve()},target=/opt/crowdb/etc/registry.toml,readonly"))
    check("chunk-kv-missing-config", ["server", "chunk-kv", "--config", "/opt/crowdb/etc/missing.toml",
          "--log-dir", "/opt/crowdb/data/log/chunk-kv", "--log"], "failed to load configuration")
    check("managed-root", ["server", "kv"], "requires a separate data root")
    profile = root / "invalid-profile.toml"
    profile.write_text("version = [\n")
    check("invalid-profile", [], "failed to decode deployment profile", mounts=(
        f"type=bind,source={profile.resolve()},target=/opt/crowdb/etc/selected.toml,readonly",),
        environment=("CROWDB_PROFILE_PATH=/opt/crowdb/etc/selected.toml",))
    repository = Path(__file__).resolve().parents[1]
    source = (repository / "container/oci-image/node-profile.toml").read_text()
    catalog_error = "node profile requires the complete service catalog without preset topology or media"
    for case, body, expected in (
            ("unknown-selected-service", source.replace('[[services]]\nid = "web"',
                '[[services]]\nid = "unknown"', 1), catalog_error),
            ("missing-selected-dependency", source.replace('dependencies = ["kv"]',
                'dependencies = ["missing"]', 1), "service diskdb has an invalid dependency"),
            ("preset-selected-topology", (repository / "container/single-node-container/profile.toml").read_text(),
                catalog_error)):
        profile = root / f"{case}.toml"
        profile.write_text(body)
        check(case, [], expected, mounts=(
            f"type=bind,source={profile.resolve()},target=/opt/crowdb/etc/selected.toml,readonly",),
            environment=("CROWDB_PROFILE_PATH=/opt/crowdb/etc/selected.toml",))
        for path in ("node-identity", "ssh", "bootstrap", "kv"):
            if (root / case / path).exists():
                raise AssertionError(f"{case}: invalid selection created {path}")
    (root / "timing.json").write_text(json.dumps(results, indent=2))
