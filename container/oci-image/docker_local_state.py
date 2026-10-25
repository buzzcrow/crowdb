# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Private host receipts and exact Docker resource ownership for local clusters."""

import json
import math
import os
from pathlib import Path
import re
import subprocess
import stat
import uuid

CLUSTER = "org.crowdb.cluster"
OWNER = "org.crowdb.cluster.owner"
ROOT = Path(__file__).resolve().parents[2]
RECORDS = Path(os.environ.get("CROWDB_RUNTIME_ROOT", ROOT / ".crowdb-runtime")) / "persistent/oci-clusters"


class Docker:
    def call(self, *args):
        result = subprocess.run(["docker", *map(str, args)], text=True, capture_output=True, timeout=60)
        if result.returncode:
            raise RuntimeError(f"docker {args[0]} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def identity(self):
        context = self.call("context", "show")
        details = json.loads(self.call("context", "inspect", context))[0]
        return {"daemon": self.call("info", "--format", "{{.ID}}"), "context": context,
                "endpoint": os.environ.get("DOCKER_HOST") or details["Endpoints"]["docker"]["Host"]}

    def objects(self, kind, filters=()):
        args = ("ps", "-aq") if kind == "container" else (kind, "ls", "-q")
        for condition in filters:
            args += ("--filter", condition)
        ids = self.call(*args).splitlines()
        return json.loads(self.call(kind, "inspect", *ids)) if ids else []

    def owned(self, record, kind):
        filters = (f"label={CLUSTER}={record['cluster']}", f"label={OWNER}={record['owner']}")
        return [item for item in self.objects(kind, filters) if labels(item, kind).get(CLUSTER) == record["cluster"]
                and labels(item, kind).get(OWNER) == record["owner"]
                and item.get("Name", "").lstrip("/") in resource_names(record, kind)]


def labels(item, kind):
    return (item.get("Config", {}).get("Labels") if kind in ("container", "image") else item.get("Labels")) or {}


def check_daemon(docker, record):
    if docker.identity() != record["runtime"]:
        raise ValueError("Docker endpoint/context differs from the retained cluster receipt")


def record_path(cluster, directory=RECORDS):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,47}", cluster):
        raise ValueError("cluster must be 1–48 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit")
    if any(parent.is_symlink() for parent in (directory, *directory.parents)):
        raise ValueError("cluster receipt directory must not be a symlink")
    return directory / (cluster + ".json")


def load(cluster, directory=RECORDS):
    path = record_path(cluster, directory)
    if path.is_symlink():
        raise ValueError("cluster receipt must not be a symlink")
    if not path.exists():
        return None
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077 or metadata.st_size > 1024 * 1024:
        raise ValueError("cluster receipt must be a bounded private regular file")
    record = json.loads(path.read_text())
    if record.get("version") != 1 or record.get("cluster") != cluster:
        raise ValueError("invalid cluster receipt")
    required = {"version", "cluster", "owner", "runtime", "nodes", "port_base", "image", "image_id", "cpus", "memory_mib"}
    if set(record) != required or not isinstance(record["runtime"], dict):
        raise ValueError("invalid cluster receipt fields")
    owner = uuid.UUID(record["owner"])
    if owner.int == 0 or str(owner) != record["owner"]:
        raise ValueError("invalid cluster receipt owner")
    if (set(record["runtime"]) != {"daemon", "context", "endpoint"}
            or not all(isinstance(value, str) and value for value in record["runtime"].values())
            or record["nodes"] not in (1, 3) or not isinstance(record["port_base"], int)
            or not 1 <= record["port_base"] <= 65512
            or not isinstance(record["cpus"], (float, int)) or not math.isfinite(record["cpus"]) or record["cpus"] <= 0
            or not isinstance(record["memory_mib"], int) or record["memory_mib"] < 512
            or not all(isinstance(record[key], str) and record[key] for key in ("image", "image_id"))):
        raise ValueError("invalid retained cluster inputs")
    return record


def seal(record, directory=RECORDS):
    path = record_path(record["cluster"], directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = directory / ("." + uuid.uuid4().hex)
    try:
        with open(temporary, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
            json.dump(record, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except FileExistsError:
            if load(record["cluster"], directory) != record:
                raise ValueError("another operation already accepted different cluster inputs") from None
    finally:
        temporary.unlink(missing_ok=True)


def resource_names(record, kind):
    prefix = "crowdb-" + record["cluster"]
    if kind == "network":
        return {prefix}
    suffix = "-data" if kind == "volume" else ""
    return {f"{prefix}-{index}{suffix}" for index in range(1, record["nodes"] + 1)}


def require_owned(item, record, kind):
    expected = {CLUSTER: record["cluster"], OWNER: record["owner"]}
    if (any(labels(item, kind).get(key) != value for key, value in expected.items())
            or item.get("Name", "").lstrip("/") not in resource_names(record, kind)):
        raise ValueError(f"refusing foreign {kind} ownership")
