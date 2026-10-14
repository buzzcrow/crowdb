# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit host storage inputs and their common node-visible paths."""

import json
import os
import stat
from pathlib import Path

DISK_ROOT = "/opt/crowdb/data/disks"


def inputs(args):
    """Validate supplied media without opening them for writes or creating files."""
    result = []
    targets = set()
    identities = set()
    for backend, paths in (("file", getattr(args, "disk_file", [])),
                           ("block-device", getattr(args, "device", []))):
        for source in paths:
            source = Path(source)
            if not source.is_absolute() or source.is_symlink():
                raise ValueError("disk input must be an absolute non-symlink path")
            if any(character in str(source) for character in (",", ":", "\n", "\r")):
                raise ValueError("disk input path contains a mount delimiter")
            metadata = source.stat()
            correct_type = stat.S_ISREG(metadata.st_mode) if backend == "file" else stat.S_ISBLK(metadata.st_mode)
            if not correct_type:
                raise ValueError(f"{source}: expected {backend}")
            if backend == "file" and metadata.st_size == 0:
                raise ValueError(f"{source}: supplied disk file is empty")
            identity = (("file", metadata.st_dev, metadata.st_ino) if backend == "file"
                        else ("block-device", metadata.st_rdev))
            target = f"{DISK_ROOT}/{source.name}"
            if target in targets or identity in identities:
                raise ValueError(f"{source}: duplicate disk target or underlying resource")
            targets.add(target)
            identities.add(identity)
            backing_identity = (f"file:{metadata.st_dev}:{metadata.st_ino}" if backend == "file"
                                else f"device:{metadata.st_rdev}")
            result.append({"backend": backend, "source": str(source), "path": target,
                           "backing_identity": backing_identity})
    return result


def runtime_arguments(disks):
    result = []
    for disk in disks:
        if disk["backend"] == "file":
            result += ["--mount", f'type=bind,source={disk["source"]},target={disk["path"]}']
        else:
            result += ["--device", f'{disk["source"]}:{disk["path"]}:rw']
    declaration = [{key: disk[key] for key in ("backend", "path", "backing_identity")} for disk in disks]
    return [*result, "-e", "CROWDB_DISK_INPUTS=" + json.dumps(declaration, separators=(",", ":"))]


def validate_retained_inputs(root, disks):
    """Replacement may add mappings, but must retain each recorded medium."""
    path = Path(root) / "container-storage.json"
    if not path.exists():
        return
    retained = json.loads(path.read_text())
    if not isinstance(retained, dict) or retained.get("version") != 1 or not isinstance(retained.get("disks"), list):
        raise ValueError("invalid retained container storage declaration")
    current = {disk["path"]: disk for disk in disks}
    seen = set()
    for previous in retained["disks"]:
        if not isinstance(previous, dict) or not all(
                isinstance(previous.get(key), str) and previous[key]
                for key in ("backend", "source", "path", "backing_identity")):
            raise ValueError("retained disk mapping has no verified backing identity")
        target = previous["path"]
        if target in seen:
            raise ValueError("duplicate retained disk mapping")
        seen.add(target)
        if current.get(target) != previous:
            raise ValueError(f"{target}: missing or changed retained disk mapping; restore the original input")


def record_inputs(root, disks):
    """Keep the non-secret host mapping beside node state for container replacement."""
    path = Path(root) / "container-storage.json"
    value = json.dumps({"version": 1, "disks": disks}, indent=2) + "\n"
    temporary = path.with_name(f".container-storage-{os.getpid()}.json")
    try:
        with temporary.open("x") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)
