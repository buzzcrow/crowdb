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
            result.append({"backend": backend, "source": str(source), "path": target})
    return result


def runtime_arguments(disks):
    result = []
    for disk in disks:
        if disk["backend"] == "file":
            result += ["--mount", f'type=bind,source={disk["source"]},target={disk["path"]}']
        else:
            result += ["--device", f'{disk["source"]}:{disk["path"]}:rw']
    declaration = [{"backend": disk["backend"], "path": disk["path"]} for disk in disks]
    return [*result, "-e", "CROWDB_DISK_INPUTS=" + json.dumps(declaration, separators=(",", ":"))]


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
