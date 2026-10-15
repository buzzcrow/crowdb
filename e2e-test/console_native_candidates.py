# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Disposable supplied files owned by the hardware browser acceptance fixture."""

import json
import os
import stat
from pathlib import Path
from console_native_observation import owned

CAPACITY = 65 * 1024 ** 2 + 512
TAIL_OFFSET = 64 * 1024 ** 2
TAIL = b"retained-physical-tail"
NAMES = ("supplied-blank-a.img", "supplied-blank-b.img", "supplied-nonblank.img")


def prepare(data):
    inputs = []
    for name in NAMES:
        path = data / "disks" / name
        with path.open("xb") as file:
            file.truncate(CAPACITY)
            file.seek(TAIL_OFFSET)
            file.write(TAIL)
            if name == NAMES[2]:
                file.seek(TAIL_OFFSET - 1)
                file.write(b"X")
            file.flush()
            os.fsync(file.fileno())
            observed = os.fstat(file.fileno())
        identity = f"file:{observed.st_dev}:{observed.st_ino}"
        inputs.append({"backend": "file", "path": f"/opt/crowdb/data/disks/{name}",
                       "backing_identity": identity, "resource_identity": identity})
    return inputs


def verify(cluster):
    owned(cluster, 0)
    proof = []
    for expected in cluster.candidate_inputs:
        path = cluster.data_paths[0] / "disks" / Path(expected["path"]).name
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            observed = os.fstat(descriptor)
            assert stat.S_ISREG(observed.st_mode) and observed.st_size == CAPACITY
            assert f"file:{observed.st_dev}:{observed.st_ino}" == expected["backing_identity"]
            assert os.pread(descriptor, len(TAIL), TAIL_OFFSET) == TAIL
            if path.name == NAMES[2]:
                assert os.pread(descriptor, 1, TAIL_OFFSET - 1) == b"X"
            proof.append({**expected, "capacity_bytes": observed.st_size, "tail_preserved": True})
        finally:
            os.close(descriptor)
    (cluster.artifacts / "supplied-media-proof.json").write_text(json.dumps(proof, indent=2))
