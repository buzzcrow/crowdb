#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Build an audited stdexec include overlay without modifying the submodule."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def prepare(source: Path, output: Path) -> None:
    here = Path(__file__).resolve().parent
    patch = here / "stdexec-pool.patch"
    expected = json.loads((here / "stdexec-pool-base.json").read_text())
    for name, digest in expected.items():
        if hashlib.sha256((source / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"stdexec pool baseline changed: {name}; review the allocator patch")
    fingerprint = hashlib.sha256(patch.read_bytes())
    for path in sorted(source.rglob("*")):
        if path.is_file():
            fingerprint.update(str(path.relative_to(source)).encode())
            fingerprint.update(path.read_bytes())
    signature = fingerprint.hexdigest()
    stamp = output / "stamp"
    if stamp.is_file() and stamp.read_text() == signature:
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stdexec-pool-", dir=output.parent) as temporary:
        staging = Path(temporary) / "overlay"
        shutil.copytree(source, staging / "include")
        subprocess.run(["git", "apply", "--check", "--unidiff-zero", str(patch)], cwd=staging, check=True)
        subprocess.run(["git", "apply", "--unidiff-zero", str(patch)], cwd=staging, check=True)
        (staging / "stamp").write_text(signature)
        if output.exists():
            shutil.rmtree(output)
        staging.rename(output)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: prepare_stdexec.py VENDOR_INCLUDE OUTPUT")
    prepare(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve())
