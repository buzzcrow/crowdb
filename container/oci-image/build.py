# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Construct and validate a standard OCI image with standalone BuildKit."""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import tempfile
from pathlib import Path

from artifact import inspect
from builder import preflight, running, tool


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--platform", choices=["linux/amd64"], default="linux/amd64")
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--privileged", action="store_true")
    parser.add_argument("--registry-mirror", action="append", default=[])
    args = parser.parse_args()
    preflight(args.privileged)
    context, output = args.context.resolve(), args.output.resolve()
    content = hashlib.sha256()
    for path in sorted(context.rglob("*")):
        content.update(str(path.relative_to(context)).encode() + b"\0")
        content.update(str(path.lstat().st_mode).encode() + b"\0")
        if path.is_symlink():
            content.update(os.readlink(path).encode())
        elif path.is_file():
            with path.open("rb") as stream:
                content.update(hashlib.file_digest(stream, "sha256").digest())
    runtime_sha256 = content.hexdigest()
    revision = (context / "SOURCE_REVISION").read_text().strip()
    version = (context / "VERSION").read_text().strip()
    base = (context / "Dockerfile").read_text().splitlines()[0].split()[1]
    output.parent.mkdir(parents=True, exist_ok=True)
    cache = (args.cache or output.parent / "oci-build-cache").resolve()
    versions = {name: subprocess.check_output([tool(name), "--version"], text=True).strip()
                for name in ("buildctl", "buildkitd", "runc")}
    with tempfile.TemporaryDirectory(prefix=".crowdb-image-", dir=output.parent) as directory:
        temporary = Path(directory) / "image.tar"
        if any("@" in mirror or "://" in mirror for mirror in args.registry_mirror):
            parser.error("registry mirrors must be credential-free host/path references")
        with running(cache, args.privileged, args.registry_mirror) as address:
            command = [tool("buildctl"), "--addr", address, "build", "--frontend", "dockerfile.v0",
                       "--local", f"context={context}", "--local", f"dockerfile={context}",
                       "--opt", f"platform={args.platform}", "--opt", f"build-arg:SOURCE_REVISION={revision}",
                       "--opt", f"build-arg:PREVIEW_VERSION={version}",
                       "--opt", f"build-arg:RUNTIME_SHA256={runtime_sha256}",
                       "--output", f"type=oci,dest={temporary},oci-mediatypes=true"]
            for name in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
                value = os.environ.get(name, "")
                if "@" in value:
                    raise ValueError("credential-bearing proxy settings are not accepted")
                if value:
                    if name != "no_proxy" and "://" not in value:
                        value = "http://" + value
                    command += ["--opt", f"build-arg:{name}={value}"]
            subprocess.run(command, check=True)
        result = inspect(temporary)
        if result["labels"].get("org.opencontainers.image.revision") != revision or result["labels"].get("org.opencontainers.image.version") != version:
            raise ValueError("built image differs from staged source metadata")
        result.pop("config")
        result.update(revision=revision, version=version, base_image=base, tools=versions, runtime_sha256=runtime_sha256)
        os.replace(temporary, output)
        receipt_path = Path(directory) / "receipt.json"
        receipt_path.write_text(json.dumps(result, indent=2) + "\n")
        os.replace(receipt_path, str(output) + ".json")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    main()
