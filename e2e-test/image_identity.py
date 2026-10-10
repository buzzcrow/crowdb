# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Allow client-only revisions to reuse an image with identical server sources."""

import re
import subprocess
import tomllib
from pathlib import Path

# Conservatively include manifests, dependencies, build tooling and runtime
# assets. Component tests are host clients and do not enter the release image.
SERVER_PATHS = (
    "app", "lib", "container", "tools", "cmake", ".cargo",
    "Cargo.toml", "Cargo.lock", "pixi.toml", "pixi.lock", "CMakeLists.txt",
    ":(glob,exclude)**/tests/**",
)


def verify_server_sources(root, revision):
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("release image needs a full source revision label")

    def git(*arguments):
        return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()

    try:
        git("rev-parse", "--verify", revision + "^{commit}")
        changed = git("diff", "--name-only", revision, "--", *SERVER_PATHS)
        untracked = git("ls-files", "--others", "--exclude-standard", "--", *SERVER_PATHS)
    except subprocess.CalledProcessError as error:
        raise ValueError("image source revision cannot be verified locally; rebuild the shared OCI image") from error
    runtime_changes = []
    for path in changed.splitlines():
        current = Path(root, path)
        if path.endswith("Cargo.toml") and current.is_file():
            before = tomllib.loads(git("show", f"{revision}:{path}"))
            after = tomllib.loads(current.read_text())
            before.pop("test", None)
            after.pop("test", None)
            if before == after:
                continue
        runtime_changes.append(path)
    changed = "\n".join(runtime_changes)
    if changed or untracked:
        raise ValueError("release server sources differ from the image; rebuild the shared OCI image:\n" +
                         "\n".join(filter(None, [changed, untracked])))
    return {"image_revision": revision, "client_revision": git("rev-parse", "HEAD"),
            "server_sources_match": True}
