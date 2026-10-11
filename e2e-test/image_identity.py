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
    # The staging script does not package this operator documentation.
    ":(exclude)container/single-node-container/README.md",
)



def production_manifest(manifest):
    result = dict(manifest)
    result.pop("test", None)
    result.pop("dev-dependencies", None)
    return result


def lock_matches(before, after, manifests):
    def packages(lock):
        return {(p["name"], p["version"], p.get("source")): p
                for p in lock["package"] if p["name"] != "e2e-test"}
    old, new = packages(before), packages(after)
    if old.keys() != new.keys():
        return False
    for key, previous in old.items():
        current = new[key]
        if previous == current:
            continue
        if {k: v for k, v in previous.items() if k != "dependencies"} != {
                k: v for k, v in current.items() if k != "dependencies"}:
            return False
        pair = manifests.get(key[0])
        if not pair or production_manifest(pair[0]) != production_manifest(pair[1]):
            return False
        dev, production = set(), set()
        for manifest in pair:
            for alias, specification in manifest.get("dev-dependencies", {}).items():
                dev.add(specification.get("package", alias) if isinstance(specification, dict) else alias)
            sections = [manifest, *manifest.get("target", {}).values()]
            for sections_for_target in sections:
                for section in ("dependencies", "build-dependencies"):
                    for alias, specification in sections_for_target.get(section, {}).items():
                        production.add(specification.get("package", alias) if isinstance(specification, dict) else alias)
        dev -= production
        changed = set(previous.get("dependencies", ())) ^ set(current.get("dependencies", ()))
        if any(dependency.split()[0] not in dev for dependency in changed):
            return False
    return {k: v for k, v in before.items() if k != "package"} == {
        k: v for k, v in after.items() if k != "package"}


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
    manifests = {}
    for path in changed.splitlines():
        if path.endswith("Cargo.toml") and Path(root, path).is_file():
            pair = (tomllib.loads(git("show", f"{revision}:{path}")),
                    tomllib.loads(Path(root, path).read_text()))
            name = pair[0].get("package", {}).get("name")
            if name:
                manifests[name] = pair
    runtime_changes = []
    for path in changed.splitlines():
        current = Path(root, path)
        if path == "Cargo.lock" and current.is_file():
            before = tomllib.loads(git("show", f"{revision}:{path}"))
            after = tomllib.loads(current.read_text())
            if lock_matches(before, after, manifests):
                continue
        if path.endswith("Cargo.toml") and current.is_file():
            before = tomllib.loads(git("show", f"{revision}:{path}"))
            after = tomllib.loads(current.read_text())
            if production_manifest(before) == production_manifest(after):
                continue
        runtime_changes.append(path)
    changed = "\n".join(runtime_changes)
    if changed or untracked:
        raise ValueError("release server sources differ from the image; rebuild the shared OCI image:\n" +
                         "\n".join(filter(None, [changed, untracked])))
    return {"image_revision": revision, "client_revision": git("rev-parse", "HEAD"),
            "server_sources_match": True}
