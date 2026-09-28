#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Prepare a versioned release and dispatch the verified container workflow.

Run through Pixi: pixi run -- python tools/release.py --dry-run
                 pixi run -- python tools/release.py --execute
"""

import argparse
import difflib
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(-dev)?$")
REPO = "buzzcrow/crowdb"


def command(*args: str, capture: bool = False) -> str:
    result = subprocess.run(
        args, cwd=ROOT, check=True, text=True,
        stdout=subprocess.PIPE if capture else None,
        timeout=60,
    )
    return result.stdout.strip() if capture else ""


def next_version(current: str, bump: str) -> str:
    match = VERSION_RE.fullmatch(current)
    if match is None:
        raise ValueError(f"Unsupported VERSION: {current}")
    major, minor, patch = (int(part) for part in match.group(1, 2, 3))
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    if match.group(4) is None:
        patch += 1
    return f"{major}.{minor}.{patch}"


def changes(current: str, target: str) -> dict[Path, str]:
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    workspace_count = len(workspace["workspace"]["members"])
    files = [
        "Cargo.toml", "Cargo.lock", "pixi.toml",
        "app/crowdb-access-server/tests/common/iceberg_rust/Cargo.toml",
        "app/crowdb-access-server/tests/common/iceberg_rust/Cargo.lock",
        "app/crowdb-web/ui/package.json", "app/crowdb-web/ui/package-lock.json",
    ]
    updates = {ROOT / "VERSION": target + "\n"}
    for name in files:
        path = ROOT / name
        original = path.read_text(encoding="utf-8")
        old = f'version = "{current}"' if path.suffix in (".toml", ".lock") else f'"version": "{current}"'
        new = old.replace(current, target)
        count = original.count(old)
        expected = {
            "Cargo.lock": workspace_count,
            "app/crowdb-web/ui/package-lock.json": 2,
        }.get(name, 1)
        if count != expected:
            raise ValueError(f"Expected {expected} version entries in {name}, found {count}")
        updates[path] = original.replace(old, new)
    return updates


def preflight(tag: str) -> None:
    remote_url = command("git", "remote", "get-url", "origin", capture=True)
    if remote_url not in (
        "git@github.com:buzzcrow/crowdb.git",
        "https://github.com/buzzcrow/crowdb.git",
    ):
        raise ValueError(f"Unexpected origin: {remote_url}")
    if command("git", "branch", "--show-current", capture=True) != "main":
        raise ValueError("Run --execute from the clean main branch")
    if command("git", "status", "--porcelain", capture=True):
        raise ValueError("Working tree must be clean before release")
    head = command("git", "rev-parse", "HEAD", capture=True)
    remote_lines = command("git", "ls-remote", "origin", "refs/heads/main", capture=True).split()
    if not remote_lines:
        raise ValueError("origin/main is unavailable")
    remote = remote_lines[0]
    if head != remote:
        raise ValueError("Local main does not match origin/main")
    if command("git", "tag", "--list", tag, capture=True):
        raise ValueError(f"Local tag already exists: {tag}")
    if command("git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}", capture=True):
        raise ValueError(f"Remote tag already exists: {tag}")
    command("gh", "auth", "status")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Print the plan without writing or contacting GitHub")
    mode.add_argument("--execute", action="store_true", help="Update, tag, push, release and dispatch")
    parser.add_argument("--bump", choices=("patch", "minor", "major"), default="patch")
    args = parser.parse_args()

    current = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    target = next_version(current, args.bump)
    tag = f"v{target}"
    updates = changes(current, target)
    print(f"Release {current} -> {target} ({tag})", flush=True)
    for path in updates:
        print(f"  update {path.relative_to(ROOT)}", flush=True)
    print("  check versions and diff; commit; tag; atomically push main + tag", flush=True)
    print("  create draft GitHub Release; dispatch release-container.yml", flush=True)
    if args.dry_run:
        for path, updated in updates.items():
            original = path.read_text(encoding="utf-8")
            sys.stdout.writelines(difflib.unified_diff(
                original.splitlines(keepends=True), updated.splitlines(keepends=True),
                fromfile=str(path.relative_to(ROOT)),
                tofile=str(path.relative_to(ROOT)),
            ))
        return

    preflight(tag)
    for path, content in updates.items():
        path.write_text(content, encoding="utf-8")
    command("pixi", "run", "--", "python", "tools/ci-checks/check-version.py")
    command("git", "diff", "--check")
    command("git", "add", *(str(path.relative_to(ROOT)) for path in updates))
    command("git", "commit", "-m", f"Release {target}")
    command("git", "tag", "-a", tag, "-m", tag)
    command("git", "push", "--atomic", "origin", "HEAD:refs/heads/main", f"refs/tags/{tag}")
    command("gh", "release", "create", tag, "--repo", REPO, "--verify-tag", "--generate-notes", "--draft")
    command("gh", "workflow", "run", "release-container.yml", "--repo", REPO,
            "--ref", tag, "-f", f"tag={tag}")
    print(f"Started verified publication for {tag}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(f"Release stopped: {error}", file=sys.stderr)
        raise SystemExit(1) from error
