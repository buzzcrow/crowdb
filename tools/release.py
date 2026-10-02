#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Dispatch the container workflow for the current release branch.

Run through Pixi: pixi run -- python tools/release.py --dry-run
                 pixi run -- python tools/release.py --execute
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
REPO = "buzzcrow/crowdb"


def command(*args: str, capture: bool = False) -> str:
    result = subprocess.run(
        args, cwd=ROOT, check=True, text=True,
        stdout=subprocess.PIPE if capture else None,
        timeout=60,
    )
    return result.stdout.strip() if capture else ""


def release_branch() -> str:
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    if VERSION_RE.fullmatch(version) is None:
        raise ValueError(f"Expected a release version in VERSION, found {version}")
    branch = command("git", "branch", "--show-current", capture=True)
    if branch != f"release/{version}":
        raise ValueError(f"Run from release/{version}, found {branch}")
    return branch


def preflight(branch: str) -> None:
    remote_url = command("git", "remote", "get-url", "origin", capture=True)
    if remote_url not in (
        "git@github.com:buzzcrow/crowdb.git",
        "https://github.com/buzzcrow/crowdb.git",
    ):
        raise ValueError(f"Unexpected origin: {remote_url}")
    if command("git", "status", "--porcelain", capture=True):
        raise ValueError("Working tree must be clean before dispatch")
    head = command("git", "rev-parse", "HEAD", capture=True)
    remote_lines = command("git", "ls-remote", "origin", f"refs/heads/{branch}", capture=True).split()
    if not remote_lines:
        raise ValueError(f"origin/{branch} is unavailable")
    remote = remote_lines[0]
    if head != remote:
        raise ValueError(f"Local {branch} does not match origin/{branch}")
    command("gh", "auth", "status")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Print the plan without contacting GitHub")
    mode.add_argument("--execute", action="store_true", help="Dispatch the container workflow")
    args = parser.parse_args()

    branch = release_branch()
    print(f"Dispatch release-container.yml on {branch}", flush=True)
    for repository in ("crowdb/crowdb-iceberg", "crowdb/crowdb-s3"):
        print(f"Image tag: {repository}:{branch.removeprefix('release/')}", flush=True)
        print(f"Also updates: {repository}:latest", flush=True)
    if args.dry_run:
        return

    preflight(branch)
    command("gh", "workflow", "run", "release-container.yml", "--repo", REPO,
            "--ref", branch)
    print(f"Started container verification for {branch}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(f"Release stopped: {error}", file=sys.stderr)
        raise SystemExit(1) from error
