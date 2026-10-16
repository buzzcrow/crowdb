#!/usr/bin/env python3
"""Verify the standalone SDK does not acquire server build dependencies."""

import json
import subprocess
import tomllib
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[2]
    sdk = root / "sdk"
    command = ["cargo", "metadata", "--manifest-path", str(sdk / "Cargo.toml"),
               "--no-deps", "--format-version", "1", "--locked"]
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, check=True)
    metadata = json.loads(result.stdout)
    if Path(metadata["workspace_root"]).resolve() != sdk.resolve():
        raise SystemExit("Dataset SDK is not an independent workspace")
    for package in metadata["packages"]:
        if not package["name"].startswith("crowdb-sdk-"):
            raise SystemExit(f"Unexpected SDK member: {package['name']}")
        for dependency in package["dependencies"]:
            if dependency.get("path") and not Path(dependency["path"]).resolve().is_relative_to(sdk.resolve()):
                raise SystemExit(f"SDK depends on a parent path: {dependency['name']}")
            if dependency["name"].startswith("crowdb-") and not dependency["name"].startswith("crowdb-sdk-"):
                raise SystemExit(f"SDK imports a server package: {dependency['name']}")
    lock = tomllib.loads((sdk / "Cargo.lock").read_text())
    for package in lock["package"]:
        if package.get("source", "").startswith("git+"):
            raise SystemExit(f"SDK acquired a Git-patched dependency: {package['name']}")
    print(f"Dataset SDK boundary verified: {len(metadata['packages'])} independent packages")


if __name__ == "__main__":
    main()
