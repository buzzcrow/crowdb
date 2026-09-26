# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
errors: list[str] = []


def load_toml(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def require(path: str, actual: object, expected: object) -> None:
    if actual != expected:
        errors.append(f"{path}: expected {expected!r}, found {actual!r}")


cargo = load_toml(ROOT / "Cargo.toml")
require("Cargo.toml workspace version", cargo["workspace"]["package"]["version"], EXPECTED)
workspace_names: set[str] = set()
for member in cargo["workspace"]["members"]:
    manifest_path = ROOT / member / "Cargo.toml"
    package = load_toml(manifest_path)["package"]
    workspace_names.add(package["name"])
    require(
        f"{manifest_path.relative_to(ROOT)} version",
        package.get("version"),
        {"workspace": True},
    )

lock = load_toml(ROOT / "Cargo.lock")
locked = {
    package["name"]: package["version"]
    for package in lock["package"]
    if package["name"] in workspace_names
}
for name in sorted(workspace_names):
    require(f"Cargo.lock package {name}", locked.get(name), EXPECTED)

pixi = load_toml(ROOT / "pixi.toml")
require("pixi.toml workspace version", pixi["workspace"]["version"], EXPECTED)

fixture = load_toml(ROOT / "app/crowdb-access-server/tests/common/iceberg_rust/Cargo.toml")
require("Iceberg Rust fixture version", fixture["package"]["version"], EXPECTED)

ui_root = ROOT / "app/crowdb-web/ui"
package_json = json.loads((ui_root / "package.json").read_text(encoding="utf-8"))
package_lock = json.loads((ui_root / "package-lock.json").read_text(encoding="utf-8"))
require("UI package version", package_json["version"], EXPECTED)
require("UI lock version", package_lock["version"], EXPECTED)
require("UI lock root package version", package_lock["packages"][""]["version"], EXPECTED)

if errors:
    print("version consistency check failed:", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    raise SystemExit(1)

print(f"version consistency check passed: {EXPECTED}")
