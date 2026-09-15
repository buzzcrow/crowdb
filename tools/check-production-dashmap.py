#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Reject unclassified DashMap fields in production Rust sources."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE_ROOTS = (ROOT / "app", ROOT / "lib")
FIELD_PATTERN = re.compile(
    r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?P<field>[A-Za-z_]\w*)\s*:\s*"
    r"(?:Arc\s*<\s*)?DashMap\s*<"
)
TYPE_PATTERN = re.compile(r"\bDashMap\s*<")

# Each retained field is off the request hot path or contains only a brief
# lookup guard. The reason also documents the maximum guard lifetime.
ALLOWED: dict[tuple[str, str], str] = {
    (
        "app/crowdb-kv-server/src/mgmt/operation_registry.rs",
        "operations",
    ): "low-frequency management operations; guards are dropped before polling or await",
    (
        "lib/crowdb-kv/src/cluster/group.rs",
        "snapshots",
    ): "per-group snapshot lifecycle handles; guards only clone or remove one handle",
    (
        "lib/crowdb-kv-client/src/service/discovery.rs",
        "services",
    ): "service-name cache; lookup clones Arc<ServiceState> before refresh I/O",
    (
        "lib/crowdb-kv/src/rpc/snapshot_registry.rs",
        "sessions",
    ): "snapshot transfer lifecycle; guards clone Arc<RegistryEntry> or remove before any await",
    (
        "lib/crowdb-kv/src/rpc/snapshot_registry.rs",
        "expired",
    ): "expired-session tombstones; lookups only check contains_key, insert, or remove",
}


def production_sources() -> list[Path]:
    sources: list[Path] = []
    for root in SOURCE_ROOTS:
        sources.extend(root.glob("*/src/**/*.rs"))
    return sorted(sources)


def main() -> int:
    found: dict[tuple[str, str], int] = {}
    errors: list[str] = []

    for path in production_sources():
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(ROOT).as_posix()
        matches = list(FIELD_PATTERN.finditer(text))
        typed_uses = list(TYPE_PATTERN.finditer(text))
        if len(matches) != len(typed_uses):
            parsed_offsets = {match.end() - 1 for match in matches}
            for use in typed_uses:
                if use.end() - 1 not in parsed_offsets:
                    line = text.count("\n", 0, use.start()) + 1
                    errors.append(f"{relative}:{line}: unclassified DashMap type use")
        for match in matches:
            field = match.group("field")
            line = text.count("\n", 0, match.start()) + 1
            key = (relative, field)
            found[key] = line
            if key not in ALLOWED:
                errors.append(f"{relative}:{line}: unclassified DashMap field `{field}`")

    for (relative, field), reason in ALLOWED.items():
        if (relative, field) not in found:
            errors.append(f"stale allowlist entry: {relative} `{field}` ({reason})")

    if errors:
        print("production DashMap inventory failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print(f"production DashMap inventory: {len(found)} classified fields")
    for relative, field in sorted(found):
        print(f"  {relative}:{found[(relative, field)]} `{field}` — {ALLOWED[(relative, field)]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
