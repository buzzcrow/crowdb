# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Explicit choices for fixtures that test bootstrap rather than default setup."""


def diskless_setup():
    return {"storage": {"mode": "diskless"}, "services": [
        {"kind": kind, "enabled": False, "listeners": {}}
        for kind in ("paxos-kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access-server")]}
