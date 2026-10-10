# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Resolve the repository's Pixi task calls and checked-in shell entry points."""

import re
import tomllib
from pathlib import Path

CALL = re.compile(r"\bpixi\s+run\s+(?:--skip-deps\s+)?(?:-e\s+([\w-]+)\s+)?(?!-)([\w-]+)")
SCRIPT = re.compile(r"(?:bash|source)\s+(tools/[\w/.-]+\.sh)")


class TaskGraph:
    def __init__(self, root: Path):
        self.root = root
        self.manifest = tomllib.loads((root / "pixi.toml").read_text())
        self.tasks = {("default", name): value for name, value in self.manifest["tasks"].items()}
        for feature, value in self.manifest.get("feature", {}).items():
            for name, task in value.get("tasks", {}).items():
                self.tasks[feature, name] = task

    def expand(self, text: str, seen=None) -> str:
        seen = set() if seen is None else seen
        text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        result = text
        for path in SCRIPT.findall(text):
            if path not in seen:
                seen.add(path)
                result += "\n" + self.expand((self.root / path).read_text(), seen)
        return result

    def command(self, key) -> str:
        value = self.tasks[key]
        return self.expand(value if isinstance(value, str) else value.get("cmd", ""))

    def calls(self, text: str, environment="default"):
        for selected, name in CALL.findall(text):
            key = (selected or environment, name)
            if key not in self.tasks:
                key = ("default", name)
            if key in self.tasks:
                yield key

    def reachable(self, workflow: str):
        pending = list(self.calls(workflow))
        visited = set()
        while pending:
            key = pending.pop()
            if key in visited:
                continue
            visited.add(key)
            pending.extend(self.calls(self.command(key), key[0]))
            value = self.tasks[key]
            if isinstance(value, dict):
                for dependency in value.get("depends-on", []):
                    target = (key[0], dependency)
                    pending.append(target if target in self.tasks else ("default", dependency))
        return visited
