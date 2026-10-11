# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Export measured reports and redacted text diagnostics without mounted data."""

import argparse
import json
from pathlib import Path
import re

SECRET = re.compile(
    r'(?i)([\w.-]*(?:token|password|secret|access[_-]?key|master[_-]?key|'
    r'authorization|credential|signature)[\w.-]*["\']?\s*[:=](?!:)\s*["\']?)'
    r'(?:(?:Bearer|Basic)\s+)?([^\s,"\'}&{\[\]]+)')
REPORTS = {"results.json", "timing.json", "identity.json", "client-identity.json",
           "case.json", "endpoints.json", "mounts.json", "s3-node-endpoints.json"}
PRIVATE = {"nodes", "client", "clients", "private", "node_modules", "source"}
SECRET_KEY = re.compile(
    r'(?i)token|password|secret|access[_-]?key|master[_-]?key|authorization|credential|signature')


def redact(text, secrets):
    for value in sorted(secrets, key=len, reverse=True):
        text = text.replace(value, "<redacted>")
    return SECRET.sub(r'\1<redacted>', text)


def redact_json(value, secrets):
    if isinstance(value, dict):
        return {key: ("<redacted>" if isinstance(item, str) and SECRET_KEY.search(key)
                      else redact_json(item, secrets)) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_json(item, secrets) for item in value]
    return redact(value, secrets) if isinstance(value, str) else value


def publish(source, destination):
    source, destination = source.resolve(), destination.resolve()
    if destination == source or source in destination.parents or destination in source.parents:
        raise ValueError("public export and original run directories must be separate")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("public export needs a fresh directory to avoid stale reports")
    files = [p for p in source.rglob("*") if p.is_file() and not p.is_symlink()
             and not any(part in PRIVATE for part in p.relative_to(source).parts[:-1])]
    secrets = set()
    for path in files:
        if path.suffix in {".toml", ".env", ".log", ".json"}:
            for match in SECRET.finditer(path.read_text(errors="replace")):
                value = match[2]
                if len(value) >= 8 and value not in {"<redacted>", "[REDACTED]"}:
                    secrets.add(value)
    copied = 0
    for path in files:
        relative = path.relative_to(source)
        selected = path.name in REPORTS or path.name.startswith(("client-", "topology-"))
        if path.suffix not in {".json", ".log", ".error"} or not (selected or path.suffix in {".log", ".error"}):
            continue
        text = path.read_text(errors="replace")
        # Preserve machine-readable report structure after replacing strings.
        if path.suffix == ".json":
            text = json.dumps(redact_json(json.loads(text), secrets), indent=2)
        else:
            text = redact(text, secrets)
        output = destination / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
        copied += 1
    return copied


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(f"Exported {publish(args.source, args.output)} redacted diagnostic files")
