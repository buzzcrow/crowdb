# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Redact the owned fixture's credentials before printing or saving diagnostics."""
import os
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
label = sys.argv[2] if len(sys.argv) > 2 else None
secrets = {os.environ.get("ICEBERG_TOKEN", "")}
for credential_file in root.joinpath("data/secrets").glob("*.env"):
    for line in credential_file.read_text().splitlines():
        key, _, value = line.partition("=")
        if any(name in key for name in ("TOKEN", "SECRET", "PASSWORD", "ACCESS_KEY")):
            secrets.add(value.strip().strip("\"'"))
for source in root.joinpath("private").glob("*.log"):
    text = source.read_text(errors="replace")
    for secret in sorted((value for value in secrets if value), key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(?i)(authorization\s*[:=]\s*bearer\s+)\S+", r"\1[REDACTED]", text)
    text = re.sub(
        r"(?i)((?:x-amz-(?:credential|signature|security-token)|"
        r"aws_secret_access_key|aws_session_token|access-key-id|secret-access-key)"
        r"[\"']?\s*[:=]\s*[\"']?)[^\s\"'&,}]+",
        r"\1[REDACTED]", text,
    )
    root.joinpath("results", source.name).write_text(text)
    if source.name == f"{label}.log" or (label is None and source.name not in {"container.log", "trino.log"}):
        print(text, end="")
