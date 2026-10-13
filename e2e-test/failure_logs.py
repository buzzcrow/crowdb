# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Surface owned container errors alongside the failing host case."""

import gzip
import os
import re

ERROR = re.compile(r"\b(?:error|critical|fatal)\b|panicked|segmentation fault|core dumped", re.I)
RECORD = re.compile(r"^\[?\d{4}(?:-\d{2}-\d{2}|\d{4}-)")


def error_records(text):
    records, current = [], []
    for line in text.splitlines():
        if RECORD.match(line) and current:
            records.append(current)
            current = []
        current.append(line)
    if current:
        records.append(current)
    return [line for record in records if any(ERROR.search(line) for line in record) for line in record]


def print_failure(artifacts, data_paths, case_name, error, redact):
    print(redact(f"[FAILED CASE] {case_name}: {error}\nArtifacts: {artifacts}"), flush=True)
    if os.environ.get("CROWDB_E2E_PRINT_SERVICE_ERRORS", "0") != "1":
        return
    paths = list(artifacts.glob("*-logs.log")) + list(artifacts.glob("*.error"))
    for data in data_paths:
        root = data / "log"
        if root.is_symlink():
            continue
        paths.extend(path for path in root.rglob("*")
                     if path.is_file() and not path.is_symlink()
                     and root.resolve() in path.resolve().parents
                     and (path.suffix == ".log" or path.name.endswith(".log.gz")))
    for path in sorted(set(paths)):
        try:
            if path.is_symlink():
                continue
            opener = gzip.open if path.name.endswith(".gz") else open
            with opener(path, "rt", errors="replace") as stream:
                text = stream.read()
            # Raw child stdout/stderr and diagnostic failures may have no level.
            raw = path.name == "service.log" or path.suffix == ".error"
            records = text.splitlines() if raw else error_records(text)
            if records:
                print(redact(f"[CONTAINER ERROR LOG] {path}\n" + "\n".join(records)), flush=True)
        except (OSError, EOFError) as log_error:
            print(redact(f"[LOG READ FAILED] {path}: {log_error}"), flush=True)
