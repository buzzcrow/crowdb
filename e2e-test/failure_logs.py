# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Surface owned container errors alongside the failing host case."""

import gzip
import json
import os
import re

# Match a log level, rather than a field or message containing the word
# "error".  A transient `error=...` field is useful context, but it is not by
# itself evidence that a service failed.
ERROR = re.compile(r"\b(?:ERROR|CRITICAL|FATAL)\b|(?i:panicked|segmentation fault|core dumped)")
EXPLICIT_LEVEL = re.compile(r"\b(?:ERROR|CRITICAL|FATAL)\b")
REMOVED_CONTAINER = re.compile(r"(?:no such container|no such object)", re.I)
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
            # Teardown may remove a short-lived helper container before the
            # diagnostics pass reaches its saved docker output.  This is a
            # cleanup race, not a second service failure.
            if path.suffix == ".error" and REMOVED_CONTAINER.search(text):
                continue
            # Raw child stdout/stderr and diagnostic failures may have no level.
            raw = path.name == "service.log" or path.suffix == ".error"
            records = text.splitlines() if raw else error_records(text)
            code = re.search(r"\bexit_code=(\d+)\b", text)
            if raw and path.name == "service.log" and not EXPLICIT_LEVEL.search(text):
                if not code or int(code[1]) == 0:
                    continue
            if records:
                node, service = "unknown", "unknown"
                for data in data_paths:
                    if (data / "log") in path.parents:
                        node = data.parent.name if data.name == "data" else data.name
                        service = path.relative_to(data / "log").parts[0]
                        break
                first_error = next((line for line in records if ERROR.search(line)), records[0])
                summary = {"case": case_name, "container_node": node, "service": service,
                           "service_exit_code": int(code[1]) if code else None,
                           "log": str(path), "first_error": first_error}
                print(redact("[FAILURE SUMMARY] " + json.dumps(summary, ensure_ascii=False)), flush=True)
                print(redact(f"[CONTAINER ERROR LOG] {path}\n" + "\n".join(records)), flush=True)
        except PermissionError:
            # Container-owned files may be unreadable from the host after a
            # failed run.  The publisher records the omission separately; do
            # not turn it into another failure line here.
            continue
        except (OSError, EOFError) as log_error:
            print(redact(f"[LOG READ FAILED] {path}: {log_error}"), flush=True)
