# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Export bounded failure diagnostics and small reports without mounted data."""

import argparse
from collections import Counter
import gzip
import json
from pathlib import Path

REPORTS = {"results.json", "timing.json", "identity.json", "client-identity.json",
           "case.json", "endpoints.json", "mounts.json", "s3-node-endpoints.json",
           "native-endpoints.json", "deployment-diagnostics.json"}
PRIVATE = {"nodes", "client", "clients", "private", "node_modules", "source"}
TOTAL_LIMIT = 64 * 1024 * 1024
TEXT_LIMIT = 256 * 1024
TRACE_LIMIT = 16 * 1024 * 1024
REPORT_LIMIT = 1024 * 1024


def service_log(relative):
    parts = relative.parts
    return any(parts[index] == "nodes" and parts[index + 2:index + 4] == ("data", "log")
               for index in range(len(parts) - 3)) and (
                   relative.suffix == ".log" or relative.name.endswith(".log.gz"))


def failed_cases(source):
    cases = {}
    for path in source.rglob("case.json"):
        if path.is_symlink() or not path.resolve().is_relative_to(source):
            continue
        try:
            timing = json.loads((path.parent / "timing.json").read_text())
            cases[path.parent] = not isinstance(timing, dict) or timing.get("status") != "pass"
        except (OSError, ValueError):
            cases[path.parent] = True
    return cases


def read_tail(path, limit):
    opener = gzip.open if path.name.endswith(".log.gz") else open
    with opener(path, "rb") as stream:
        if not path.name.endswith(".log.gz"):
            size = path.stat().st_size
            stream.seek(max(0, size - limit))
            return stream.read(limit), size
        tail, size = b"", 0
        for chunk in iter(lambda: stream.read(65536), b""):
            size += len(chunk)
            tail = (tail + chunk)[-limit:]
        return tail, size


def publish(source, destination, *, total_limit=TOTAL_LIMIT, text_limit=TEXT_LIMIT,
            trace_limit=TRACE_LIMIT):
    source, destination = source.resolve(), destination.resolve()
    if destination == source or source in destination.parents or destination in source.parents:
        raise ValueError("export and original run directories must be separate")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("export needs a fresh directory to avoid stale reports")
    if min(total_limit, text_limit, trace_limit) < 1:
        raise ValueError("diagnostic size limits must be positive")
    cases = failed_cases(source)
    candidates = []
    for path in source.rglob("*"):
        if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(source):
            continue
        relative = path.relative_to(source)
        mounted_log = service_log(relative)
        if any(part in PRIVATE for part in relative.parts[:-1]) and not mounted_log:
            continue
        owner = next((parent for parent in path.parents if parent in cases), None)
        failed = cases.get(owner, not cases)
        report = (path.name in REPORTS or path.name.startswith("client-")) and path.suffix == ".json"
        trace = path.name == "trace.zip" and "browser" in relative.parts
        text = path.suffix in {".log", ".error"} or mounted_log or path.name == "error-context.md"
        if not report and not (failed and (trace or text)):
            continue
        priority = (0 if path.suffix == ".error" or path.name.startswith("client-") and not report
                    else 1 if report or path.name == "error-context.md" else 2 if trace else 3)
        candidates.append((priority, relative.as_posix(), path, report, trace))
    records, used, copied = [], 0, 0
    destination.mkdir(parents=True, exist_ok=True)
    for _, relative, path, report, trace in sorted(candidates):
        record = {"path": relative}
        try:
            record["original_bytes"] = path.stat().st_size
            remaining = total_limit - used
            if remaining < 1 or (trace and path.stat().st_size > trace_limit):
                record["status"] = "skipped"
                record["reason"] = "job budget exhausted" if remaining < 1 else "trace size limit"
            elif report or trace:
                if report and path.stat().st_size > REPORT_LIMIT:
                    record.update(status="skipped", reason="report size limit")
                elif path.stat().st_size > remaining:
                    record.update(status="skipped", reason="file exceeds remaining job budget")
                else:
                    payload = path.read_bytes()
                    output = destination / relative
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_bytes(payload)
                    record.update(status="copied", exported_bytes=len(payload))
            else:
                payload, size = read_tail(path, min(text_limit, remaining))
                output = destination / relative
                if path.name.endswith(".log.gz"):
                    output = output.with_suffix("")
                    record["exported_path"] = output.relative_to(destination).as_posix()
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(payload)
                record.update(status="truncated" if len(payload) < size else "copied",
                              original_text_bytes=size, exported_bytes=len(payload))
                if len(payload) < size:
                    record["reason"] = "job budget" if remaining < text_limit else "text size limit"
            if "exported_bytes" in record:
                used += record["exported_bytes"]
                copied += 1
        except (OSError, EOFError) as error:
            record.update(status="read_failed", error=str(error))
            print(f"[DIAGNOSTIC READ FAILED] {relative}: {error}", flush=True)
        if record.get("status") in {"skipped", "truncated"}:
            print(f"[DIAGNOSTIC OMISSION] {relative}: {record['status']} "
                  f"reason={record.get('reason', 'text size limit')} "
                  f"original_bytes={record.get('original_bytes')} "
                  f"exported_bytes={record.get('exported_bytes', 0)}", flush=True)
        records.append(record)
    counts = Counter(record["status"] for record in records)
    summary = {"exported_bytes": used, "total_limit": total_limit, "text_limit": text_limit,
               "trace_limit": trace_limit, "report_limit": REPORT_LIMIT,
               "counts": dict(counts), "files": records}
    (destination / "export-summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Diagnostics: {copied} files, {used} bytes; limits and omissions in export-summary.json", flush=True)
    print(f"Diagnostic omissions: truncated={counts['truncated']} skipped={counts['skipped']} "
          f"read_failed={counts['read_failed']}", flush=True)
    return copied


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    publish(args.source, args.output)
