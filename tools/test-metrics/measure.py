#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Measure selected Pixi suites sequentially, retaining complete logs."""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/ci-checks"))
from task_graph import TaskGraph

ROOT = Path(__file__).resolve().parents[2]


def test_counts(text):
    """Count each Rust case once, including later explicit ignored-test runs."""
    binary = None
    cases = {}
    helpers = set()
    pending = None
    for line in text.splitlines():
        running = re.search(r"Running .* \(([^)]+)\)", line)
        if running:
            binary = Path(running[1]).name
            pending = None
        docs = re.match(r"\s*Doc-tests\s+(\S+)", line)
        if docs:
            binary = "doc:" + docs[1]
        case = re.match(r"test (.+?) \.\.\. (ok|FAILED|ignored)\b(.*)", line)
        started = re.match(r"test (.+?) \.\.\.", line)
        if binary and started and not case:
            pending = (binary, started[1])
        if pending and line.strip() in {"ok", "FAILED"}:
            cases[pending] = line.strip()
            pending = None
        if binary and case:
            key = (binary, case[1])
            if "test-only child listener" in case[3]:
                helpers.add(key)
            if case[2] == "FAILED" or cases.get(key) != "ok":
                cases[key] = case[2]
    cases = {key: state for key, state in cases.items() if key not in helpers}
    if cases:
        passed = sum(state == "ok" for state in cases.values())
        ignored = sum(state == "ignored" for state in cases.values())
        failed = sum(state == "FAILED" for state in cases.values())
    else:
        passed = sum(int(n) for n in re.findall(r"(?:Tests\s+|^\s*)(\d+) passed", text, re.M))
        ignored = 0
        failed = sum(int(n) for n in re.findall(r"(?:Tests\s+|^\s*)(\d+) failed", text, re.M))
    passed += sum(int(n) for n in re.findall(r"100% tests passed(?:, 0 tests failed)? out of (\d+)", text))
    passed += sum(int(n) for n in re.findall(r"\[\s*PASSED\s*\]\s*(\d+) tests?", text))
    ignored_cases = [f"{re.sub(r'-[0-9a-f]+$', '', binary)}::{name}"
                     for (binary, name), state in sorted(cases.items()) if state == "ignored"]
    return passed, ignored, ignored_cases, failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suites", nargs="+", help="Pixi task names; feature environment is inferred")
    args = parser.parse_args()
    graph = TaskGraph(ROOT)
    output = ROOT / ".crowdb-runtime/artifacts/measure-tests"
    output.mkdir(parents=True, exist_ok=True)
    run_date = datetime.now(timezone.utc)
    archive = output / run_date.strftime("%Y%m%dT%H%M%S.%fZ")
    archive.mkdir()
    results = []
    for suite in args.suites:
        matches = [key for key in graph.tasks if key[1] == suite]
        if len(matches) != 1:
            parser.error(f"expected one task named {suite}, found {matches}")
        environment, _ = matches[0]
        subprocess.run(["pixi", "run", "clean-env"], cwd=ROOT, check=True)
        log = archive / f"measure-{suite}.out"
        started = time.monotonic()
        with log.open("w") as stream:
            result = subprocess.run(["pixi", "run", "-e", environment, suite], cwd=ROOT,
                                    stdout=stream, stderr=subprocess.STDOUT)
        elapsed = round(time.monotonic() - started, 2)
        text = re.sub(r"\x1b\[[0-9;]*m", "", log.read_text(errors="replace"))
        passed, ignored, ignored_cases, failed = test_counts(text)
        record = dict(suite=suite, environment=environment, passed=passed, ignored=ignored,
                      ignored_cases=ignored_cases, failed=failed,
                      seconds=elapsed, exit_code=result.returncode, log=str(log),
                      run_started_at=run_date.isoformat())
        results.append(record)
        encoded = json.dumps(results, indent=2) + "\n"
        (archive / "results.json").write_text(encoded)
        (output / "latest.json").write_text(encoded)
        print(json.dumps(record), flush=True)
        if result.returncode:
            print(text, flush=True)
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
