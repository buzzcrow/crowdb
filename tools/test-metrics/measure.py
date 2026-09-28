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
        rust = re.findall(r"test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored", text)
        passed = sum(int(row[0]) for row in rust)
        ignored = sum(int(row[2]) for row in rust)
        if not rust:
            passed = sum(int(n) for n in re.findall(r"(?:Tests\s+|^\s*)(\d+) passed", text, re.M))
        if not rust and not passed:
            ctest = re.search(r"100% tests passed, 0 tests failed out of (\d+)", text)
            gtest = re.search(r"\[\s*PASSED\s*\]\s*(\d+) tests?", text)
            match = ctest or gtest
            if match:
                passed = int(match[1])
        record = dict(suite=suite, environment=environment, passed=passed, ignored=ignored,
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
