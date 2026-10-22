# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Collect per-flow client phases and complete fixture wall times."""

import json


def write_report(root, timings):
    cases = []
    for path in sorted(root.glob("crowdb-e2e-*/case.json")):
        case = json.loads(path.read_text())
        directory = path.parent
        phases = [json.loads(phase.read_text()) for phase in sorted(directory.glob("client-*.json"))]
        fixture_path = directory / "timing.json"
        fixture = json.loads(fixture_path.read_text()) if fixture_path.exists() else {}
        completed = fixture.get("status") == "pass" and bool(phases) and all(
            phase.get("exit_code") == phase.get("expected_exit_code", 0) for phase in phases)
        actions = [json.loads(action.read_text()) for action in sorted(
            (directory / "control").glob("*.response.json"))]
        cases.append({**case, "actions": actions, "artifacts": str(directory), "phases": phases,
                      "fixture": fixture, "status": "pass" if completed else "failed"})
    cases.sort(key=lambda case: case["fixture"].get("total", 0), reverse=True)
    slowest_clients = sorted(
        ({"case": case["case"], "scope": case["scope"], **phase}
         for case in cases for phase in case["phases"]),
        key=lambda phase: phase["seconds"], reverse=True)
    report = {"timings": timings, "cases": cases, "slowest_clients": slowest_clients[:10]}
    (root / "results.json").write_text(json.dumps(report, indent=2))
    (root / "timing.json").write_text(json.dumps(timings, indent=2))
