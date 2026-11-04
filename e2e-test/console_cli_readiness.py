# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Continuous negative readiness evidence during the original DiskIO outage."""

import json
from pathlib import Path
import sys
import time


OUTAGE_SECONDS = 40
OBSERVATION_SECONDS = 8


def check_unavailable(values, endpoints):
    assert type(endpoints) is int and endpoints > 0, endpoints
    assert len(values) == endpoints, values
    for value in values:
        assert value.get("ready") is False and value.get("live") is True, value
        observation = value.get("observation", {})
        generation = observation.get("generation")
        checked = observation.get("checked_at_unix_ms")
        age = observation.get("age_ms")
        assert type(generation) is int and generation > 0, value
        assert type(checked) is int and checked > 0, value
        assert type(age) is int and age >= 0, value
        assert observation.get("max_age_ms") == 3000, value
        assert isinstance(value.get("details", {}).get("chunks"), str), value
        assert value["details"]["chunks"], value
        state = observation.get("state")
        if state == "current":
            assert age <= 3000 and value.get("chunks") == "unavailable", value
            assert "chunks" in value.get("reasons", []), value
        else:
            # age_ms truncates Duration, so an expired check may display 3000.
            assert state == "expired" and age >= 3000, value
            assert value.get("chunks") == "unknown", value
            assert "observation-expired" in value.get("reasons", []), value


def check_progress(history, endpoints):
    assert history, "No outage observations were retained"
    for sample in history:
        check_unavailable(sample["responses"], endpoints)
    for endpoint in range(endpoints):
        previous = None
        generations = set()
        for sample in history:
            observation = sample["responses"][endpoint]["observation"]
            current = (observation["generation"], observation["checked_at_unix_ms"])
            if previous is not None:
                assert current[0] >= previous[0], (endpoint, previous, current)
                if current[0] == previous[0]:
                    assert current[1] == previous[1], (endpoint, previous, current)
                else:
                    assert current[1] > previous[1], (endpoint, previous, current)
            generations.add(current[0])
            previous = current
        assert len(generations) >= 3, ("Sampler did not advance twice", endpoint, history)


def observe_unavailable(artifacts, snapshot, endpoints):
    started = time.monotonic()
    deadline = started + OUTAGE_SECONDS
    time.sleep(OUTAGE_SECONDS - OBSERVATION_SECONDS)
    history = []
    failure = None
    try:
        while True:
            sample = {"started_at_unix_ms": time.time_ns() // 1_000_000,
                      "elapsed_seconds": time.monotonic() - started,
                      "responses": snapshot()}
            sample["finished_at_unix_ms"] = time.time_ns() // 1_000_000
            history.append(sample)
            check_unavailable(sample["responses"], endpoints)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.25, remaining))
        check_progress(history, endpoints)
    except BaseException as error:
        failure = repr(error)
        raise
    finally:
        report = {"outage_seconds": OUTAGE_SECONDS,
                  "observation_seconds": OBSERVATION_SECONDS,
                  "elapsed_seconds": time.monotonic() - started,
                  "failure": failure, "samples": history}
        try:
            (Path(artifacts) / "access-health-paused-history.json").write_text(
                json.dumps(report, indent=2))
        except OSError as error:
            if failure is None:
                raise
            print(f"Secondary readiness history write error: {error}", file=sys.stderr, flush=True)
