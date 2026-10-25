# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Bounded, trial-boundary observations of an owned Dataset workload."""

import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time

from dataset_observation_resources import local_delta, resource_delta
from dataset_observation_phases import summarize_phases

ENVIRONMENT = "CROWDB_DATASET_OBSERVATION_RUN_ID"
PREFIX = b"CROWDB_DATASET_OBSERVATION "
LINE_LIMIT = 1024 * 1024
SIZES = [0, 128, 513, 4096, 65_535, 262_144, 1_048_576, 4_194_321]
ORDER = [(concurrency, repeat) for concurrency in (1, 3) for repeat in range(3)]
HEX_ID = re.compile(r"[0-9a-f]{32}\Z")


class ObservationError(RuntimeError):
    pass


class _Child:
    def __init__(self, command, environment):
        self.process = subprocess.Popen(command, env=environment, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        start_new_session=True, bufsize=0)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.pid = self.process.pid

    def read(self, timeout):
        if not self.selector.select(timeout):
            return None
        return os.read(self.process.stdout.fileno(), 65536)

    def send(self, record):
        self.process.stdin.write(json.dumps(record, separators=(",", ":")).encode() + b"\n")
        self.process.stdin.flush()

    def wait(self, timeout):
        return self.process.wait(timeout=timeout)

    def close(self, log):
        if self.process.poll() is None:
            try:
                os.killpg(self.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            chunk = self.read(max(0, deadline - time.monotonic()))
            if chunk is None:
                continue
            if not chunk:
                break
            log.write(chunk)
            log.flush()
        self.selector.close()
        self.process.stdin.close()
        self.process.stdout.close()


def _trial(record, run_id, path, index, snapshot):
    trial = record.get("trial")
    if not isinstance(trial, dict) or set(trial) != {"run_id", "path", "concurrency", "repeat", "snapshot"}:
        raise ObservationError("invalid trial identity shape")
    if index >= len(ORDER) or type(trial["concurrency"]) is not int or type(trial["repeat"]) is not int:
        raise ObservationError("invalid trial order")
    if ((trial["concurrency"], trial["repeat"]) != ORDER[index] or trial["run_id"] != run_id
            or trial["path"] != path or not isinstance(trial["snapshot"], str)
            or not HEX_ID.fullmatch(trial["snapshot"]) or snapshot not in (None, trial["snapshot"])):
        raise ObservationError("trial order, run, path or snapshot changed")
    return trial


def _observation(record):
    observation = record.get("observation")
    if not isinstance(observation, dict) or set(observation) != {
            "started_unix_ns", "finished_unix_ns", "topology", "resources", "unavailable"}:
        raise ObservationError("invalid local observation shape")
    if not all(isinstance(observation[key], str) and observation[key].isdecimal()
               for key in ("started_unix_ns", "finished_unix_ns")):
        raise ObservationError("invalid local capture timestamps")
    if not isinstance(observation["unavailable"], dict) or any(
            observation[key] is not None and not isinstance(observation[key], dict)
            for key in ("topology", "resources")):
        raise ObservationError("invalid local snapshot values")
    return observation


def run_observed(command, *, environment, output, path, run_id, capture, timeout=600,
                 expected_snapshot=None, child_factory=_Child, clock=time.monotonic_ns):
    """Run six measured boundary handshakes; preserve raw output and partial failures."""
    if not HEX_ID.fullmatch(run_id) or path not in ("http", "native-rust", "native-python"):
        raise ObservationError("invalid run or workload path")
    output = Path(output)
    artifact = output / f"{path}-observations.json"
    document = {"schema_version": 1, "run_id": run_id, "path": path, "trials": [],
                "deadline_seconds": timeout,
                "scope": "client_inner_and_service_enclosing_windows_are_not_additive",
                "not_measured": ["all_runtime_receive_frames_bytes",
                                 "http_wire_frames_bytes", "total_payload_copy_bytes",
                                 "background_gc_attribution", "per_trial_memory_peaks", "per_request_server_cpu",
                                 "control_and_chunkdb_transport_sent_counters"]}
    child_environment = dict(environment, **{ENVIRONMENT: run_id})
    (output / f"{path}.log").touch()
    try:
        child = child_factory(command, child_environment)
    except BaseException as error:
        document.update(complete=False, error=f"{type(error).__name__}: {error}")
        artifact.write_text(json.dumps(document, indent=2))
        raise
    deadline = clock() + int(timeout * 1_000_000_000)
    pending, snapshot, buffer = None, expected_snapshot, b""

    def remaining():
        value = (deadline - clock()) / 1_000_000_000
        if value <= 0:
            raise ObservationError(f"workload exceeded its {timeout}-second execution bound")
        return value

    def reply(event, trial):
        child.send({"v": 1, "event": event, "trial": trial})
        return clock()

    def save():
        artifact.write_text(json.dumps(document, indent=2))

    try:
        with (output / f"{path}.log").open("wb") as log:
            while True:
                chunk = child.read(remaining())
                if chunk is None:
                    continue
                if not chunk:
                    break
                log.write(chunk)
                log.flush()
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if len(line) + 1 > LINE_LIMIT:
                        raise ObservationError("workload output line exceeds 1 MiB")
                    if not line.startswith(PREFIX):
                        continue
                    record = json.loads(line[len(PREFIX):])
                    if not isinstance(record, dict) or type(record.get("v")) is not int or record["v"] != 1:
                        raise ObservationError("unsupported observation protocol")
                    trial = _trial(record, run_id, path, len(document["trials"]), snapshot)
                    if record.get("event") == "ready" and pending is None:
                        snapshot = trial["snapshot"]
                        pending = {"trial": trial, "ready_received_monotonic_ns": clock(),
                                   "local_before": _observation(record), "before": capture(child.pid)}
                        document["active"] = pending
                        pending["go_sent_monotonic_ns"] = reply("go", trial)
                    elif record.get("event") == "done" and pending is not None and pending["trial"] == trial:
                        pending.update(done_received_monotonic_ns=clock(), local_after=_observation(record))
                        result = record.get("result", {})
                        if (not isinstance(result, dict)
                                or type(result.get("completed_requests")) is not int
                                or type(result.get("bytes")) is not int
                                or type(result.get("errors")) is not int
                                or result.get("completed_requests") != trial["concurrency"] * 16 * len(SIZES)
                                or result.get("bytes") != trial["concurrency"] * 16 * sum(SIZES)
                                or result.get("errors") != 0):
                            raise ObservationError("measured completion contract changed")
                        pending.update(result=result, after=capture(child.pid))
                        try:
                            pending["phase_summary"] = summarize_phases(result, path, trial["concurrency"], SIZES)
                        except ValueError as error:
                            raise ObservationError(str(error)) from error
                        pending["enclosing_delta"] = resource_delta(pending["before"], pending["after"])
                        pending["local_delta"] = local_delta(pending["local_before"], pending["local_after"])
                        document["trials"].append(pending)
                        del document["active"]
                        save()
                        pending["ack_sent_monotonic_ns"] = reply("ack", trial)
                        pending = None
                    else:
                        raise ObservationError("unexpected observation phase")
                    save()
                if len(buffer) > LINE_LIMIT:
                    raise ObservationError("workload output line exceeds 1 MiB")
            if buffer.startswith(PREFIX) or pending is not None or len(document["trials"]) != len(ORDER):
                raise ObservationError("workload exited with an incomplete trial protocol")
            code = child.wait(remaining())
            if code != 0:
                raise ObservationError(f"workload exited with status {code}")
            document["complete"] = True
    except BaseException as error:
        document.update(complete=False, error=f"{type(error).__name__}: {error}")
        if pending is not None and "after" not in pending:
            try:
                pending["after_failure"] = capture(child.pid)
            except Exception as capture_error:
                pending["capture_error"] = str(capture_error)
        raise
    finally:
        try:
            with (output / f"{path}.log").open("ab") as log:
                child.close(log)
        finally:
            save()
    return document
