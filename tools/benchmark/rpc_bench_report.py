# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Validate canonical rows and retain complete comparison tables."""

import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess


def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity(root, binaries, device, hca_port):
    cpuinfo = Path("/proc/cpuinfo").read_text() if Path("/proc/cpuinfo").exists() else ""
    cpu = next((line.split(":", 1)[1].strip() for line in cpuinfo.splitlines()
                if line.startswith("model name")), platform.processor())
    return dict(git_sha=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                git_dirty=bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root)),
                binary_hashes={side: file_hash(path)
                               for side, path in binaries.items()},
                cpu_hca_identity=dict(cpu=cpu, logical_cpus=os.cpu_count(), kernel=platform.release(),
                                      architecture=platform.machine(), device=device, hca_port=hca_port,
                                      runner_affinity=sorted(os.sched_getaffinity(0))
                                      if hasattr(os, "sched_getaffinity") else None))


def provider_counters(device, port):
    base = Path("/sys/class/infiniband") / device / "ports" / str(port)
    result = {}
    for group in ("counters", "hw_counters"):
        for path in sorted((base / group).glob("*")):
            try:
                result[f"{group}.{path.name}"] = int(path.read_text().strip())
            except (ValueError, OSError):
                result[f"{group}.{path.name}"] = None
    return result or None


def validate_sizes(row, case):
    sizes = row.get("size_results")
    expected = {int(part.split(":")[0]) for part in case["payload_mix"].split(",")}
    if not isinstance(sizes, list) or len(sizes) != len(expected):
        return ["missing/incomplete per-size results"]
    seen, completed, payload = set(), 0, 0
    for item in sizes:
        if not isinstance(item, dict):
            return ["invalid per-size results"]
        size, count = item.get("payload_bytes"), item.get("completed")
        if (type(size) is not int or size not in expected or size in seen
                or type(count) is not int or count < 0
                or item.get("payload_bytes_completed") != size * count):
            return ["invalid per-size class/count/bytes"]
        latency = [item.get(key) for key in ("p50_ns", "p99_ns", "p999_ns", "max_ns")]
        if any(type(value) is not int or value < 0 for value in latency) or latency != sorted(latency):
            return ["invalid per-size latency bounds"]
        seen.add(size)
        completed += count
        payload += size * count
    if completed != row.get("completed_in_window") or payload != row.get("payload_bytes_completed"):
        return ["mixed completion/byte totals mismatch"]
    duration = row.get("measure_s")
    bandwidth = row.get("payload_Bps")
    if (type(duration) is not int or duration <= 0 or type(bandwidth) not in (int, float)
            or not math.isfinite(bandwidth) or not math.isclose(bandwidth, payload / duration, rel_tol=1e-9)):
        return ["mixed payload bandwidth mismatch"]
    return []


def validate(row, case, transport):
    errors = []
    if row.get("schema_version") != 1:
        errors.append("missing/unsupported report schema")
    if type(row.get("valid")) is not bool:
        errors.append("missing/invalid validity flag")
    required = ("errors_total", "report_errors", "setup_errors", "submitted", "completed_in_window", "latency_count",
                "submit_rejects", "timeouts", "connection_errors", "cq_errors", "payload_mismatches",
                "duplicate_completions", "retries", "histogram_overflow_count", "measure_s")
    for name in required:
        value = row.get(name)
        if type(value) is not int or value < 0:
            errors.append(f"missing/invalid {name}")
    if row.get("actual_path") != transport or row.get("allow_tcp_fallback") is not False:
        errors.append("actual path/fallback policy mismatch")
    if "mode" in case and row.get("client_mode") != case["mode"]:
        errors.append("client mode mismatch")
    if row.get("fallback_count") != 0:
        errors.append("missing/nonzero fallback count")
    if row.get("case_id") != case["case_id"] or row.get("measure_s") != case["duration_secs"]:
        errors.append("case identity/duration mismatch")
    for name in ("loader_count", "connection_count", "payload_bytes"):
        if row.get(name) != case[name]:
            errors.append(f"workload mismatch: {name}")
    if row.get("latency_count") != row.get("completed_in_window"):
        errors.append("latency/completion count mismatch")
    if case.get("payload_mix"):
        parameters = row.get("parameters") or {}
        if (parameters.get("payload_mix") != case["payload_mix"]
                or parameters.get("payload_seed") != case.get("payload_seed", 1)):
            errors.append("payload mix/seed mismatch")
        errors.extend(validate_sizes(row, case))
    measure, completed = row.get("measure_s", 0), row.get("completed_in_window", 0)
    tps = row.get("rpc_tps")
    if (type(tps) not in (int, float) or not math.isfinite(tps)
            or type(measure) is not int or measure <= 0 or type(completed) is not int
            or not math.isclose(tps, completed / measure, rel_tol=1e-9, abs_tol=1e-6)):
        errors.append("TPS does not use the measured completion window")
    latency = [row.get(name) for name in ("min_ns", "p50_ns", "p90_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns")]
    if any(type(value) is not int or value < 0 for value in latency) or latency != sorted(latency):
        errors.append("invalid/missing latency bounds")
    if transport == "rdma":
        try:
            received = row["server_after"]["rdma"]["rpc_frames_received"] - row["server_before"]["rdma"]["rpc_frames_received"]
            if received != row["submitted"]:
                errors.append("server request total does not match accepted submissions")
        except (KeyError, TypeError):
            errors.append("missing server request totals")
        connections = row.get("client_connections", [])
        if not isinstance(connections, list) or not all(isinstance(conn, dict) for conn in connections):
            return errors + ["missing/malformed connection statuses"]
        qpairs = [(conn.get("info") or {}).get("qpair_number") if isinstance(conn.get("info"), dict)
                  else None for conn in connections]
        if (len(qpairs) != case["connection_count"] or any(type(qp) is not int or qp <= 0 for qp in qpairs)
                or len(set(qpairs)) != len(qpairs)
                or any(conn.get("path") != "rdma" or conn.get("open") is not True for conn in connections)
                or row.get("qpair_count") != case["connection_count"]):
            errors.append("actual unique QP count/path mismatch")
    if row.get("valid") is True and (row.get("errors_total") != 0 or not completed):
        errors.append("successful report contradicts errors/completions")
    return errors


def flatten(value, prefix=""):
    result = {}
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            result.update(flatten(item, name))
        else:
            result[name] = ("null" if item is None else json.dumps(item, separators=(",", ":"))
                            if isinstance(item, (list, bool)) else item)
    return result


def write_tables(output, rows):
    flat = [flatten(row) for row in rows]
    leading = ["case_id", "repeat_index", "topology", "actual_path", "loader_count", "qpair_count",
               "rpc_tps", "avg_ns", "p90_ns", "p99_ns", "errors_total", "report_errors", "valid"]
    fields = leading + sorted(set().union(*(row.keys() for row in flat)) - set(leading))
    for suffix, delimiter in (("csv", ","), ("tsv", "\t")):
        with (output / f"results.{suffix}").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, delimiter=delimiter)
            writer.writeheader()
            writer.writerows(flat)
    (output / "results.json").write_text(json.dumps(dict(schema_version=1, results=rows), indent=2))


def print_row(row):
    print(f'{row["case_id"]}: TPS={row.get("rpc_tps")} avg_ns={row.get("avg_ns")} '
          f'p90_ns={row.get("p90_ns")} p99_ns={row.get("p99_ns")} '
          f'errors={row.get("errors_total")} report_errors={row.get("report_errors")} '
          f'valid={row["valid"]}', flush=True)
