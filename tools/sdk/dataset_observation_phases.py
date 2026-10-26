# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Validate bounded client-visible Dataset read phases without timing the host."""

PHASE_KEYS = {"index", "header_us", "first_payload_us", "verified_eof_us", "complete_us",
              "bytes", "nonempty_deliveries", "explicit_consumer_copy_bytes"}


def _integer(value):
    return type(value) is int and value >= 0


def _percentiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50_us": None, "p99_us": None}
    return {"count": len(values), "p50_us": values[(len(values) - 1) // 2],
            "p99_us": values[(len(values) * 99 + 99) // 100 - 1]}


def summarize_phases(result, path, concurrency, sizes, rounds=16):
    phases = result.get("read_phases")
    if (not isinstance(phases, dict) or set(phases) != {"schema_version", "samples"}
            or type(phases["schema_version"]) is not int or phases["schema_version"] != 1
            or not isinstance(phases["samples"], list)
            or len(phases["samples"]) != concurrency * rounds * len(sizes)):
        raise ValueError("missing or invalid bounded read phase observations")
    grouped = [[] for _ in sizes]
    for row in phases["samples"]:
        if (not isinstance(row, dict) or set(row) != PHASE_KEYS
                or not all(_integer(row[key]) for key in PHASE_KEYS - {"first_payload_us"})
                or row["index"] >= len(sizes)):
            raise ValueError("invalid read phase sample shape")
        size = sizes[row["index"]]
        first = row["first_payload_us"]
        if (row["bytes"] != size or row["explicit_consumer_copy_bytes"] != (
                size if path == "native-python" else 0)
                or not row["header_us"] <= row["verified_eof_us"] <= row["complete_us"]
                or (size == 0 and (first is not None or row["nonempty_deliveries"] != 0))
                or (size != 0 and (not _integer(first) or row["nonempty_deliveries"] == 0
                                   or not row["header_us"] <= first <= row["verified_eof_us"]))):
            raise ValueError("read phases violate delivery, EOF, copy or size contract")
        grouped[row["index"]].append(row)
    latency = result.get("latency")
    if not isinstance(latency, list) or len(latency) != len(sizes):
        raise ValueError("missing original completion latency")
    summary = []
    for index, rows in enumerate(grouped):
        if len(rows) != concurrency * rounds:
            raise ValueError("read phases changed the per-size request count")
        complete = _percentiles([row["complete_us"] for row in rows])
        if (not isinstance(latency[index], dict)
                or not all(_integer(value) for value in latency[index].values())
                or latency[index] != {"bytes": sizes[index], **complete}):
            raise ValueError("phase completion differs from original completion latency")
        values = {key: _percentiles([row[key] for row in rows if row[key] is not None])
                  for key in ("header_us", "first_payload_us", "verified_eof_us", "complete_us")}
        summary.append({"bytes": sizes[index], "count": len(rows), "cumulative_from_start": values,
                        "nonempty_deliveries": sum(row["nonempty_deliveries"] for row in rows),
                        "explicit_consumer_copy_bytes": sum(row["explicit_consumer_copy_bytes"] for row in rows)})
    return {"scope": "client_visible_batch_header_first_nonempty_delivery_verified_eof",
            "timing": "cumulative_microseconds_not_additive_or_wire_first_byte",
            "instrumentation": "client_clocks_and_capture_included_post_trial_json_and_host_summary_excluded",
            "sizes": summary,
            "explicit_consumer_copy_bytes": sum(row["explicit_consumer_copy_bytes"]
                                                 for row in phases["samples"]),
            "not_measured": ["all_payload_copies", "wire_first_byte", "per_request_server_cpu"]}
