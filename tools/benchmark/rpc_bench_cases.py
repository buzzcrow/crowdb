# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Shared case resolution for native and container TCP/RDMA benchmarks."""

import json
import os


DEFAULTS = dict(loader_count=1, connection_count=1, payload_bytes=128,
                io_engines=1, io_workers=1, cq_count=1, sq_depth=1024,
                rq_depth=1024, posted_receive_target=1024, wire_slot_capacity=1024,
                poll_batch=256, verbs_poll_batch=32, poll_empty_budget=256,
                send_queue_capacity=4096, aggregation_enabled=False,
                aggregation_max_frames=256, enable_nagle=False,
                warmup_secs=1, duration_secs=10, metrics_interval=1,
                request_timeout_secs=5, registered_memory_budget_bytes=0)

ENV_FIELDS = dict(cq_count="CQ_COUNT", cq_depth="CQ_DEPTH", sq_depth="SQ_DEPTH",
                  send_batch_size="SEND_BATCH_SIZE", send_window_size="SEND_WINDOW_SIZE",
                  receive_batch_size="RECEIVE_BATCH_SIZE",
                  send_window_bytes="SEND_WINDOW_BYTES",
                  rq_depth="RQ_DEPTH", posted_receive_target="POSTED_RECEIVE_TARGET",
                  wire_slot_capacity="WIRE_SLOT_CAPACITY", send_arena_bytes="SEND_ARENA_BYTES",
                  receive_arena_bytes="RECEIVE_ARENA_BYTES", poll_batch="POLL_BATCH",
                  verbs_poll_batch="VERBS_POLL_BATCH", poll_empty_budget="POLL_EMPTY_BUDGET",
                  send_queue_capacity="SEND_QUEUE_CAPACITY", aggregation_enabled="AGGREGATION_ENABLED",
                  aggregation_max_bytes="AGGREGATION_MAX_BYTES", aggregation_max_frames="AGGREGATION_MAX_FRAMES",
                  registered_memory_budget_bytes="REGISTERED_MEMORY_BUDGET_BYTES")


def resolve(case, defaults=None):
    resolved = DEFAULTS | (defaults or {}) | case
    if resolved.get("mode", "coroutine") not in ("coroutine", "tokio"):
        raise ValueError("invalid client mode")
    layout = resolved.get("wire_layout", "contiguous")
    if layout not in ("contiguous", "control-data"):
        raise ValueError("invalid wire layout")
    if layout == "control-data" and resolved["wire_slot_capacity"] <= 1024:
        raise ValueError("control-data requires payload capacity")
    qps, cqs = resolved["connection_count"], resolved["cq_count"]
    if not 0 < cqs <= 256 or not 0 < qps <= resolved["loader_count"]:
        raise ValueError("invalid CQ, QP/connection or loader count")
    slot = resolved["wire_slot_capacity"]
    if not resolved.get("cq_depth"):
        resolved["cq_depth"] = ((qps + cqs - 1) // cqs) * (resolved["sq_depth"] + resolved["posted_receive_target"])
    if not resolved.get("send_arena_bytes"):
        resolved["send_arena_bytes"] = slot * resolved["sq_depth"]
    if not resolved.get("receive_arena_bytes"):
        resolved["receive_arena_bytes"] = slot * resolved["posted_receive_target"]
    if not resolved.get("aggregation_max_bytes"):
        resolved["aggregation_max_bytes"] = min(slot, 1 << 20)
    return resolved


def load_cases(path, transport):
    prefix = "RDMA" if transport == "rdma" else "RPC"
    defaults = dict(duration_secs=int(os.environ.get(f"{prefix}_BENCH_DURATION", "10")),
                    payload_bytes=int(os.environ.get(f"{prefix}_BENCH_VALUE_SIZE", "128")))
    if path:
        manifest = json.loads(path.read_text())
        if manifest.get("schema_version") != 1:
            raise ValueError("unsupported case manifest schema")
        defaults |= manifest.get("defaults", {})
        cases = manifest["cases"]
    else:
        cases = [dict(case_id=f"loaders-{n}", loader_count=n) for n in (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)]
        if transport == "rdma":
            cases += [dict(case_id=f"qp-{n}-agg-{int(agg)}", loader_count=512, connection_count=n,
                           aggregation_enabled=agg) for agg in (False, True) for n in (1, 2, 4, 8)]
            cases += [dict(case_id=f"cq-{n}", loader_count=512, connection_count=8,
                           cq_count=n, aggregation_enabled=True) for n in (2, 4, 8)]
            cases += [dict(case_id="rq-4096", loader_count=512, connection_count=4,
                           cq_count=4, rq_depth=4096, posted_receive_target=4096, aggregation_enabled=True)]
            cases += [dict(case_id=f"slot-{slot}", loader_count=512, aggregation_enabled=True,
                           wire_slot_capacity=slot) for slot in (65536, 262144)]
        else:
            cases += [dict(case_id=f"connections-{n}-nagle-{int(nagle)}", loader_count=512,
                           connection_count=n, enable_nagle=nagle) for nagle in (False, True) for n in (1, 2, 4, 8)]
            cases += [dict(case_id=f"workers-{n}", loader_count=1000, connection_count=32,
                           io_workers=n, enable_nagle=True) for n in (2, 4, 8, 16, 32)]
    selected = set(os.environ.get(f"{prefix}_BENCH_CASES", "").split())
    result = [resolve(case, defaults) for case in cases if not selected or case["case_id"] in selected]
    names = [case["case_id"] for case in result]
    if not result or len(set(names)) != len(names):
        raise ValueError("case selection is empty or contains duplicate case_id values")
    return result


def server_environment(case, transport, address, device, hca_port, port, runtime):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CROWDB_RDMA_")}
    env.update(CROWDB_RUNTIME_ROOT=str(runtime), CROWDB_RDMA_ENABLED=str(int(transport == "rdma")),
               CROWDB_LOG_ROOT=str(runtime / "logs"),
               CROWDB_RDMA_ALLOW_TCP_FALLBACK="0", CROWDB_RDMA_E2E_PORT=str(port),
               CROWDB_RDMA_DEVICE=device, CROWDB_RDMA_SOURCE_ADDR=address,
               CROWDB_RDMA_PORT=str(hca_port), CROWDB_PORT_OFFSET="0")
    env["CROWDB_RDMA_WIRE_LAYOUT"] = str(int(case.get("wire_layout") == "control-data"))
    for field, suffix in ENV_FIELDS.items():
        if field not in case:
            continue
        value = case[field]
        env[f"CROWDB_RDMA_{suffix}"] = str(int(value)) if isinstance(value, bool) else str(value)
    return env


def client_arguments(case):
    keys = set(DEFAULTS) | set(ENV_FIELDS) | {"case_id", "repeat_index", "payload_mix", "payload_seed",
                                            "wire_layout", "registered_payload"}
    args = []
    for key in sorted(keys):
        if key not in case:
            continue
        value = case[key]
        flag = "--" + key.replace("_", "-")
        if isinstance(value, bool):
            if value:
                args.append(flag)
        else:
            args += [flag, str(value)]
    return args
