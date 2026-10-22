#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""One manifest, CLI workload and report contract for TCP/RDMA regressions."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys

from rpc_bench_cases import client_arguments, load_cases, server_environment
from rpc_bench_process import Runtime, ready, start, wait_client
from rpc_bench_report import identity, print_row, provider_counters, validate, write_tables


ROOT = Path(__file__).resolve().parents[2]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("rdma", "tcp"), default="rdma")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--server-bin", type=Path)
    parser.add_argument("--cli-bin", type=Path)
    parser.add_argument("--address")
    parser.add_argument("--device", default=os.environ.get("CROWDB_RDMA_DEVICE", "rocep35s0f0"))
    parser.add_argument("--hca-port", type=int, default=int(os.environ.get("CROWDB_RDMA_PORT", "1")))
    parser.add_argument("--port", type=int, default=19101 + int(os.environ.get("CROWDB_PORT_OFFSET", "0")))
    parser.add_argument("--topology", choices=("native", "containers", "containers-no-rdma"), default="native")
    parser.add_argument("--image", default=os.environ.get("CROWDB_RDMA_TEST_IMAGE", "crowdb-node:dev"))
    parser.add_argument("--client-cpus")
    parser.add_argument("--server-cpus")
    parser.add_argument("--peak-repeats", type=int, default=int(os.environ.get("RPC_BENCH_PEAK_REPEATS", "3")))
    parser.add_argument("--startup-timeout", type=float, default=10)
    args = parser.parse_args()
    prefix = "RDMA" if args.transport == "rdma" else "RPC"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    runtime = Path(os.environ.get("CROWDB_RUNTIME_ROOT", ROOT / ".crowdb-runtime"))
    args.output_dir = (args.output_dir or Path(os.environ.get(f"{prefix}_BENCH_LOG_ROOT",
                         runtime / f"artifacts/bench/{args.transport}-rpc-{stamp}"))).resolve()
    args.server_bin = (args.server_bin or Path(os.environ.get(f"{prefix}_BENCH_SERVER_BIN",
                        ROOT / "lib/crowdb-rpc/build/crowdb-rpc-fb-server"))).resolve()
    args.cli_bin = (args.cli_bin or Path(os.environ.get(f"{prefix}_BENCH_CLI_BIN", ROOT / "target/release/crowdb-rpc-test"))).resolve()
    args.address = args.address or (os.environ.get("CROWDB_RDMA_SOURCE_ADDR", "10.200.1.1")
                                    if args.transport == "rdma" else "127.0.0.1")
    if args.peak_repeats < 0 or not 0 < args.port <= 65535:
        parser.error("invalid peak repeats or service port")
    return args


def run_case(args, case, index, metadata):
    directory = args.output_dir / f"case-{index:03d}"
    directory.mkdir()
    runtime = Runtime(ROOT, args.output_dir, shutil.which("pixi"), args.topology, args.image)
    env = server_environment(case, args.transport, args.address, args.device, args.hca_port, args.port, directory / "runtime")
    status_file, report_file = directory / "server-startup.json", directory / "client-result.json"
    server_argv = [args.server_bin, "--host", args.address, "--port", args.port,
                   "--io_engines", case["io_engines"], "--io_workers", case["io_workers"],
                   "--send_queue_capacity", case["send_queue_capacity"], "--status_file", status_file,
                   "--metrics_interval", case["metrics_interval"], "--logdir", directory / "server-log"]
    if args.transport == "rdma":
        server_argv.append("--require_rdma")
    if case["enable_nagle"]:
        server_argv.append("--enable_nagle")
    server_command = runtime.command(server_argv, env, "server", args.server_cpus)
    client_argv = [args.cli_bin, "--log-dir", directory / "client-log", "--mode", "coroutine",
                   "--server-host", args.address, "--server-port", args.port, "--output", report_file,
                   "--device", args.device, "--source-address", args.address, "--hca-port", args.hca_port,
                   *client_arguments(case)]
    if args.transport == "rdma":
        client_argv.append("--rdma")
    client_command = runtime.command(client_argv, env, "client", args.client_cpus)
    (directory / "commands.json").write_text(json.dumps(dict(server=server_command, client=client_command,
                                                              parameters=case), indent=2))
    row = dict(case_id=case["case_id"], repeat_index=case.get("repeat_index", 1), schema_version=1,
               valid=False, errors_total=None, report_errors=0, setup_errors=0,
               client_exit=None, server_exit=None, failure=None)
    server = client = None
    startup, failure = None, None
    before = provider_counters(args.device, args.hca_port)
    try:
        with (directory / "server.log").open("w") as server_log, (directory / "client.log").open("w") as client_log:
            server = start(server_command, env, server_log, ROOT)
            startup = ready(server, status_file, args.transport, args.startup_timeout)
            runtime.server_pid = startup.get("process_id")
            client = start(client_command, env, client_log, ROOT)
            wait_client(client, server, case["warmup_secs"] + case["duration_secs"] + 20 + case["connection_count"] * 3)
            report = json.loads(report_file.read_text())
            if not isinstance(report, dict):
                raise ValueError("client result must be an object")
            row |= report
            problems = validate(row, case, args.transport)
            if problems:
                previous = row.get("report_errors")
                row["report_errors"] = (previous if type(previous) is int and previous >= 0 else 0) + len(problems)
                failure = "; ".join(problems)
    except (OSError, ValueError, RuntimeError) as error:
        failure = str(error)
        category = "setup_errors" if client is None else "report_errors"
        row[category] = (row.get(category) or 0) + 1
    finally:
        if client is not None:
            row["client_exit"] = runtime.stop(client, "client")
        if server is not None:
            row["server_exit"] = runtime.stop(server, "server")
    row.update(metadata)
    row.update(server_startup=startup, provider_counters_before=before,
               provider_counters_after=provider_counters(args.device, args.hca_port),
               topology=args.topology, physical_hosts=1, parameters=case)
    row["server_request_frames"] = None
    if args.transport == "rdma":
        try:
            row["server_request_frames"] = row["server_after"]["rdma"]["rpc_frames_received"] - row["server_before"]["rdma"]["rpc_frames_received"]
        except (KeyError, TypeError):
            pass
    after = row["provider_counters_after"] or {}
    row["rnr_provider_counters"] = {}
    for name, value in after.items():
        if "rnr" in name.lower():
            previous = (before or {}).get(name)
            delta = value - previous if type(value) is int and type(previous) is int and value >= previous else None
            row["rnr_provider_counters"][name] = dict(before=previous, after=value, delta=delta)
    row["rnr_provider_counters"] = row["rnr_provider_counters"] or None
    row["valid"] = (row.get("valid") is True and row.get("errors_total") == 0
                    and row.get("report_errors") == 0 and row["client_exit"] == 0
                    and row["server_exit"] == 0 and failure is None)
    if failure is not None:
        row["runner_failure"] = failure
    if not row["valid"]:
        print(f'FAILED CASE {case["case_id"]}: {failure}; client_exit={row["client_exit"]} '
              f'server_exit={row["server_exit"]}', file=sys.stderr)
        for path in (directory / "server.log", directory / "client.log"):
            if path.exists():
                print(f"{path}:\n{path.read_text()}", file=sys.stderr)
    (directory / "result.json").write_text(json.dumps(row, indent=2))
    return row


def main():
    args = arguments()
    args.output_dir.mkdir(parents=True)
    cases = load_cases(args.manifest, args.transport)
    (args.output_dir / "manifest.json").write_text(json.dumps(dict(schema_version=1, cases=cases), indent=2))
    metadata = identity(ROOT, dict(server=args.server_bin, client=args.cli_bin), args.device, args.hca_port)
    rows = []
    for case in cases:
        row = run_case(args, case, len(rows), metadata)
        rows.append(row)
        write_tables(args.output_dir, rows)
        print_row(row)
        if not row["valid"]:
            return 1
    peak = max(zip(rows, cases), key=lambda pair: pair[0]["rpc_tps"])[1]
    for repeat in range(args.peak_repeats):
        case = peak | dict(case_id=peak["case_id"] + "-peak", repeat_index=repeat + 1)
        row = run_case(args, case, len(rows), metadata)
        rows.append(row)
        write_tables(args.output_dir, rows)
        print_row(row)
        if not row["valid"]:
            return 1
    print(f"Results: {args.output_dir / 'results.csv'}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(2)
