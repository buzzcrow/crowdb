#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Run successive, fixed-duration RDMA RPC load profiles on the local HCA."""

import argparse
import csv
import datetime
import os
from pathlib import Path
import re
import signal
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32])
    parser.add_argument("--rq-depth", type=int, choices=[1024, 4096], default=1024)
    parser.add_argument("--window", type=int, default=32)
    parser.add_argument("--io-workers", type=int, help="hold I/O workers per peer fixed across profiles")
    parser.add_argument("--connections", type=int, help="hold QP connections fixed across profiles")
    parser.add_argument("--skip-serial", action="store_true", help="skip the one-in-flight baseline")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300 or not 1 <= args.window <= 1024:
        parser.error("seconds must be 1..300 and window must be 1..1024")
    if any(count < 1 or count > 32 for count in args.threads):
        parser.error("thread counts must be 1..32")
    if any(count is not None and not 1 <= count <= 32 for count in [args.io_workers, args.connections]):
        parser.error("I/O workers and connections must be 1..32")

    root = Path(__file__).resolve().parents[2]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = root / ".crowdb-runtime/artifacts/rdma" / stamp
    output.mkdir(parents=True)
    pixi = os.environ.get("PIXI_BIN", "/home/cj/.pixi/bin/pixi")
    runtime_env = os.environ.copy()
    runtime_env["CROWDB_RUNTIME_ROOT"] = str(output / "runtime")
    subprocess.run([pixi, "run", "clean-env"], cwd=root, env=runtime_env, check=True)
    profiles = [] if args.skip_serial else [(1, 1)]
    profiles.extend((count, args.window) for count in args.threads)
    print(f"Timed RDMA load artifacts: {output}", flush=True)

    with (output / "results.csv").open("w", newline="") as results:
        writer = None
        for index, (count, window) in enumerate(profiles):
            env = runtime_env.copy()
            env.update({
                "CROWDB_RDMA_ENABLED": "1",
                "CROWDB_RDMA_ALLOW_TCP_FALLBACK": "0",
                "CROWDB_RDMA_RPC_E2E": "1",
                "CROWDB_RDMA_E2E_FILTER": "RdmaHardwareTest.RpcServerClientTimedLoad",
                "CROWDB_RDMA_TEST_SECONDS": str(args.seconds),
                "CROWDB_RDMA_TEST_WARMUP_SECONDS": "1",
                "CROWDB_RDMA_TEST_PRODUCERS": str(count),
                "CROWDB_RDMA_TEST_WORKERS": str(args.io_workers or count),
                "CROWDB_RDMA_TEST_CONNECTIONS": str(args.connections or count),
                "CROWDB_RDMA_TEST_WINDOW": str(window),
                "CROWDB_RDMA_RQ_DEPTH": str(args.rq_depth),
                "CROWDB_RDMA_POSTED_RECEIVE_TARGET": str(args.rq_depth),
                "CROWDB_RDMA_E2E_PORT": str(19201 + index),
            })
            command = [pixi, "run", "--", "bash", "tools/rdma/run-local-loop.sh", "local"]
            log_path = output / f"{index:02d}-{count}threads-{window}inflight.log"
            with log_path.open("w") as log:
                process = subprocess.Popen(command, cwd=root, env=env, stdout=log,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    returncode = process.wait(timeout=args.seconds + 45)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=10)
                    returncode = 124
            text = log_path.read_text()
            print(text, end="", flush=True)
            if returncode != 0:
                print(f"Profile failed: exit={returncode}, full log={log_path}", file=sys.stderr)
                return returncode
            matches = re.findall(r"^RDMA timed load: (.+)$", text, re.MULTILINE)
            if len(matches) != 1:
                raise RuntimeError(f"expected one timed load result in {log_path}")
            row = dict(field.split("=", 1) for field in matches[0].split())
            row.update(rq_depth=args.rq_depth, sq_depth=env.get("CROWDB_RDMA_SQ_DEPTH", "1024"),
                       wire_slot_capacity=env.get("CROWDB_RDMA_WIRE_SLOT_CAPACITY", "1024"), log=log_path.name)
            if writer is None:
                writer = csv.DictWriter(results, fieldnames=list(row))
                writer.writeheader()
            writer.writerow(row)
            results.flush()
    print(f"Completed timed RDMA load: {output / 'results.csv'}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
