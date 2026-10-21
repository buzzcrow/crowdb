#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Own one real echo server for one bounded transport workload, including shutdown."""

import argparse
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


def child_environment(args, role):
    env = dict(os.environ)
    if args.asan_runtime:
        env["LD_PRELOAD"] = str(args.asan_runtime)
        env["ASAN_OPTIONS"] = f"detect_leaks=1:abort_on_error=0:log_path={args.asan_log_prefix}-{role}"
    return env


def wait_ready(server, path):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        content = path.read_text(errors="replace")
        match = re.search(r"^listening port=(\d+)$", content, re.MULTILINE)
        if match:
            return match.group(1)
        if server.poll() is not None:
            raise RuntimeError(f"echo server exited before readiness: {content}")
        time.sleep(0.01)
    raise RuntimeError(f"echo server readiness deadline: {path.read_text(errors='replace')}")


def run(args):
    args.log_dir.mkdir(parents=True, exist_ok=True)
    server_log = args.log_dir / "server.stdout.log"
    command = [str(args.server.resolve()), "--port=0", f"--io_engines={args.io_engines}",
               f"--io_workers={args.io_workers}", f"--logdir={args.log_dir.resolve() / 'server'}",
               f"--metrics_interval={args.metrics_interval}"]
    if args.enable_nagle:
        command.append("--enable_nagle")
    with server_log.open("w") as output:
        server = subprocess.Popen(command, stdout=output, stderr=output, env=child_environment(args, "server"))
        try:
            port = wait_ready(server, server_log)
            command = [str(args.client.resolve()), "--log-dir", str(args.log_dir / "client"),
                       "--server-port", port, "--duration-secs", str(args.duration_secs),
                       "--loader-num", str(args.loader_num), "--connections", str(args.connections),
                       "--value-size", str(args.value_size), "--io-engines", str(args.io_engines),
                       "--io-workers", str(args.io_workers), "--mode", args.mode,
                       "--metrics-interval", str(args.metrics_interval), "--json"]
            if args.enable_nagle:
                command.append("--enable-nagle")
            result = subprocess.run(command, env=child_environment(args, "client"), timeout=args.duration_secs + 30)
        finally:
            if server.poll() is None:
                server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
                raise RuntimeError("owned echo server did not shut down gracefully") from None
        if server.returncode:
            raise RuntimeError(f"echo server shutdown failed ({server.returncode}): {server_log.read_text(errors='replace')}")
        return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, default=Path("lib/crowdb-rpc/build/crowdb-rpc-fb-server"))
    parser.add_argument("--client", type=Path, default=Path("target/release/crowdb-rpc-test"))
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--duration-secs", type=int, default=10)
    parser.add_argument("--loader-num", type=int, default=1)
    parser.add_argument("--connections", type=int, default=1)
    parser.add_argument("--value-size", type=int, default=128)
    parser.add_argument("--io-engines", type=int, default=1)
    parser.add_argument("--io-workers", type=int, default=1)
    parser.add_argument("--metrics-interval", type=int, default=1)
    parser.add_argument("--mode", choices=("coroutine", "tokio"), default="coroutine")
    parser.add_argument("--enable-nagle", action="store_true")
    parser.add_argument("--asan-runtime", type=Path)
    parser.add_argument("--asan-log-prefix", type=Path)
    args = parser.parse_args()
    if min(args.duration_secs, args.loader_num, args.connections, args.value_size,
           args.io_engines, args.io_workers) < 1 or args.io_workers % args.io_engines:
        parser.error("positive workload parameters and evenly distributed I/O workers are required")
    if not args.server.is_file() or not args.client.is_file():
        parser.error("build the C++ echo server and crowdb-rpc-test client before running")
    if args.asan_runtime and (not args.asan_runtime.is_file() or not args.asan_log_prefix):
        parser.error("sanitizer runtime and log prefix must both be supplied")
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        return run(args)
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
