# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Loopback or remote S3 baseline; emits raw JSON samples and summary to stdout."""

import argparse
import json
import os
import platform
import resource
import time
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5
from urllib.request import urlopen

import boto3
from botocore.config import Config


def percentile(samples, percentage):
    # Do not give a one-sample smoke run a fabricated tail-latency estimate.
    minimum = {50: 1, 95: 20, 99: 100}[percentage]
    if len(samples) < minimum:
        return None
    ordered = sorted(samples)
    position = (len(ordered) - 1) * percentage / 100
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def metrics(endpoint):
    try:
        with urlopen(f"{endpoint}/_crowdb/metrics", timeout=5) as response:
            return response.read().decode("ascii")
    except OSError:
        return None  # A remote S3 endpoint need not expose CROWDB telemetry.


def process_snapshot(pid):
    if pid is None:
        return None  # Remote endpoints do not have a local access-server PID.
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii") as stat_file:
            stat = stat_file.read().rsplit(") ", maxsplit=1)[1].split()
        with open(f"/proc/{pid}/status", encoding="ascii") as status_file:
            status = status_file.read().splitlines()
    except OSError:
        return None
    clock_ticks = os.sysconf("SC_CLK_TCK")
    memory_kb = {
        name: int(line.split()[1])
        for line in status
        for name in ("VmRSS", "VmHWM")
        if line.startswith(f"{name}:")
    }
    return {
        "cpu_user_seconds": int(stat[11]) / clock_ticks,
        "cpu_system_seconds": int(stat[12]) / clock_ticks,
        "rss_kb": memory_kb.get("VmRSS"),
        "peak_rss_kb": memory_kb.get("VmHWM"),
    }


def exercise(client, bucket, size, index):
    key = f"bench/{size}/{index}"
    payload = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    result = {"size": size, "key": key}
    start = time.perf_counter_ns()
    put = client.put_object(Bucket=bucket, Key=key, Body=payload)
    result["put_ns"] = time.perf_counter_ns() - start
    assert put["ETag"] == f'"{md5(payload).hexdigest()}"'

    start = time.perf_counter_ns()
    response = client.get_object(Bucket=bucket, Key=key)
    first = response["Body"].read(1)
    result["get_ttfb_ns"] = time.perf_counter_ns() - start
    assert first + response["Body"].read() == payload
    result["get_ns"] = time.perf_counter_ns() - start

    start = time.perf_counter_ns()
    response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{min(size, 4096) - 1}")
    first = response["Body"].read(1)
    result["range_ttfb_ns"] = time.perf_counter_ns() - start
    assert first + response["Body"].read() == payload[:4096]
    result["range_ns"] = time.perf_counter_ns() - start
    client.delete_object(Bucket=bucket, Key=key)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--sizes", default="65536,1048576,4194304")
    parser.add_argument("--concurrency", default="1,4")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--server-pid", type=int, help="local access-server PID for CPU/RSS sampling")
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be positive")
    sizes = [int(value) for value in args.sizes.split(",")]
    concurrencies = [int(value) for value in args.concurrency.split(",")]
    if not sizes or min(sizes) < 1 or not concurrencies or min(concurrencies) < 1:
        parser.error("sizes and concurrency must contain positive values")
    client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        region_name=os.environ.get("CROWDB_S3_E2E_REGION", "us-east-1"),
        aws_access_key_id=os.environ["CROWDB_S3_E2E_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CROWDB_S3_E2E_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = f"crowdb-e2e-bench-{os.getpid()}"
    before = metrics(args.endpoint)
    server_before = process_snapshot(args.server_pid)
    initial_usage = resource.getrusage(resource.RUSAGE_SELF)
    client.create_bucket(Bucket=bucket)
    samples = []
    try:
        for size in sizes:
            for concurrency in concurrencies:
                with ThreadPoolExecutor(max_workers=concurrency) as workers:
                    results = list(
                        workers.map(
                            lambda index: exercise(client, bucket, size, index),
                            range(args.samples * concurrency),
                        )
                    )
                samples.extend({**item, "concurrency": concurrency} for item in results)
    finally:
        client.delete_bucket(Bucket=bucket)
    final_usage = resource.getrusage(resource.RUSAGE_SELF)
    server_after = process_snapshot(args.server_pid)
    server_process = None
    if server_before is not None and server_after is not None:
        server_process = {
            "pid": args.server_pid,
            "cpu_user_seconds": server_after["cpu_user_seconds"] - server_before["cpu_user_seconds"],
            "cpu_system_seconds": server_after["cpu_system_seconds"] - server_before["cpu_system_seconds"],
            "rss_before_kb": server_before["rss_kb"],
            "rss_after_kb": server_after["rss_kb"],
            "peak_rss_kb": server_after["peak_rss_kb"],
        }
    summaries = []
    for size in sizes:
        for concurrency in concurrencies:
            group = [item for item in samples if item["size"] == size and item["concurrency"] == concurrency]
            summaries.append({
                "size": size,
                "concurrency": concurrency,
                "count": len(group),
                **{
                    name: {"p50": percentile(values, 50), "p95": percentile(values, 95), "p99": percentile(values, 99)}
                    for name in ("put_ns", "get_ns", "get_ttfb_ns", "range_ns", "range_ttfb_ns")
                    for values in ([item[name] for item in group],)
                },
            })
    print(json.dumps({
        "endpoint": args.endpoint,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "client_cpu_user_seconds": final_usage.ru_utime - initial_usage.ru_utime,
        "client_cpu_system_seconds": final_usage.ru_stime - initial_usage.ru_stime,
        "client_peak_rss_kb": final_usage.ru_maxrss,
        "server_process": server_process,
        "samples": samples,
        "summary": summaries,
        "metrics_before": before,
        "metrics_after": metrics(args.endpoint),
    }, indent=2))


if __name__ == "__main__":
    main()
