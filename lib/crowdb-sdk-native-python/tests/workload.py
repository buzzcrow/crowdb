# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native Python comparison; to_bytes copies are included in measured CPU."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import time

import crowdb_dataset_native as native
from real_consumer import SIZES


def read(client, snapshot, index):
    start = time.perf_counter_ns()
    batch = client.read_batch(snapshot, [list(f"size-{index}".encode())], ["value"])
    header = json.loads(batch.header)
    assert bytes(header["snapshot"]) == snapshot and len(header["fields"]) == 1
    length = 0
    while (lease := batch.next()) is not None:
        data = lease.to_bytes()
        assert data == bytes([index]) * len(data)
        length += len(data)
    assert length == SIZES[index]
    return (time.perf_counter_ns() - start) // 1000


def trial(client, snapshot, concurrency, rounds):
    def lane(number):
        observations = []
        for repeat in range(rounds):
            for order in range(len(SIZES)):
                index = (order * 5 + number + repeat) % len(SIZES)
                observations.append((index, read(client, snapshot, index)))
        return observations

    cpu = time.process_time()
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as workers:
        results = list(workers.map(lane, range(concurrency)))
    seconds = time.perf_counter() - start
    cpu = time.process_time() - cpu
    times = [[] for _ in SIZES]
    for result in results:
        for index, micros in result:
            times[index].append(micros)
    latencies = []
    for size, values in zip(SIZES, times):
        values.sort()
        latencies.append(dict(bytes=size, count=len(values), p50_us=values[(len(values)-1)//2],
                              p99_us=values[(len(values)*99+99)//100-1]))
    return dict(seconds=seconds, completed_requests=concurrency * rounds * len(SIZES),
                bytes=concurrency * rounds * sum(SIZES), cpu_ticks=cpu * os.sysconf("SC_CLK_TCK"),
                rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, latency=latencies, errors=0)



def observation_trial(snapshot, concurrency, repeat):
    run = os.environ.get("CROWDB_DATASET_OBSERVATION_RUN_ID")
    if not run:
        return None
    if any(len(value) != 32 or any(char not in "0123456789abcdef" for char in value)
           for value in (run, snapshot)):
        raise ValueError("invalid workload observation identity")
    return dict(run_id=run, path="native-python", concurrency=concurrency,
                repeat=repeat, snapshot=snapshot)


def local_observation(runtime):
    started = str(time.time_ns())
    values, unavailable = {}, {}
    for name, capture in (("topology", lambda: runtime.topology(refresh=False)),
                          ("resources", runtime.status)):
        try:
            values[name] = json.loads(capture())
            if not isinstance(values[name], dict):
                raise ValueError("native status is not an object")
        except Exception as error:
            values[name] = None
            unavailable[name] = str(error)
    return dict(started_unix_ns=started, finished_unix_ns=str(time.time_ns()),
                topology=values["topology"], resources=values["resources"], unavailable=unavailable)


def observe(trial, event, reply, runtime, result=None):
    if trial is None:
        return
    record = dict(v=1, event=event, trial=trial, observation=local_observation(runtime))
    if result is not None:
        record["result"] = result
    line = "CROWDB_DATASET_OBSERVATION " + json.dumps(record, separators=(",", ":")) + "\n"
    if len(line.encode()) > 1024 * 1024:
        raise ValueError("workload observation exceeds the line limit")
    sys.stdout.write(line)
    sys.stdout.flush()
    line = sys.stdin.readline(1024 * 1024 + 1)
    if len(line.encode()) > 1024 * 1024 or not line.endswith("\n"):
        raise ValueError("missing or oversized workload observation reply")
    if json.loads(line) != dict(v=1, event=reply, trial=trial):
        raise ValueError("mismatched workload observation reply")


def main():
    root = Path(os.environ["CROWDB_DATASET_CONSUMER_ROOT"])
    snapshot = bytes.fromhex(root.joinpath("snapshot").read_text())
    runtime = native.NativeRuntime(os.environ["CROWDB_MANAGEMENT_SEEDS"].split(","))
    client = runtime.dataset(os.environ["CROWDB_DATASET_NAME"])
    try:
        for concurrency in (1, 3):
            trial(client, snapshot, concurrency, 2)
            for repeat in range(3):
                observation = observation_trial(snapshot.hex(), concurrency, repeat)
                observe(observation, "ready", "go", runtime)
                result = trial(client, snapshot, concurrency, 16)
                observe(observation, "done", "ack", runtime, result)
                data = bytes([93]) * 1024 * 1024
                start = time.perf_counter_ns()
                for _ in range(64):
                    hashlib.md5(data).digest()
                digest = dict(algorithm="md5", implementation="Python hashlib", bytes=64 * len(data),
                              microseconds=(time.perf_counter_ns()-start)//1000)
                report = dict(path="native-python", snapshot=snapshot.hex(), sizes=SIZES, concurrency=concurrency,
                              repeat=repeat, result=result, digest=digest, load="closed-loop", warmup_rounds=2,
                              in_flight_budget_bytes=256 * 1024 * 1024, python_bytes_copy="included")
                root.joinpath(f"native-python-{concurrency}-{repeat}.json").write_text(json.dumps(report, indent=2))
                print(json.dumps(report), flush=True)
        root.joinpath("native-python-resources.json").write_text(runtime.status())
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
