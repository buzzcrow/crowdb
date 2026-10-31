# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import sys, json, time
from concurrent.futures import ThreadPoolExecutor
import requests
from pathlib import Path
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

control = Path(sys.argv[5]) if len(sys.argv) > 5 else None

def run(worker):
    samples = {}
    windows = {}
    def measure(operation, call):
        begin = time.time_ns() if len(sys.argv) > 2 else 0
        started = time.perf_counter_ns()
        result = call()
        samples.setdefault(operation, []).append(time.perf_counter_ns() - started)
        if begin:
            windows.setdefault(operation, []).append([begin, time.time_ns()])
        return result
    catalog = load_catalog(f"gc-{worker}", type="rest", uri=sys.argv[1], token="w" * 32)
    prefix = sys.argv[4] if len(sys.argv) > 4 else "gc-pressure"
    namespace = (f"{prefix}-{worker}",)
    measure("create_namespace", lambda: catalog.create_namespace(namespace))
    if control:
        (control / f"ready-{worker}").touch()
        while not (control / "run").exists():
            if (control / "stop").exists():
                raise RuntimeError("foreground cancelled before release")
            time.sleep(0.01)
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else 0
    # Keep paced workers spread across a cycle so a short deletion interval
    # cannot repeatedly fall into all four workers' inter-table gaps.
    if interval:
        time.sleep(worker * interval / 4)
    index = 0
    minimum = count if count else 3
    while index < minimum or (control and not (control / "stop").exists()):
        identifier = namespace + (f"events-{index}",)
        table = measure("create_table", lambda: catalog.create_table(identifier, Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True))))
        measure("commit", lambda: table.transaction().set_properties({"gc-probe": str(index)}).commit_transaction())
        loaded = measure("load_table", lambda: catalog.load_table(identifier))
        assert loaded.properties["gc-probe"] == str(index)
        response = measure("credentials", lambda: requests.get(
            f"{sys.argv[1]}/v1/namespaces/{namespace[0]}/tables/{identifier[1]}/credentials",
            headers={"Authorization": "Bearer " + "w" * 32},
            timeout=5,
        ))
        response.raise_for_status()
        loaded.io.properties.update(response.json()["storage-credentials"][0]["config"])
        def read_metadata():
            with loaded.io.new_input(loaded.metadata_location).open() as stream:
                return stream.read()
        assert measure("read_metadata", read_metadata).startswith(b"{")
        measure("drop_table", lambda: catalog.drop_table(identifier))
        index += 1
        if control:
            temporary = control / f"progress-{worker}.tmp"
            temporary.write_text(str(index))
            temporary.replace(control / f"progress-{worker}")
        if len(sys.argv) > 3:
            time.sleep(interval)
    measure("drop_namespace", lambda: catalog.drop_namespace(namespace))
    return samples, windows

with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(run, range(4)))
combined = {}
combined_windows = {}
for result, windows in results:
    for operation, samples in result.items():
        combined.setdefault(operation, []).extend(samples)
        combined_windows.setdefault(operation, []).extend(windows.get(operation, []))
report = {}
for operation, samples in combined.items():
    ordered = sorted(samples)
    report[operation] = {"raw_ns":samples,"count":len(samples),"p50_ns":ordered[(len(ordered)-1)//2],"p99_ns":ordered[(99*len(ordered)+99)//100-1],"windows_unix_ns":combined_windows[operation]}
print("gc_foreground_official_sdk " + json.dumps(report), flush=True)
