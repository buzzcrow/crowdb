#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
#
# Current baseline: 2026-10-24, Intel Core i9-7960X (16 cores / 32 logical CPUs).
# Retained MemDisk, mem-block KV/WAL, 2 synchronized mirrors, 32 Tokio workers.
# All fixtures/workloads/builds ran serially. No CPU power policy change.
# Affinity applies to the client AND every fixture service, sharing the same CPUs.
# default=0-31 (16 physical cores); taskset=0-3 (4 physical cores).
#
# Placement       Case          Seconds     ops/s      avg/p50/p99 us       CPU client/services %
# default         append_4k_1t   60.002    1641.33   608/594/1149              92.90/171.00
# default         append_4k_32t  60.016   15640.45  2044/2017/3818            111.11/206.81
# cpus_0_3        append_4k_1t   60.002    6110.25   163/149/338               92.91/170.88
# cpus_0_3        append_4k_32t  60.015   51258.14   623/513/3638             101.94/163.38
# CPU %: 100%=one logical CPU. Process CPU time covers the whole benchmark
# command, including stream/dataset preparation and teardown; workload latency
# and throughput exclude that preparation. Per-process affinities are verified
# from /proc; CPU timing is captured before/after, without hot-path sampling.
#
# Default-placement remaining cases: throughput, p99 us:
#   append_1m_8t         265.71 MiB/s, p99 67035 us
#   gc_576m            10599.53 MiB/s, p99 23790 us
#   random_4k_32t      46395.38 ops/s, p99 1707 us
#   replay_320m_1t       495.56 MiB/s, p99 665975 us
#   rollover_4m          307.26 MiB/s, p99 240149 us
# All nine cases pass original throughput/p99, queue/RSS and cleanup gates.
# Ordinary append: zero errors/watchdogs/cursor checkpoints; record publication
# count equals initial attachment plus rollovers. Trim may checkpoint its boundary.
# Benchmark SHA-256: 0ae700e68147f616ef613f993e0130e95274c5668bbd9d37c9311453f8235f29
# DiskIO SHA-256:    a41b81ccf358f51e917b3c9ea6e79164347183b5341f7a6cef675eb162bdf4bf
# Full raw phase/CPU/process/cleanup receipts under .crowdb-runtime/artifacts/bench/:
#   chunk-stream-regression-20261024T064043Z (default, seven cases)
#   chunk-stream-regression-20261024T064825Z (taskset 0-3, two 60-second cases)
# The measured tree includes uncommitted common C6 instrumentation; binary hashes
# identify the measured artifacts. This baseline is the current implementation.
#
# Retained-MemDisk Stream regression; fresh native fixture for each case.
# Run through: pixi run bash tools/benchmark/bench-chunk-stream-regression.sh
#
# Optional environment variables:
#   CHUNK_STREAM_BENCH_CASES       space-separated labels (default: all seven)
#   CHUNK_STREAM_BENCH_DURATION    large-write/read/replay seconds (default: 10)
#   CHUNK_STREAM_BENCH_APPEND_DURATION  one-/32-producer 4 KiB seconds (60)
#   CHUNK_STREAM_BENCH_TIMEOUT     per-case seconds, including setup data (180)
#   CHUNK_STREAM_BENCH_WORKERS     Tokio workers for client and services (32)
#   CHUNK_STREAM_BENCH_CPUS        optional taskset CPU list for both sides
#   CHUNK_STREAM_BENCH_BIN         current release binary; required with skip-build
#   CHUNK_STREAM_BENCH_SKIP_BUILD  1 reuses all release artifacts (default: 0)
#   CHUNK_STREAM_BENCH_LOG_ROOT    new/empty retained run directory
#   CHUNK_STREAM_BENCH_RESULTS     result TSV path
#
# This runs the current implementation as the regression baseline. Default
# CPU placement and optional affinity diagnostics are distinct conditions;
# record them with the results instead of treating their throughput as equal.
set -euo pipefail
cd "$(dirname "$0")/../.."

report() {
    pixi run python - "$@" <<'PY_STREAM_REGRESSION'
#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Strict result checks and retained reports for the Stream regression script."""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

CASES = {
    "append_4k_1t": ("append", 4096, 1, 0, "ops_s", 250, 10000),
    "append_4k_32t": ("append", 4096, 32, 0, "ops_s", 3000, 20000),
    "append_1m_8t": ("append", 1048576, 8, 0, "mib_s", 125, 150000),
    "rollover_4m": ("append", 4194304, 8, 0, "mib_s", 200, 500000),
    "random_4k_32t": ("random-read", 4096, 32, 335544320, "ops_s", 30000, 5000),
    "replay_320m_1t": ("replay", 4096, 1, 335544320, "mib_s", 300, 2000000),
    "gc_576m": ("gc", 4096, 1, 603979776, "mib_s", 1000, 100000),
}
INTEGER_FIELDS = """operations bytes errors avg_us p50_us p99_us rss_start_kib
rss_end_kib rss_peak_kib max_queue_requests max_queue_bytes batches batch_requests
rollovers metadata_publications cache_hits cache_misses physical_reads reclaimed_bytes
watchdogs""".split()
FLOAT_FIELDS = ("seconds", "ops_s", "mib_s")
PHASE_FIELDS = ("count", "avg_us", "p50_us", "p99_us", "max_us", "total_us")
REQUIRED_PHASES = {
    "stream.append.e2e.lh", "stream.append.queue.lh", "stream.append.batch.lh",
    "stream.append.mirror.lh", "stream.capacity.wait.lh", "stream.successor.wait.lh",
    "stream.checkpoint.cursor.lh", "stream.metadata.publish.lh",
    "stream.append.capacity.lh", "stream.capacity.prepare.lh", "stream.successor.prepare.lh",
    "stream.capacity.grow.lh", "stream.capacity.initialize.lh", "stream.chunk.seal.lh",
    "stream.chunk.seal_drain.lh", "stream.append.encode.lh", "stream.append.local_view.lh",
    "stream.checkpoint.lh", "stream.metadata.record_build.lh", "stream.metadata.record_get.lh",
    "stream.metadata.record_scan.lh", "stream.metadata.record_cas.lh", "stream.read.e2e.lh",
    "stream.read.plan.lh", "stream.read.io.lh", "chunkio.diskio.write.e2e.lh",
    "rpc.request.e2e", "rpc.response.schedule.lh",
}


def fields(line, integers, floats, strings=()):
    value = {}
    for item in line.split():
        key, separator, raw = item.partition("=")
        if not separator or key in value:
            raise ValueError(f"malformed or duplicate field: {item}")
        value[key] = raw
    for key in (*integers, *floats, *strings):
        if key not in value:
            raise ValueError(f"missing field: {key}")
    for key in (*integers, *floats):
        number = int(value[key]) if key in integers else float(value[key])
        if number < 0 or not math.isfinite(number):
            raise ValueError(f"invalid nonnegative number: {key}={value[key]}")
        value[key] = number
    return value


def analyze(text, label, duration, status=0):
    lines = [line.removeprefix("chunk-stream: ") for line in text.splitlines()
             if line.startswith("chunk-stream: ")]
    if len(lines) != 1:
        raise ValueError(f"expected one completed result, found {len(lines)}")
    result = fields(lines[0], INTEGER_FIELDS, FLOAT_FIELDS, ("workload",))
    runtime_lines = [line.removeprefix("bench-runtime: ") for line in text.splitlines()
                     if line.startswith("bench-runtime: ")]
    if len(runtime_lines) != 1:
        raise ValueError("expected one benchmark runtime/affinity record")
    runtime = fields(runtime_lines[0], ("pid", "tokio_workers"), (), ("allowed_cpus",))
    phases = {}
    for line in text.splitlines():
        prefix = next((p for p in ("stream-phase: ", "client-phase: ") if line.startswith(p)), None)
        if prefix is None:
            continue
        phase = fields(line[len(prefix):], ("count", "p50_us", "p99_us", "max_us"),
                       ("avg_us",), ("name",))
        name = phase.pop("name")
        if name in phases:
            raise ValueError(f"duplicate phase: {name}")
        # Stages overlap; this cumulative time must not be summed as if every
        # histogram described an exclusive portion of each request.
        phase["total_us"] = phase["count"] * phase["avg_us"]
        phases[name] = phase
    missing = REQUIRED_PHASES - phases.keys()
    if missing:
        raise ValueError(f"missing phases: {sorted(missing)}")
    workload, size, _, _, rate, minimum, p99 = CASES[label]
    checks = {
        "completed": status == 0 and result["operations"] > 0 and result["seconds"] > 0,
        "workload": result["workload"] == workload,
        "no_errors": result["errors"] == 0 and result["watchdogs"] == 0,
        "full_duration": workload == "gc" or result["seconds"] >= duration - 0.001,
    }
    checks.update(
        queue=result["max_queue_requests"] <= 1024 and result["max_queue_bytes"] <= 67108864,
        rss=0 < result["rss_start_kib"] <= result["rss_peak_kib"]
        and 0 < result["rss_end_kib"] <= result["rss_peak_kib"]
        and result["rss_peak_kib"] - result["rss_start_kib"] <= 262144,
        throughput=result[rate] >= minimum,
        p99=result["p99_us"] <= p99,
        publication_accounting=phases["stream.metadata.publish.lh"]["count"]
        == result["metadata_publications"],
    )
    if workload == "append":
        checks.update(
            publications=result["metadata_publications"] == result["rollovers"] + 1,
            no_cursor_checkpoint=phases["stream.checkpoint.cursor.lh"]["count"] == 0,
            append_bytes=result["bytes"] == result["operations"] * size,
            completed_requests=result["batch_requests"] == result["operations"],
            batch_accounting=phases["stream.append.batch.lh"]["count"] == result["batches"],
            mirror_batches=phases["stream.append.mirror.lh"]["count"] == result["batches"],
            mirror_writes=phases["chunkio.diskio.write.e2e.lh"]["count"] >= 2 * result["batches"],
        )
        if label == "rollover_4m":
            checks["rollover"] = result["rollovers"] > 0
    elif workload in ("random-read", "replay"):
        multiplier = 32 if workload == "random-read" else 1
        checks["record_lookups"] = (result["physical_reads"] > 0 and result["cache_misses"]
                                    <= multiplier * (result["rollovers"] + 1))
        if workload == "random-read":
            checks["cache_hits"] = result["cache_hits"] > 0
    else:
        checks["reclaimed_chunk"] = result["reclaimed_bytes"] > 0
    return dict(case=label, runtime=runtime, result=result, phases=phases, checks=checks,
                passed=all(checks.values()))


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def initialize(args):
    root = Path.cwd()
    binaries = [args.binary, os.environ["CROWDB_REGRESSION_FIXTURE_BIN"],
                *(root / "target/release" / name for name in
                  ("crowdb-cli", "crowdb-kv-server", "crowdb-diskdb", "crowdb-chunkdb")),
                root / "app/crowdb-diskio/build/crowdb-diskio"]
    value = dict(commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                 dirty_paths=subprocess.check_output(["git", "status", "--short"], text=True).splitlines(),
                 binaries={str(Path(p).resolve()): digest(p) for p in binaries},
                 cpus=args.cpus or "default", inherited_cpus=sorted(os.sched_getaffinity(0)),
                 tokio_workers=int(os.environ["TOKIO_WORKER_THREADS"]), duration_secs=args.duration,
                 append_duration_secs=args.append_duration, mirror_copies=2, cases=args.cases.split(),
                 media="retained-MemDisk", kv_backend="mem-block", wal_backend="mem-block",
                 optional_checkpoints=False, fresh_fixture_per_case=True)
    topology = {}
    for cpu in os.sched_getaffinity(0):
        path = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        topology[cpu] = {name: (path / name).read_text().strip()
                         for name in ("core_id", "physical_package_id", "thread_siblings_list")}
    value["cpu_topology"] = topology
    (args.root / "conditions.json").write_text(json.dumps(value, indent=2) + "\n")


def record(args):
    try:
        value = analyze(args.log.read_text(), args.label, args.duration, args.status)
        value["cpu"] = json.loads((args.log.parent / "cpu.json").read_text())
        receipt = json.loads((args.log.parent / "console.fixture-stopped.json").read_text())
        value["checks"]["cleanup"] = receipt.get("ok") is True and receipt.get("error") is None
        value["passed"] = all(value["checks"].values())
    except (OSError, ValueError) as error:
        value = dict(case=args.label, result={}, phases={}, checks={},
                     passed=False, error=str(error))
    (args.log.parent / "result.json").write_text(json.dumps(value, indent=2) + "\n")
    if not value["passed"]:
        print(f"FAILED {args.label}: " + value.get("error", str(value["checks"])), file=sys.stderr)
    return 0 if value["passed"] else 1


def summarize(args):
    cases = [json.loads(path.read_text()) for path in sorted(args.root.glob("*/result.json"))]
    conditions = json.loads((args.root / "conditions.json").read_text())
    expected = set(conditions["cases"])
    complete = len(cases) == len(expected) and {c["case"] for c in cases} == expected
    if not complete:
        print(f"FAILED case inventory: expected {sorted(expected)}, received "
              f"{[c['case'] for c in cases]}", file=sys.stderr)
    value = dict(conditions=conditions, cases=cases,
                 complete=complete, passed=bool(cases) and complete and all(c["passed"] for c in cases))
    (args.root / "results.json").write_text(json.dumps(value, indent=2) + "\n")
    with args.results.open("w") as stream, (args.root / "phases.tsv").open("w") as phase_stream:
        results = csv.writer(stream, delimiter="\t")
        phases = csv.writer(phase_stream, delimiter="\t")
        columns = ["workload", *INTEGER_FIELDS, *FLOAT_FIELDS]
        results.writerow(["case", "passed", *columns])
        phases.writerow(["case", "name", *PHASE_FIELDS])
        for case in cases:
            results.writerow([case["case"], case["passed"],
                              *(case["result"].get(field, "") for field in columns)])
            for name, phase in case["phases"].items():
                phases.writerow([case["case"], name,
                                 *(phase[field] for field in PHASE_FIELDS)])
    print(json.dumps(dict(passed=value["passed"], results=str(args.results)), indent=2))
    return 0 if value["passed"] else 1


def cpu_snapshot(pid, start):
    proc = Path(f"/proc/{pid}")
    fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
    if fields[19] != start or fields[0] == "Z":
        raise ValueError(f"recorded benchmark service {pid} was replaced or exited")
    affinities = {}
    for task in (proc / "task").iterdir():
        try:
            mask = ",".join(map(str, sorted(os.sched_getaffinity(int(task.name)))))
        except ProcessLookupError:
            continue
        affinities[mask] = affinities.get(mask, 0) + 1
    return dict(pid=pid, start=start, executable=(proc / "exe").resolve().name,
                cpu_seconds=(int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                allowed_cpus=sorted(os.sched_getaffinity(pid)), thread_affinities=affinities)


def cpu_start(state):
    descriptor = json.loads(state.with_suffix(".fixture.json").read_text())
    namespace = json.loads((Path(descriptor["root"]) / "namespace.json").read_text())
    services = [cpu_snapshot(p["pid"], p["start"]) for p in namespace["processes"]]
    if not services:
        raise ValueError("fixture has no recorded service processes")
    (state.parent / "cpu-start.json").write_text(json.dumps(
        dict(monotonic=time.monotonic(), services=services), indent=2) + "\n")


def cpu_finish(state):
    before = json.loads((state.parent / "cpu-start.json").read_text())
    elapsed = time.monotonic() - before["monotonic"]
    services = []
    for previous in before["services"]:
        current = cpu_snapshot(previous["pid"], previous["start"])
        delta = current["cpu_seconds"] - previous["cpu_seconds"]
        if delta < 0:
            raise ValueError("service CPU accounting regressed")
        current.update(cpu_seconds=delta, average_cpu_percent=100 * delta / elapsed)
        services.append(current)
    client = json.loads((state.parent / "cpu-client.json").read_text())
    client["cpu_seconds"] = client["user_seconds"] + client["system_seconds"]
    client["average_cpu_percent"] = 100 * client["cpu_seconds"] / client["wall_seconds"]
    value = dict(window="whole benchmark command, including stream/dataset preparation and teardown",
                 percent_unit="100% is one logical CPU; services are measured across the same command window",
                 server_window_seconds=elapsed, services=services, client=client,
                 service_cpu_seconds=sum(p["cpu_seconds"] for p in services))
    (state.parent / "cpu.json").write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("cases").add_argument("selection")
    sub.add_parser("arguments").add_argument("label", choices=CASES)
    sub.add_parser("binary").add_argument("messages", type=Path)
    init = sub.add_parser("init")
    for flag in ("binary", "cpus"):
        init.add_argument("--" + flag, required=True)
    init.add_argument("--duration", type=int, required=True)
    init.add_argument("--append-duration", type=int, required=True)
    init.add_argument("--cases", required=True)
    init.add_argument("--root", type=Path, required=True)
    case = sub.add_parser("case")
    case.add_argument("--label", choices=CASES, required=True)
    case.add_argument("--duration", type=int, required=True)
    case.add_argument("--status", type=int, required=True)
    case.add_argument("--log", type=Path, required=True)
    summary = sub.add_parser("summary")
    summary.add_argument("--root", type=Path, required=True)
    summary.add_argument("--results", type=Path, required=True)
    for action in ("cpu-start", "cpu-finish"):
        sub.add_parser(action).add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "cases":
        labels = args.selection.split() or list(CASES)
        if len(set(labels)) != len(labels) or any(label not in CASES for label in labels):
            raise ValueError("case selection has unknown or duplicate labels")
        print("\n".join(labels))
    elif args.action == "arguments":
        workload, size, concurrency, dataset, *_ = CASES[args.label]
        values = ["--workload", workload, "--object-size", str(size), "--concurrency", str(concurrency)]
        if dataset:
            values += ["--dataset-bytes", str(dataset)]
        print("\n".join(values))
    elif args.action == "binary":
        paths = {m["executable"] for line in args.messages.read_text().splitlines()
                 if (m := json.loads(line)).get("reason") == "compiler-artifact"
                 and m["target"]["name"] == "production_bounds" and m.get("executable")}
        if len(paths) != 1:
            raise ValueError("build did not identify one production_bounds executable")
        print(paths.pop())
    elif args.action == "cpu-start":
        cpu_start(args.state)
    elif args.action == "cpu-finish":
        cpu_finish(args.state)
    elif args.action == "init":
        initialize(args)
    elif args.action == "case":
        return record(args)
    else:
        return summarize(args)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Stream regression failed: {error}", file=sys.stderr)
        sys.exit(2)
PY_STREAM_REGRESSION
}

unset CROWDB_ASAN
DURATION="${CHUNK_STREAM_BENCH_DURATION:-10}"
APPEND_DURATION="${CHUNK_STREAM_BENCH_APPEND_DURATION:-60}"
TIMEOUT_SECS="${CHUNK_STREAM_BENCH_TIMEOUT:-180}"
SKIP_BUILD="${CHUNK_STREAM_BENCH_SKIP_BUILD:-0}"
BENCH_BIN="${CHUNK_STREAM_BENCH_BIN:-}"
CPUS="${CHUNK_STREAM_BENCH_CPUS:-}"
export TOKIO_WORKER_THREADS="${CHUNK_STREAM_BENCH_WORKERS:-32}"
CASES="${CHUNK_STREAM_BENCH_CASES:-}"
for value in "$DURATION" "$APPEND_DURATION" "$TIMEOUT_SECS" "$TOKIO_WORKER_THREADS"; do
    if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo 'ERROR: duration, timeout and worker counts must be positive integers' >&2
        exit 2
    fi
done
if [[ "$SKIP_BUILD" != 0 && "$SKIP_BUILD" != 1 ]]; then
    echo 'ERROR: CHUNK_STREAM_BENCH_SKIP_BUILD must be 0 or 1' >&2
    exit 2
fi
if [ "$TIMEOUT_SECS" -le "$DURATION" ] || [ "$TIMEOUT_SECS" -le "$APPEND_DURATION" ]; then
    echo 'ERROR: per-case timeout must exceed workload duration' >&2
    exit 2
fi
labels=$(report cases "$CASES")
mapfile -t labels <<<"$labels"
affinity=()
if [ -n "$CPUS" ]; then
    affinity=(taskset -c "$CPUS")
    pixi run "${affinity[@]}" true
fi
if [ -n "$BENCH_BIN" ] && [ ! -x "$BENCH_BIN" ]; then
    echo "ERROR: benchmark binary is not executable: $BENCH_BIN" >&2
    exit 2
fi
if [ "$SKIP_BUILD" -eq 1 ] && [ -z "$BENCH_BIN" ]; then
    echo 'ERROR: skip-build requires CHUNK_STREAM_BENCH_BIN; no stale artifact guessing' >&2
    exit 2
fi

RUN_STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG_ROOT="${CHUNK_STREAM_BENCH_LOG_ROOT:-${CROWDB_RUNTIME_ROOT:-$(pwd)/.crowdb-runtime}/artifacts/bench/chunk-stream-regression-$RUN_STAMP}"
mkdir -p "$LOG_ROOT"
if [ -n "$(ls -A "$LOG_ROOT")" ]; then
    echo "ERROR: run directory must be empty: $LOG_ROOT" >&2
    exit 2
fi
LOG_ROOT=$(realpath "$LOG_ROOT")
RESULTS_FILE="${CHUNK_STREAM_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
mkdir -p "$(dirname "$RESULTS_FILE")" "$LOG_ROOT/bin"
REGRESSION_LOG_ROOT="$LOG_ROOT"
source tools/benchmark/bench-regression-common.sh
cleanup() {
    local status=$?
    trap - EXIT
    regression_destroy || status=1
    exit "$status"
}
trap cleanup EXIT

# Finish every build before starting fixtures or measurements. Invoke the
# identified executable directly, so a case cannot trigger a Cargo rebuild.
if [ "$SKIP_BUILD" -eq 0 ]; then
    pixi run build-cpp
    pixi run cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb
    if [ -z "$BENCH_BIN" ]; then
        pixi run cargo bench -p crowdb-chunk-stream --bench production_bounds --no-run \
            --message-format=json >"$LOG_ROOT/build.jsonl"
        BENCH_BIN=$(report binary "$LOG_ROOT/build.jsonl")
    fi
fi
regression_init
export CROWDB_REGRESSION_FIXTURE_BIN="${CROWDB_REGRESSION_FIXTURE_BIN:-$(pwd)/target/release/examples/regression-fixture}"
cp -- "$BENCH_BIN" "$LOG_ROOT/bin/current"
BENCH_BIN="$LOG_ROOT/bin/current"
report init --root "$LOG_ROOT" --binary "$BENCH_BIN" \
    --cpus "$CPUS" --duration "$DURATION" --append-duration "$APPEND_DURATION" --cases "${labels[*]}"

run_case() {
    local label="$1" duration="$2" name="$1" arguments seed status
    local case_args=() statuses=()
    REGRESSION_CONFIG="$LOG_ROOT/$name/console.toml"
    mkdir -p "$LOG_ROOT/$name"
    echo ">>> $name (${duration}s, workers=$TOKIO_WORKER_THREADS, cpus=${CPUS:-default})"
    pixi run "${affinity[@]}" python tools/benchmark/fixture.py --state "$REGRESSION_CONFIG" \
        --owner-pid "$$" setup -- --kind combined --metrics-interval 1 \
        --kv-backend mem-block --wal-backend mem-block --no-fsync --diskio-dummy-disk-type mem
    seed=$(pixi run python tools/benchmark/fixture.py --state "$REGRESSION_CONFIG" seed)
    arguments=$(report arguments "$label")
    mapfile -t case_args <<<"$arguments"
    report cpu-start --state "$REGRESSION_CONFIG"
    date -u --iso-8601=ns >"$LOG_ROOT/$name/started.txt"
    set +e
    /usr/bin/time --quiet --format='{"user_seconds":%U,"system_seconds":%S,"wall_seconds":%e,"rss_peak_kib":%M}' \
        --output="$LOG_ROOT/$name/cpu-client.json" \
        timeout --signal=INT --kill-after=10 "$TIMEOUT_SECS" pixi run "${affinity[@]}" "$BENCH_BIN" \
        --management-seed "$seed" --duration-secs "$duration" "${case_args[@]}" \
        2>&1 | tee "$LOG_ROOT/$name/workload.log"
    statuses=("${PIPESTATUS[@]}")
    set -e
    status=${statuses[0]}
    if [ "${statuses[1]}" -ne 0 ]; then
        echo "ERROR: could not retain $name output" >&2
        exit 1
    fi
    date -u --iso-8601=ns >"$LOG_ROOT/$name/finished.txt"
    report cpu-finish --state "$REGRESSION_CONFIG"
    regression_destroy
    if ! report case --label "$label" --duration "$duration" --status "$status" \
        --log "$LOG_ROOT/$name/workload.log"; then
        echo "ERROR: $name failed; continuing with a fresh fixture for the next case" >&2
    fi
}

for label in "${labels[@]}"; do
    duration="$DURATION"
    if [[ "$label" == append_4k_1t || "$label" == append_4k_32t ]]; then
        duration="$APPEND_DURATION"
    fi
    run_case "$label" "$duration"
done

report summary --root "$LOG_ROOT" --results "$RESULTS_FILE"
echo "Logs, binary identities, phase distributions and cleanup receipts: $LOG_ROOT"
