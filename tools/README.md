<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Repository tools

Start with the task name in [`pixi.toml`](../pixi.toml), then read its script.
Run commands through `pixi run`. Read only the directory relevant to the task.

- **`pixi-tasks/`** — build, install and clean commands extracted from Pixi. `test.py` is the single host test dispatcher:
  `unit GROUP`, `e2e DOMAIN`, `profile NAME`, or `all`. Container lifecycle
  remains owned by `e2e-test/run.py`. Component runs retain phase logs and wall
  times under `.crowdb-runtime/artifacts/unit/`.
  `clean-env` stops only recorded ephemeral processes and preserves persistent
  clusters. `oci-clean --cluster NAME` removes owned containers and unused
  bridges; data deletion requires explicit `--delete-data`.
- **`ci-checks/`** — version consistency, production concurrency-container
  policy and package-to-Pixi-to-CI coverage. `task_graph.py` follows task calls
  through shell scripts; it is shared with timing collection.
- **`test-metrics/`** — sequential suite timing and test counts. Logs and JSON
  results are archived by UTC run timestamp under
  `.crowdb-runtime/artifacts/measure-tests/`; `latest.json` contains the newest
  results without overwriting previous baselines.
- **`cpp-checks/`** — clang-tidy, sanitizer regression and tree/chunk link
  isolation probes. The `tree-link-isolation/` sources belong to that probe.
- **`benchmark/`** — repeatable performance regressions, shared result handling,
  leak inspection and KV write sentinel checks.
- **[`rpc-test/`](rpc-test/README.md)** — real echo transport measurements and
  owned server fixtures, including both client schedulers and sanitizer runs.
- **`profiling/`** — perf setup and write-path flamegraphs.

Common entry points:

```sh
pixi run check-ci-test-tasks
pixi run check-version
pixi run clean-env
pixi run bash tools/test-metrics/measure.sh test-access test-console
```

For test ownership and timings, read
[`doc/working/test.md`](../doc/working/test.md). CI calls component tasks;
update the package groups in `pixi-tasks/test.py` and the checked CI map, then run
`check-ci-test-tasks`.
Feature-gated or ignored client tests need explicit task selection.

Benchmark and profiling scripts create an explicit native measurement fixture
through `benchmark/fixture.py`. The fixture is a `test-util` example, built by
the scripts, and owns its services by PID plus process start identity. It stops
them on explicit teardown or when the measurement owner exits. Its private
descriptor and retained logs live beside the selected measurement state file.
Memory backends, server transport tuning and group reset belong to this fixture;
the installed CLI attaches to its target using the normal client contract.
ChunkDB measurements retain protected placement over three logical native nodes
so EC workloads remain covered, including in release builds. These processes
share one physical host and do not qualify host-failure tolerance. Read/replay
fixtures use memory disks that retain bytes; allocation and write-throughput
fixtures may use NullDisk.
These native measurements do not certify OCI startup or runtime acceptance.

KV measurements keep the human console report and `report.json` in the latest
CLI benchmark directory. The fixture saves every invocation under `measurements/`
before another invocation replaces it. Reports contain average, p50 and p99
latency; scripts print `unavailable` for the unsupported p999 field. Failed
commands clear prior reports and preserve complete diagnostics. The Console
component task explicitly builds and runs `benchmark/fixture-smoke.py`, checking
real read/scan/write, reset, foreign control rejection, owner exit and scoped
cleanup. `bench-leak-inspect.sh` remains alive during inspection; Ctrl-C ends its
ownership and stops its services. Profiling records the attached client; fixture
server PIDs are retained in its state for separate server sampling.

Container packaging and its acceptance scripts live under
[`container/single-node-container/`](../container/single-node-container/README.md).
Generated artifacts belong under `target/` or `.crowdb-runtime/`, not here.

Regular CI groups component tests into C++, Core, Storage, Access (including
SDK protocol peers), Console and UI. Container gates share one release image:
KV, S3, Iceberg and Console are independent jobs; the storage matrix groups
disk (DiskDB/DiskIO), chunk (ChunkDB/Chunk-KV/Chunk stream/Chunk client) and
platform (Dataset/Deployment). Every complete domain has exactly one owner.
Special runtime profiles remain explicit.

```sh
pixi run test unit core
pixi run test e2e s3 --case s3.sdk::java
pixi run test profile containerd-cluster
pixi run test-e2e-iceberg
```

On a fresh checkout, use `test-cpp` and `test-console-ui`; their Pixi
dependencies build C++ and install UI packages before dispatch.
