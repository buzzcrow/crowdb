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
- **`ci-build/`** — release runtime and host client artifacts for CI consumers.
  `build.py runtime` packages the already staged production runtime;
  `build.py clients` compiles the shared acceptance clients. `unpack.py`
  verifies the source identity and file digests before restoring either
  artifact. Consumers run the supplied binaries with `CROWDB_COMPILED_TESTS`
  pointing at `target/compiled-tests`.
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

Regular CI starts lint, UI, C++, Core, Storage, Access (including SDK protocol
peers), Console, runtime compilation and host client compilation independently.
Rust component jobs compile and execute their own tests with Cargo's default
test profile and line-table debug information in CI; UI tests need no Rust
runtime build. Runtime services, shared container clients and shipped Dataset
SDK artifacts use release builds. Cargo uses its default CPU parallelism, and
CMake selects the available system CPU count. Local jobs sharing a build
directory still run their compilation stages sequentially.

OCI construction waits only for the staged production runtime. Container jobs
wait for both the OCI artifact and the host client artifact, then reuse those
exact inputs without Cargo compilation. Production runtime staging has its own
job, separate from test code and test features. Cache entries contain debug and
release compilation outputs under separate job keys; immutable workflow
artifacts carry runtime/client inputs.
Producers and consumers use Ubuntu 24.04 with the locked Pixi toolchain.
Consumers take artifact names from producer outputs, so rerunning a failed
consumer can reuse a successful compilation from an earlier workflow attempt.

KV, S3, Iceberg and Console have independent container jobs; the storage matrix
groups disk (DiskDB/DiskIO), chunk (ChunkDB/Chunk-KV/Chunk stream/Chunk client),
Dataset and Deployment. Every complete domain has exactly one owner. The
Iceberg matrix runs core, GC, SDK and grace partitions independently, so GC
convergence tests do not delay the ordinary core cases. The
preview and release workflows use the same compilation split and preserve
their explicit Docker/containerd runtime gates. The manual ecosystem workflow
consumes the shared OCI artifact and prepares its external client tools in the
acceptance job. Special runtime profiles remain explicit.

```sh
pixi run test unit core
pixi run test e2e s3 --case s3.sdk::java
pixi run test profile containerd-cluster
pixi run test-e2e-iceberg
```

On a fresh checkout, use `test-cpp` and `test-console-ui`; their Pixi
dependencies build C++ and install UI packages before dispatch.
