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
