<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Repository tools

Start with the task name in [`pixi.toml`](../pixi.toml), then read its script.
Run commands through `pixi run`. Read only the directory relevant to the task.

- **`pixi-tasks/`** — build, install, clean, component test and Iceberg client
  commands extracted from Pixi.
  `clean` stops all CROWDB services owned by the current user, then removes
  the complete runtime tree and build output. `clean-env` stops only recorded
  ephemeral processes and preserves persistent clusters.
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
update the component script when adding a package, then run
`check-ci-test-tasks`.
Feature-gated or ignored client tests need explicit task selection.

Container packaging and its acceptance scripts live under
[`container/single-node-container/`](../container/single-node-container/README.md).
Generated artifacts belong under `target/` or `.crowdb-runtime/`, not here.
