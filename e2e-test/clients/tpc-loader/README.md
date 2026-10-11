<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Manual TPC client

This locked Python 3.12 project runs loader 0.1.1 on the host. Its PyArrow and
PyIceberg requirements differ from the ordinary SDK environment. The formal
release image runs the fourteen storage/Access services; containers do not run
loaders. This scope is excluded from ordinary domain selection and CI.

- Install: `pixi install --locked --manifest-path e2e-test/clients/tpc-loader/pixi.toml`.
- Prepare cached generators and validate complete SF1 data:
  `pixi run --locked --manifest-path e2e-test/clients/tpc-loader/pixi.toml prepare .crowdb-runtime/artifacts/tpc-prerequisites`.
- Run: `pixi run test-container-e2e --scope iceberg.manual.tpc --concurrency 1 --node-cpus 0.5 --cpus 16 --memory-mib 16384`.

Preparation may download generator prerequisites. Acceptance uses `--no-download`
and concurrently runs TPC-H with eight upload workers and TPC-DS with twenty-four,
using the normal loader validation and a 32-character write token. Public catalog
inventory must contain all eight and twenty-four tables. Each run retains separate
work directories, loader reports, output, server data/logs and timing artifacts.
Missing host Python prerequisites fail before allocating containers.

Accepted run: `run-eff8919c09894107ac996f6838f19694`, one pass, 59.549 seconds
including setup/build/teardown (45.21-second host client). Timing is a manual scope
measurement and is not added to the ordinary Iceberg domain count.
