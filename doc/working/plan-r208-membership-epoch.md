<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Crash-safe membership epoch persistence Plan

Upstream: [R208](../backlog/R208-kv-membership-epoch-persistence.md),
[`design-crowdb-kv-reconfiguration.md`](../design/kv/design-crowdb-kv-reconfiguration.md)

Goal: persist the complete membership and epoch before publishing a rebuilt
group, fail closed on persistence errors, and preserve restart fencing.

## Phase 1 — persistence API and publication ordering

- [x] **Candidate persistence**: add a fallible group-config persistence path
  that can serialize a candidate group before `PxKvStore::add_group` publishes
  it, while using the store's bound local endpoint. Files:
  `lib/crowdb-kv/src/cluster/group_membership.rs`,
  `app/crowdb-kv-server/src/mgmt/replica_ops.rs`.
- [x] **Atomic shared-file update**: serialize concurrent `NodeConfigStore`
  read-modify-write operations and sync the renamed file/directory. Files:
  `lib/crowdb-kv/src/cluster/node_config.rs`.
- [x] **Fail-closed restore**: stop swallowing malformed node-config reads in
  membership recovery; return an actionable error instead of an empty default.
  Files: `lib/crowdb-kv/src/cluster/node_config.rs`,
  `app/crowdb-kv-server/src/recovery/startup.rs`.

## Phase 2 — focused verification

- [x] **Persistence tests**: exercised node-config round trips and concurrent-safe
  read-modify-write paths with the existing node-config suite.
- [x] **Fence regression**: reran membership mismatch, restore, and reconfiguration tests.

## Files and tests

- Source: `lib/crowdb-kv/src/cluster/node_config.rs`,
  `lib/crowdb-kv/src/cluster/group_membership.rs`,
  `app/crowdb-kv-server/src/mgmt/replica_ops.rs`,
  `app/crowdb-kv-server/src/recovery/startup.rs`.
- Unit: `pixi run cargo test -p crowdb-kv --test node_config_test`.
- Integration: `pixi run cargo test -p crowdb-kv-server --all-targets` and the
  focused membership/recovery tests.
- Gates: `pixi run rs-fmt-check`, `pixi run rs-lint`.
