<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Regression Benchmark Layout (R135)

This implementation design refines the retained-artifact and repeated-run
lifecycle in `doc/backlog/R135-chunkio-end-to-end-performance.md`. It depends
on the local deployment model in `doc/design/console/design-crowdb-console.md`
and the workload contract in `doc/design/chunkio/design-crowdb-chunkio.md`.

## 1. Stable Artifact Ownership

Every CLI process creates
`cli-<command-chain>-<timestamp>/` beneath a caller-selected run root. The
deployment invocation owns `deploy/`; aggregate `results.tsv` and
`console.toml` belong to the run root.

Every locally spawned server owns
`deploy/rack<id>/node<id>/<service>-<server-id>/`. Its binary, configuration,
logs, and storage stay below that root. A KV server derives `waldata/` and
`ctdata/` directly from the server root. Stable server IDs, rather than PIDs or
internal service instance IDs, name directories.

Failures to create or write a server root abort deployment. Teardown retains
the tree and console configuration for diagnosis.

## 2. Topology

The combined local fixture uses rack 1 with nodes 1 through 3. It does not
rewrite nodes into synthetic racks. Tests that exercise placement failure
domains create racks explicitly. Unsafe EC remains an explicit local-only
option when a policy requests more failure domains than this fixture provides.

## 3. Repeated Regression Cases

Cases with identical deploy-time tunables share one deployment. Between cases,
the orchestrator wipes each benchmark KV group, clears service-owned metadata,
restarts DiskDB, ChunkDB, and DiskIO to discard caches and pending runtime
state, waits for registration and routing convergence, then resets the metrics
window. A failed clean or restart invalidates the next sample and triggers
bounded teardown.

Cases with different server worker, connection, storage backend, or protocol
settings use separate deployments because those values cannot be reset safely
in process.

## 4. Metrics Contract

Regression validation checks content rather than file existence. KV requires
Rust, C++ RPC, and C++ tree sections. DiskDB and ChunkDB require Rust and C++
RPC sections. DiskIO requires C++ RPC and system counters. The benchmark client
requires its workload counters and C++ RPC section. A missing section fails the
case even when the metrics file is non-empty.

## 5. Scope

- `app/crowdb-cli/`: invocation naming and regression lifecycle commands.
- `lib/crowdb-console-shared/`: stable deployment roots and clean/restart
  orchestration.
- `app/crowdb-{kv-server,diskdb,chunkdb,diskio}/`: server roots and metric
  flushing.
- `tools/bench-*-regression.sh`: common retained layout and grouped lifecycle.
- Console and ChunkIO permanent designs plus R135 acceptance text.

## 6. Complexity

High. Directory changes are mechanical, but safe cluster reuse crosses four
services, persisted group-0 metadata, service leases, caches, readiness, and
metrics epochs. Restart must preserve stable identity and endpoints while
updating tracked PIDs.

## 7. Test Design

- Create a CLI invocation with an invalid local service type -> inspect the log
  root -> assert exactly one `cli-cluster-local-deploy-*` directory exists.
- Deploy the combined fixture -> inspect config and disk -> assert one rack,
  three nodes, stable per-server roots, and KV `waldata/` plus `ctdata/` directly
  beneath each KV root.
- Run a full-stack write, clean and restart, then repeat the deterministic write
  -> assert both cases complete with exact independent accounting and no stale
  route or lifecycle state.
- Exercise every service during a metrics interval -> stop gracefully -> assert
  every required metrics section contains registered counters.
- Run each sentinel with its shortest supported case selection -> inspect the
  retained root -> assert the shared run-level and invocation-level structure.

## 8. Module Structure

```text
tools/
  bench-regression-common.sh       shared artifact and validation helpers
  bench-*-regression.sh            workload matrices
app/crowdb-cli/
  src/main.rs                      CLI invocation directory ownership
lib/crowdb-console-shared/src/
  ops/cluster.rs                   topology and service roots
  lifecycle.rs                     server process roots and restart
```

## 9. Config Extensions

No user-facing server data-root option is needed. Local deployment passes the
stable server root through the existing KV `--root` interface. Restart wiring
may persist a local launch specification when existing `ServerEntry` fields do
not contain enough arguments to reproduce a process.

## 10. Server Wiring

DiskDB and ChunkDB attach the crowdb-rpc global registry to their Rust
`MetricsRunner` C++ flush callback. DiskIO flushes its process-global metrics
registry beside its system collector. The cluster orchestrator owns restart
ordering and waits for group-0 service registration before admitting a new
case.
