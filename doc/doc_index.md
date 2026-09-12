<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Documentation Index

One-line pointer to every permanent doc. Match by keywords and open only the
listed document or section needed by the task.

## Top-Level Docs

| Doc                                             | When to read                                             |
| ----------------------------------------------- | -------------------------------------------------------- |
| `doc/design/kv/design-crowdb-kv.md`             | KV architecture and sub-design map.                      |
| `doc/design/protocol/design-crowdb-protocol.md` | Protocol and key encoding architecture.                  |
| `doc/design/diskdb/design-crowdb-diskdb.md`     | Diskdb lifecycle, placement, and concurrency.            |
| `doc/design/diskio/design-crowdb-diskio.md`     | Disk I/O engine, fallbacks, cancellation, and RPC.       |
| `doc/design/chunkdb/design-crowdb-chunkdb.md`   | Chunk lifecycle, placement, reservation, and recovery.   |
| `doc/design/chunkio/design-crowdb-chunkio.md`   | Chunk I/O write pipeline, backpressure, and rotation.    |
| `doc/design/tree/design-crowdb-tree.md`         | Storage-engine architecture and sub-design map.          |
| `doc/design/rpc/design-crowdb-rpc.md`           | RPC engine, wire format, FFI, and transport.             |
| `doc/design/console/design-crowdb-console.md`   | Console architecture and service lifecycle.              |
| `doc/design/config/design-crowdb-config.md`     | Configuration ownership, precedence, validation, reload. |
| `doc/user-manual/user-guide.md`                 | Web UI, CLI, REST API, setup, operations, upgrade.       |

## Backlog (`doc/backlog/`)

| Doc                                      | When to read                                                 |
| ---------------------------------------- | ------------------------------------------------------------ |
| `doc/backlog/backlog.md`                 | Requirement index, priority, and status.                     |
| `doc/backlog/R**-<component>-<topic>.md` | One requirement's high-level design and acceptance contract. |

## Working & Flow-Analysis Docs

Temporary plans live under `doc/working/`; flow analyses live under
`doc/design/{kv,chunkio,rpc}/`.

| Doc                                                    | When to read                                   |
| ------------------------------------------------------ | ---------------------------------------------- |
| `doc/design/kv/kv-read-flow-analysis.md`               | KV point-read flow and benchmarks.             |
| `doc/design/kv/kv-scan-flow-analysis.md`               | KV scan flow and benchmarks.                   |
| `doc/design/kv/kv-write-flow-analysis.md`              | KV write flow and optimization evidence.       |
| `doc/design/chunkio/chunkio-write-flow-analysis.md`    | Chunk I/O large-write flow and benchmarks.     |
| `doc/design/chunkio/chunkio-small-io-flow-analysis.md` | Chunk I/O small-I/O flow and benchmarks.       |
| `doc/design/rpc/rpc-flow-analysis.md`                  | RPC flow, benchmarks, and performance history. |

## Dev Environment (`doc/dev/`)

| Doc                    | When to read                                                          |
| ---------------------- | --------------------------------------------------------------------- |
| `doc/dev/env_setup.md` | Benchmark commands, sentinels, prerequisites, and perf-counter setup. |

## Project Files (repo root)

| File                 | When to read                                |
| -------------------- | ------------------------------------------- |
| `AGENTS.md`          | Always-on project rules and skill dispatch. |
| `CONTRIBUTING.md`    | PR setup, conventions, and process.         |
| `CHANGELOG.md`       | Release history.                            |
| `SECURITY.md`        | Vulnerability handling.                     |
| `CODE_OF_CONDUCT.md` | Community behavior.                         |

## Sub-Designs (`doc/design/{kv,tree,console,protocol,diskdb,diskio,chunkdb,chunkio,chunkds,rpc}/`)

| Doc                                                               | Read when working on                                   |
| ----------------------------------------------------------------- | ------------------------------------------------------ |
| `doc/design/kv/design-crowdb-kv-leader-election.md`               | Election, lease, ReadIndex, step-down.                 |
| `doc/design/kv/design-crowdb-kv-slot.md`                          | Slot pipelining, gap repair, catch-up, coalescing.     |
| `doc/design/kv/design-crowdb-kv-rpc.md`                           | Paxos RPC transport, schema, and errors.               |
| `doc/design/kv/design-crowdb-kv-rpc-client.md`                    | Client KV RPC, watch push, rollout, topology cache.    |
| `doc/design/kv/design-crowdb-kv-reconfiguration.md`               | Membership change, leader transfer, epoch fence.       |
| `doc/design/kv/design-crowdb-kv-group0.md`                        | Group-0 schema, registry, topology, cleanup.           |
| `doc/design/kv/design-crowdb-kv-state-machine.md`                 | Apply semantics, per-key slots, snapshot, compaction.  |
| `doc/design/kv/design-crowdb-kv-wal.md`                           | WAL flush, replay, recovery, index, GC.                |
| `doc/design/kv/design-crowdb-kv-watch-notify.md`                  | Watch registry, apply trigger, client, fallback.       |
| `doc/design/kv/design-crowdb-kv-server.md`                        | KV server startup, API, lifecycle, cleanup.            |
| `doc/design/kv/design-crowdb-kv-test.md`                          | KV test strategy and coverage.                         |
| `doc/design/kv/design-crowdb-kv-observability.md`                 | Metrics, collectors, instrumentation, logs.            |
| `doc/design/chunkds/design-crowdb-chunk-kv.md`                    | Chunk-backed range KV, recovery, transfer, split.      |
| `doc/design/chunkds/design-crowdb-chunk-kv-server.md`             | Range catalog, fencing, balance, lifecycle.            |
| `doc/design/chunkds/design-crowdb-chunk-kv-client.md`             | Routed client, cache, composition, scans.              |
| `doc/design/tree/design-crowdb-tree-engine.md`                    | In-memory engine, versioned root, reclamation, I/O.    |
| `doc/design/tree/design-crowdb-tree-storage.md`                   | Durable pages, buffer pool, snapshot, mapping, GC.     |
| `doc/design/tree/design-crowdb-tree-chunk-storage.md`             | Mirrored page packs, manifests, rebuild, reclaim.      |
| `doc/design/tree/design-crowdb-tree-engine-flush-flow.md`         | L0→L1 flush path and bottlenecks.                      |
| `doc/design/tree/design-crowdb-tree-engine-snapshot-flow.md`      | Snapshot persist path and bottlenecks.                 |
| `doc/design/console/design-crowdb-console-ui.md`                  | Web UI shell, canvas, inspector, KV operator.          |
| `doc/design/protocol/design-crowdb-protocol-key.md`               | Binary/text key encoding and evolution.                |
| `doc/design/protocol/design-crowdb-protocol-types.md`             | Wire types, ID aliases, schemas, re-exports.           |
| `doc/design/chunkdb/design-crowdb-chunkdb-mirror-to-ec.md`        | Mirror-to-EC tasks, leases, publication, recovery.     |
| `doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md`       | Range binding, routing, migration, precise free.       |
| `doc/design/chunkdb/design-crowdb-chunkdb-rpc.md`                 | Chunkdb RPC schema, service, transport, errors.        |
| `doc/design/chunkdb/chunkdb-allocate-flow-analysis.md`            | EC allocation benchmark and bottlenecks.               |
| `doc/design/chunkio/design-crowdb-chunkio-small-object-writer.md` | Shared-chunk admission, routing, recovery, elasticity. |
| `doc/design/chunkio/design-crowdb-chunkio-reader.md`              | Mirror/EC reads, recovery, fencing, repair.            |
| `doc/design/chunkds/design-crowdb-chunk-stream.md`                | Logical streams, append, rollover, trim, transfer.     |
| `doc/design/diskdb/design-crowdb-diskdb-zone-management.md`       | Zone allocation, free, compaction, recovery.           |
| `doc/design/diskdb/design-crowdb-diskdb-space-metrics.md`         | Capacity counters, reporting, aggregation.             |
| `doc/design/diskdb/diskdb-allocate-flow-analysis.md`              | Durable allocation flow and benchmarks.                |
| `doc/design/rpc/design-crowdb-rpc-tcp.md`                         | TCP engines, worker loop, zero-copy I/O, scaling.      |
| `doc/design/rpc/design-crowdb-rpc-rdma.md`                        | RDMA setup, CQ polling, registered buffers.            |
| `doc/design/rpc/design-crowdb-rpc-diskdb-migration.md`            | Diskdb RPC migration, rollout, connection lifetime.    |

Prefer the most specific match. Open a root design only for cross-topic work;
update its row when a permanent document is renamed or materially rescoped.
