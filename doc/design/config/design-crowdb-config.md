<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Service Configuration

This document defines the startup configuration contract shared by CROWDB
server processes. Component designs define the meaning of their domain
settings; this document defines how values are owned, loaded, merged,
validated, and activated.

## Table of Contents

- [1. Scope](#1-scope)
- [2. Ownership](#2-ownership)
- [3. Resolution and Activation](#3-resolution-and-activation)
- [4. TOML Schema](#4-toml-schema)
- [5. Reload and Restart](#5-reload-and-restart)
- [6. Deployment](#6-deployment)
- [7. Failure Handling](#7-failure-handling)
- [8. Invariants](#8-invariants)
- [9. Non-Goals](#9-non-goals)

## 1. Scope

The `crowdb-kv-server`, `crowdb-diskdb`, `crowdb-chunkdb`, and
`crowdb-diskio` processes accept typed TOML startup configuration. A service
may require a file or make it optional, but a supplied file follows the same
resolution and failure rules in every process.

This contract covers process configuration. Durable cluster topology,
membership, allocation metadata, and application data keep their existing
authorities.

## 2. Ownership

- A reusable library owns config types for behavior implemented by that
  library. The KV schema is therefore owned with the KV library.
- A server binary owns startup inputs and the final merge before constructing
  listeners and services.
- `crowdb-common` owns shared Rust TOML loading, validation, watching, and
  structural diff mechanics; it does not own service-specific fields.
- `DioConfig` owns the equivalent typed C++ schema and TOML decoding for
  diskio.

Client-side concurrency fields remain named for the dependency they drive,
such as `kv_rpc_workers`, `diskdb_rpc_workers`, and `peer_pool_size`.
`server.rpc_workers` always means workers accepting that process's inbound RPC
traffic.

## 3. Resolution and Activation

Every process resolves a complete candidate in this order:

```text
typed compiled defaults -> TOML values -> explicitly present CLI values
                         -> complete validation -> runtime construction
```

A CLI parser default is not an explicit value and cannot overwrite a file.
Rust CLI fields participating in the merge use optional values. Diskio loads
its single `--config` path before replaying the other explicitly supplied CLI
options, so argument order does not change precedence.

The final candidate is validated before listener construction. Runtime
components receive values from this candidate; wiring layers do not maintain
independent defaults.

## 4. TOML Schema

Common section names describe process roles:

| Section       | Ownership                                                   |
|---------------|-------------------------------------------------------------|
| `[server]`    | Listener identity, server-side RPC resources, and heartbeat |
| `[engine]`    | Local execution and I/O engine tuning                       |
| `[group0]`    | Local group-0 discovery and registration inputs             |
| `[metrics]`   | Metrics output and cadence                                  |
| Domain tables | Service-owned storage, placement, lifecycle, or policy      |

The service schemas retain their different domain sections:

| Service | File policy | `server.rpc_workers` default | Principal domain sections                       |
|---------|-------------|------------------------------|-------------------------------------------------|
| KV      | Optional    | 2                            | Paxos, WAL, engine, metrics                     |
| diskdb  | Required    | 2                            | Storage, heartbeat, persistence, scanner, sync  |
| chunkdb | Required    | 2                            | Storage, placement, lifecycle, topology, clients|
| diskio  | Optional    | 4                            | Engine, group-0 discovery, metrics, disk entries|

All schemas tolerate unknown keys so a newer file can be staged before a
binary upgrade. A known key with the wrong type or invalid value rejects the
candidate. Omitted fields take their typed defaults.

Diskio represents disks as ordered `[[disk]]` entries. A disk ID is a string
because it accepts either a single hexadecimal low word or `high:low`. Each
entry supplies a path and one zone capacity. An empty path selects a dummy
disk. Fault latency bounds and error rate are optional `[server]` fields.

## 5. Reload and Restart

`server.rpc_workers` is static in all services because changing it requires
reconstructing the RPC listener. A file watcher may detect and report the
change, but the running listener retains its startup value until restart.

Other fields follow their component's reload contract. A watcher never makes
a startup-only field dynamic merely because it can observe a changed file.

## 6. Deployment

Tracked templates are executable examples guarded by loader tests. Local
deployment writes node-specific files where addresses, roots, or group-0 seeds
vary, and restart reuses the same file path. Explicit deployment overrides are
represented as explicit CLI options and therefore retain the standard
precedence.

## 7. Failure Handling

Malformed TOML, an unreadable named file, a wrong known-field type, or failed
validation terminates startup before bind. The loader does not silently fall
back to defaults after an operator explicitly names an invalid file.

Diskio file loading is transactional: decoding and validation use a temporary
`DioConfig`, and the caller's config changes only after the complete candidate
is valid.

## 8. Invariants

- **I1 — deterministic precedence:** The same defaults, file, and explicit CLI
  inputs resolve to the same candidate independent of CLI argument order.
- **I2 — complete activation:** No runtime component observes a partially
  decoded or partially validated candidate.
- **I3 — worker viability:** Every server starts with at least one inbound RPC
  worker.
- **I4 — one runtime authority:** Listener construction reads
  `server.rpc_workers` from the final candidate and has no shadow default.
- **I5 — static-change honesty:** A detected static change is not reported as
  active until a restart has constructed runtime state from it.
- **I6 — restart stability:** Deployment restart uses the same serialized
  configuration path as initial startup.

## 9. Non-Goals

Configuration is local to each server. Group 0 does not publish configuration
revisions, resolve scoped overrides, track applied revisions, or coordinate
dynamic activation. The local file remains the startup authority.
