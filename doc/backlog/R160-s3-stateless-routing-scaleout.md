<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R160: access server / S3 — Stateless access-server routing and scale-out

## Problem

An S3 frontend that owns upload, listing, or bucket authority in local memory
cannot survive restarts or distribute retries across instances. A shared
multi-protocol dispatcher would also put Catalog and Dataset traffic on the S3
hot path and weaken protocol isolation.

The process boundary is
`doc/design/accessserver/design-crowdb-access-server.md` §§3–6.

## Solution

1. Host S3 as an independent library in `crowdb-access-server`; give S3,
   Catalog, and Dataset separate configured listen addresses and direct Hyper
   entry points. Do not add a common host/path protocol dispatcher.
2. Keep all authoritative bucket, object, upload, delete, and pagination state
   in Chunk-KV or chunk storage. Local caches are hints keyed by durable
   generation and may be dropped at any time.
3. Make request and retry identities portable so any access-server instance can
   reconcile an operation started elsewhere. Load balancers require no sticky
   session for basic bucket/object operations.
4. Feature-gate and config-gate each protocol library. A disabled protocol
   binds no port, spawns no task, creates no pool, registers no metric worker,
   and adds no request-path branch to another protocol.
5. Coordinate graceful shutdown through shared process primitives while each
   protocol independently stops admission and drains its known metadata
   outcomes.

## Dependencies

- Depends on R152 and durable identities from R153/R154.
- Uses the existing routed chunk and Chunk-KV clients.
- Catalog, Dataset, and accelerated transfer are peers, not prerequisites.

## Acceptance

- Given uploads, deletes, and list tokens created through one instance, when
  retries go to another instance after the first stops, assert they reconcile
  the same durable outcomes. Invariant: basic S3 has no gateway-local authority.
  E2E test.
- Given distinct configured S3, Catalog, and Dataset addresses, when each
  listener receives another protocol's request, assert it is handled only by
  that listener's library and no shared dispatcher runs. Invariant: protocol
  hot paths are isolated. Integration test.
- Given S3 is disabled at build time or configuration time, when the process
  starts under load from another protocol, assert no S3 resources or branches
  are present. Invariant: an unloaded plugin has zero runtime influence.
  Integration test.
- Given increasing access-server instances and Chunk-KV partitions, when a
  balanced workload runs, assert correctness is unchanged and no single
  frontend-owned lock serializes throughput. Invariant: frontend scale-out is
  horizontal. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
