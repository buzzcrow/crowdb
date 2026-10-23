<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: ChunkdbService RPC

Depends on: [RPC transport](../rpc/design-crowdb-rpc.md),
[ChunkDB lifecycle](design-crowdb-chunkdb.md).
Satisfies: ChunkDB control-plane transport, routing and mutation fences.

ChunkDB uses the crowdb-rpc FlatBuffer transport for lifecycle, placement,
reservation, replacement, conversion and repair operations. Clients and services
use one current schema. Unknown messages and malformed requests fail explicitly.

## Table of Contents

- [1. Transport and routing](#1-transport-and-routing)
- [2. Wire records](#2-wire-records)
- [3. Mutation contracts](#3-mutation-contracts)
- [4. Dispatch and responses](#4-dispatch-and-responses)
- [5. Errors and retry](#5-errors-and-retry)

## 1. Transport and routing

- `ChunkdbClient` requires an `Arc<ChunkdbRpcTransport>` at construction.
  `RangeBindingClient` resolves a chunk's service-slot owner from Group 0.
  `ServiceRegistryClient` supplies live endpoints; endpoint cache refresh does
  not change ownership.
- `ChunkdbRpcTransport` uses an immutable `ConnectionPoolIndex`. Cold connection
  creation happens outside the index, followed by compare-and-swap installation.
  Retry invalidates only the selected connection generation.
- `ChunkdbRpcService` dispatches to the independently scoped system/user-data
  lifecycle and task runtimes. Each mutation checks current range authority.
- The RPC listener defaults to port 9961; HTTP health and metrics default to
  9972. The configured RPC address must parse as a socket address.

## 2. Wire records

- Requests start with `id` and `rpc_create_nano`, followed by operation fields.
  Responses carry request identity, `ret_code`, optional `error_msg`, diagnostic
  `range_start`/`range_end`, and typed result data.
- Message identities are defined by `FBMsgType`. Generated Rust/C++ definitions
  derive from the same schema; native producers use the same request contract.
- `FBChunkType` requires explicit Wal, BtreePage, PageIndex, Stream, S3 or
  IcebergTable purpose. Reserved zero is invalid. Chunk-ID purpose, stored type
  and owner attribution must agree. Stream/Wal carry their logical Stream name;
  other types have an empty owner key.
- `FBChunk` includes writer epoch, exact acknowledged byte cursor, lease deadline,
  physical strips, coarse capacity/sealed geometry, cleanup intents and the last
  replacement operation. `FBChunkStrip` identifies sequence, logical interval,
  unit geometry, mirror/EC segments and unavailable segment identities.
- Segment identity includes disk, zone, unit range, allocation incarnation and
  owning chunk. Mirror and EC bodies carry their explicit physical geometry.
- `QueryChunkResponse` also returns reader-layout validity duration.
  `ListChunksResponse` carries a bounded chunk page and continuation identity.
- Response `Ref` wrappers parse one root and expose typed borrowed accessors.
  Invalid buffers remain invalid; callers check validity and return codes before
  accepting data.

## 3. Mutation contracts

- Allocation specifies purpose, writer epoch, lease and applicable owner key.
  The active chunk and its finalization task are created atomically. Allocation
  retries retain operation identity rather than create unrelated chunks.
- Append and strip reservation extend physical layout under their current fences.
  Reservations remain hidden until confirmation. A consumed reservation prevents
  conflicting seal transitions.
- `AdvanceChunkWrite` carries expected metadata revision, writer epoch, exact
  byte cursor and optional closed-strip sequence. It cannot move progress backward
  or beyond capacity.
- `SealChunkRequest.seal_bytes` is the exact effective byte boundary, including
  zero for an empty seal. KiB geometry does not authorize extra readable bytes.
  Native clients validate the returned identity, epoch, exact cursor and state.
- `ReplaceChunkStripRange` carries expected revision, exact old strips, replacement
  strips and a stable operation ID. The lifecycle validates geometry and ownership,
  commits new segments before publication, and retains retired segments through
  cleanup grace. One-strip changes use this same fenced range contract.
- Replacement allocation and discard manage tentative segments. Conversion and
  repair publish through their fenced lifecycle transitions and durable tasks.
- Delete publishes a tombstone with durable cleanup work before freeing blocks.
  Whole-chunk retries are idempotent. `DeleteChunkRange` returns `Unimplemented`
  until shared-object range reclamation is supported; overlap alone cannot free
  a strip containing other live objects.

## 4. Dispatch and responses

- Each registered message has a request metric guard and typed handler. Dispatch
  validates its FlatBuffer and required identities before spawning lifecycle work.
- Handlers execute on the service runtime and retain captured ownership checks
  around external operations. Responses are submitted through the RPC server.
- Successful responses contain current typed state. Lifecycle errors map to a
  typed return code; `NotMyRange` includes routing diagnostics for the client.
- Layout replacement, conversion and relocation share the `layout_mutation`
  request metric family. Allocate, append, reservation, cursor, query, seal,
  deletion, listing and ad-hoc recovery retain their own families.
- Startup validates topology and service/storage slot maps, constructs separate
  domain runtimes, registers RPC handlers and starts discovery refresh plus HTTP
  observability. Shutdown stops background work and transport through the runtime.

## 5. Errors and retry

- Invalid arguments and strip indices map to `InvalidArgument` or
  `StripIndexOutOfRange`. Missing/existing chunks map to `NotFound`/`AlreadyExists`.
- Invalid lifecycle transitions map to `FailedPrecondition`; revision/operation
  conflicts map to `Aborted`. Storage/allocation failures map to `Internal`.
- Lock admission failures map to `Unavailable`. Ownership failures return
  `NotMyRange`, causing binding refresh and routing to the current owner.
- Client retries are bounded and apply only to transient failures. Typed fence
  failures and malformed responses do not become successes. Mutations that may
  have completed retain their operation identity and reconcile durable state.
- A transport response alone does not establish Stream writer ownership or a
  synchronized DiskIO write; those contracts belong to their respective layers.
