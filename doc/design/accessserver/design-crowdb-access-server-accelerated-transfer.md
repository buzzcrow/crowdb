<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server Accelerated Transfer

Accelerated Transfer is an optional data plane for cuObject-compatible S3 and
native Dataset operations. It is compiled and operated independently from the
basic protocol implementations.

Depends on: [Access Server](design-crowdb-access-server.md),
[S3](design-crowdb-access-server-s3.md),
[Dataset](design-crowdb-access-server-dataset.md),
[DiskIO](../diskio/design-crowdb-diskio.md), and
[RPC RDMA](../rpc/design-crowdb-rpc-rdma.md).

Satisfies: direct transfer between DiskIO-owned buffers and client registered
memory without an Access Server payload bounce.

External contracts: [NVIDIA cuObject](https://docs.nvidia.com/gpudirect-storage/cuobject/),
[cuObjServer API](https://docs.nvidia.com/gpudirect-storage/cuobject/cuObjServer-api/),
and [server support matrix](https://docs.nvidia.com/gpudirect-storage/cuobject/cuobject-server-release-notes/).

## Table of contents

1. [Isolation](#1-isolation)
2. [cuObject placement](#2-cuobject-placement)
3. [Distributed GET](#3-distributed-get)
4. [PUT and EC](#4-put-and-ec)
5. [Completion and fallback](#5-completion-and-fallback)
6. [Limits and hardware](#6-limits-and-hardware)
7. [Correctness invariants](#7-correctness-invariants)

## 1. Isolation

The S3 cuObject adapter, native transfer facade, C++ transport engine, DiskIO
endpoint, configuration, metrics, and tests form one optional module. A build
without that module links no cuObject library, creates no RDMA resource, and
adds no negotiation branch to the basic S3 or Dataset path.

The basic protocols expose stable request, immutable-generation, buffer-owner,
and completion boundaries. Acceleration consumes those boundaries and cannot
change publication, range, integrity, authorization, or error semantics.

## 2. cuObject placement

Access Server owns HTTP negotiation, authentication, immutable-generation
lookup, logical planning, and completion aggregation. Each participating
DiskIO process owns a local `cuObjServer` endpoint, DCI/channel pool, registered
host-buffer pool, and completion polling.

An HTTP request does not bind a DC connection in Access Server.
`cuObjServer::isConnected()` reports that a DiskIO-local RDMA session started.
For each transfer, `handleGetObject()` or `handlePutObject()` receives the
client's opaque descriptor and establishes the dynamic DC data path. Opaque
registration and channel handles remain in their owning DiskIO process.

## 3. Distributed GET

For a 4 MiB object whose logical bytes reside on four nodes:

```text
Client/GPU       AccessServer          Chunk plan            DiskIO A..D
    | GET + token     |                    |                       |
    |---------------->| authenticate/read |                       |
    |                 |------------------->|                       |
    |                 |<-- four fenced 1 MiB spans ---------------|
    |                 |-- signed span + descriptor -------------->A
    |                 |-- signed span + descriptor -------------->B
    |                 |-- signed span + descriptor -------------->C
    |                 |-- signed span + descriptor -------------->D
    |<================ parallel RDMA WRITE to offsets 0..4 MiB ===|
    |                 |<--------- four completions ----------------|
    |<-- HTTP success + RDMA reply -|                       |
```

The client registers one destination and sends its opaque descriptor in the
S3 control request. Access Server authenticates, resolves one object generation,
and obtains a fenced logical read plan. Each internal span task binds operation,
generation, source reference, length, destination offset, expiry, nonce, and
authority for exactly that interval.

DiskIO reads its span directly into a locally registered native buffer and
performs an RDMA WRITE to `client_base + destination_offset`. Disjoint intervals
may complete in any order. Access Server returns success only after every span
completion is authenticated and successful; the client does not consume the
full destination earlier.

## 4. PUT and EC

For PUT, Access Server plans bounded destination writers before payload
submission. DiskIO performs RDMA READ into local registered buffers and feeds
those owners into chunk and EC writers. Object metadata publishes only after
all data, parity, checksums, seals, and transfer completions succeed.

A mirrored GET chooses one healthy replica. An EC GET assigns an executor that
obtains enough shards and reconstructs the requested logical bytes before the
RDMA WRITE. Raw parity or shard bytes are never written into client object
positions.

## 5. Completion and fallback

TCP fallback is allowed only before any RDMA payload operation is submitted.
After partial GET, the client destination is wholly invalid. After partial PUT,
the upload remains unpublished and recovery owns cleanup. A request never
splices TCP payload into a partial RDMA transfer.

Local buffers and registrations remain alive until the relevant completion is
polled. Shutdown stops admission, drains or fails transfers, reports terminal
span status, and only then destroys channels, registrations, and device state.

## 6. Limits and hardware

Configuration bounds operations, registered bytes, spans, channels, DCI/CQ
depth, SGE count, per-operation size, retries, polling, and deadlines. The
effective values cannot exceed device or cuObject hard limits.

cuObject uses DC v1. ConnectX-5 is the minimum validated NVIDIA generation and
a newer supported adapter is included in compatibility testing. The distributed
test proves that one client descriptor supports concurrent non-overlapping
writes from multiple DiskIO endpoints; failure disables distributed offload
without adding an Access Server payload bounce.

## 7. Correctness invariants

- **XFER-I1 — No gateway payload:** accelerated payload bytes never traverse
  Access Server memory.
- **XFER-I2 — DiskIO ownership:** each endpoint, registered local buffer,
  channel, and completion queue remains owned by its DiskIO process.
- **XFER-I3 — Bounded delegation:** a span cannot access another tenant,
  generation, direction, or remote interval.
- **XFER-I4 — Aggregate completion:** protocol success follows every required
  span and durability completion, never submission alone.
- **XFER-I5 — Disjoint parallelism:** parallel endpoints write only validated,
  non-overlapping destination intervals.
- **XFER-I6 — No mixed partial transfer:** fallback occurs before submission or
  the complete operation fails.
- **XFER-I7 — Transport equivalence:** accelerated and ordinary paths return the
  same logical bytes, integrity, publication, and error outcome.
