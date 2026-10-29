<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Native S3 and Dataset RDMA Access

Native S3 and Dataset clients use CROWDB's shared RPC transport for direct
storage access. The HTTP S3 endpoint retains its protocol and authorization
contract; it does not negotiate an alternate cuObject data plane.

Depends on: [Access Server](../design-crowdb-access-server.md),
[S3](design-crowdb-access-s3.md),
[Dataset](../dataset/design-crowdb-access-dataset.md), and
[RPC RDMA](../../rpc/design-crowdb-rpc-rdma.md).

Satisfies: bounded native storage access without an Access Server payload bounce.

## Table of contents

1. [Access paths](#1-access-paths)
2. [Transport and buffers](#2-transport-and-buffers)
3. [Correctness and scope](#3-correctness-and-scope)

## 1. Access paths

```text
HTTP client -> Access Server -> shared metadata/chunk services

Trusted native S3 / Dataset client
  -> shared ClientRuntime
     -> metadata authority and immutable content resolution
     -> ChunkIO / DiskIO RPC or stream
        -> registered host buffers over RC SEND/RECV
```

Native clients resolve immutable object generations or Dataset Snapshots and
use the existing metadata, publication, durability, integrity and reclamation
contracts. Transport selection changes byte delivery, not logical ownership.
Native clients are trusted storage participants; HTTP authorization is not
implicitly inherited by direct native access.

## 2. Transport and buffers

RDMA uses RC SEND/RECV, RDMA CM connections, registered host-buffer arenas,
bounded completion polling and explicit buffer ownership. Endpoint capability
preflight and fallback follow the shared RPC transport contract. An established
connection does not switch transports in the middle of an operation. Callers
retain request identities and treat interrupted mutations as uncertain.

The native SDK owns request and byte admission. Returned host-memory leases
remain valid until released, including after runtime shutdown. Dataset fields
and S3 streaming bodies preserve their existing integrity checks and bounded
lifetime. Device selection, class-specific frame capacities, diagnostics and
hardware verification are defined by the RPC design and native SDK operations
instructions.

## 3. Correctness and scope

- I1: Transport selection never changes immutable generation/Snapshot binding,
  exact offsets, integrity validation, publication or durable completion.
- I2: Buffer ownership persists through asynchronous completion and consumer use.
- I3: Partial or uncertain transfers are not reported as complete logical results.
- I4: Native admission and the shared RPC fallback policy bound resource use;
  failed transfers do not silently splice data from another transport.
- Host-memory RC transfer is the supported path. cuObject/DC negotiation,
  one-sided client-memory READ/WRITE, GPU pointer registration and direct GPU
  destinations are outside this contract. Their absence is not a missing step
  in native host-buffer RDMA support.
