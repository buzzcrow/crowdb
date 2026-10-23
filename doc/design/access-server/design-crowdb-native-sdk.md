<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Trusted native storage SDK

The native SDK embeds internal storage access in the application. Its shared
runtime, topology and host-buffer delivery support typed S3 and Dataset frontends.

Depends on: [Access Server](design-crowdb-access-server.md),
[RDMA RPC](../rpc/design-crowdb-rpc-rdma.md),
[Chunk I/O](../chunkio/design-crowdb-chunkio.md).

Satisfies: Access Server invariants AS-I3, AS-I5, AS-I7 and AS-I9.

## Table of contents

1. [Trust and semantic ownership](#1-trust-and-semantic-ownership)
2. [Shared runtime](#2-shared-runtime)
3. [Topology and selection](#3-topology-and-selection)
4. [Data and completion](#4-data-and-completion)
5. [Budgets and shutdown](#5-budgets-and-shutdown)
6. [Transport and integration](#6-transport-and-integration)
7. [Correctness invariants](#7-correctness-invariants)

## 1. Trust and semantic ownership

The embedding process is a trusted internal cluster component in the Access
Server trust domain. It supplies tenant and operation context and reaches
management, Chunk-KV, Chunkdb and DiskIO directly. Native operations do not
verify SigV4, receive a user-secret database or call Access Server to authorize
an operation. Network access to internal services belongs to the embedding
system's deployment boundary.

`S3Executor` owns shared S3 namespace, metadata selection, conditions,
publication and multipart orchestration. HTTP supplies parsing, authentication
and HTTP/XML encoding; `S3Client` supplies typed context and results. Both
preserve S3 generation, checksum, visibility and uncertain-publication rules.
Copy captures a source generation. Multipart completion binds an exact selected
part set; a repeated completed request returns its recorded outcome.

`DatasetClient` attaches `DatasetAuthority`, `DatasetUploads` and
`DatasetSessions` to these same clients. Shared composition helpers enroll
upload ownership before edits and bind candidate ownership before publication.
Snapshot/Read Plan/Split selection, worker partition, CAS, durable intent
recovery and cleanup remain in the existing Dataset services.

## 2. Shared runtime

`ClientRuntime` owns or borrows control, metadata and data clients. Business
handles share transport groups, discovery, topology observations and resource
budgets. Compatible writer policies share one bounded small-writer pool;
incompatible policies use distinct pools while sharing the transport owners.
S3 and Dataset chunks retain their respective types; native embedding introduces no generic user-data
chunk type.

`connect` creates owned groups. `adopt_clients` transfers shutdown
responsibility for existing groups. `from_clients` borrows them, so shutting
down that facade cannot stop its original owner's workers. These ownership
choices are explicit rather than inferred from an Arc reference count.

## 3. Topology and selection

`TopologyView` retains catalog generation, independently versioned disk routes,
service observations and actual transport configuration/counters. Sources have
separate observation ages and are not a globally atomic topology epoch. An
independent refresh with unknown observation time reports unknown freshness.
Catalog regression or invalid pages cannot replace a valid cached generation.

`ResolvedObject` binds one published object record to a tenant and metadata
authority. `object_locality` returns a bounded page of logical extents with
layout revisions, fragment roles and rack/node/disk observations. Inspection
does not dial storage, publish placement or replace routing checks. Execution
uses current routes and layout fencing while preserving the selected object's
identity. A retained old view remains inspectable but cannot pin execution to
an old owner or force a deleted/expired layout into a read.

## 4. Data and completion

`ObjectBody` is a lazy pull stream of `BufferLease` values. Each lease exposes
request/stream identity, sequence, logical offset, length, allocation capacity
and registration identity. Storage frame/control bytes are excluded from the
logical object stream. `OwnedPayload` retains the allocation through RPC,
DiskIO, ChunkIO, metrics wrappers and S3 adaptation.

Successful EOF requires all applicable length and integrity checks. Delivered
metadata or a valid prefix is not full-read success. Terminal errors remain
sticky across polling; early body drop is cancellation. Cancelling one pending
pull preserves its position when the body itself remains live.

Ordinary input retains its allocation or uses a bounded adapter. A unique
producer allocation can be filled directly in final storage frame slots,
frozen and consumed by `write_framed`. Submitted input is immutable and retained
through physical completion. Acceptance of an input buffer is distinct from
durable object publication; only successful finalization establishes success.
A lost publication reply retains uncertainty and cannot authorize a blind
replay with a new identity.

## 5. Budgets and shutdown

Admission is lock-free and bounded across requests, producers and caller-held
allocation capacity. Shared views charge physical capacity once until the final
owner releases it. An allocation's registration budget is separate from SDK
held bytes, posted receives, SQ work requests and writer windows.

Held-capacity exhaustion pauses production without blocking RPC completion
workers. Releasing the final owner returns capacity and wakes waiters; deadlines,
cancellation and shutdown remain observable. Unknown external owners reserve
bounded capacity before a compatibility copy. Open small strips can retain
input for replica repair after operation completion until seal or pool drain.

Shutdown closes admission, drains owned writer groups and joins owned workers.
All waiters observe one shutdown result; cancelling a waiter does not cancel
the shared cleanup. Caller-held leases retain storage after shutdown, and final
release never reposts to a destroyed transport. A host-to-GPU copy holds its
host lease until the copy completes, independently of stream completion.

## 6. Transport and integration

Metadata and data groups retain their own TCP/RDMA policies. Disabled RDMA uses
TCP; eligible setup failures follow the configured fallback policy. Accepted
or partial RDMA reads fail terminally through routing, mirror/EC recovery and
object delivery; they cannot append a TCP suffix or switch replicas to conceal
an interrupted transfer. Ambiguous writes preserve uncertainty.

Eligible registered producers avoid staging; eligible receives transfer their
allocation without a second SDK payload copy. Framing/control decoding,
alignment and EC construction/reconstruction retain their required copies.
The base lease API exposes host memory and opaque ownership, not verbs objects,
GPU pointers or RDMA READ/WRITE authority.

The independent Dataset HTTP distribution has no dependency on this storage/FFI
runtime. A Dataset facade attaches to the common runtime and delivery contracts
while retaining Dataset authority, Snapshot/Split binding and explicit
acknowledgement. Buffer release, download completion and processing
acknowledgement remain separate events. `BatchBody` delivers field metadata and
lazy payload leases with per-part and whole-field integrity checks. Active
reads renew the existing 60-second protection while live; cancellation stops
renewal and releases the active handle, with TTL expiry bounding interrupted
cleanup. Durable session close is explicit and never acknowledges processing.

The optional native Python extension owns a shared runtime and exposes retained
immutable leases. Slicing shares allocation ownership; conversion to Python
bytes explicitly copies. Pulls are serialized per batch with a bounded request
channel and no automatic payload prefetch. Runtime shutdown preserves already
delivered leases. Forked processes create new runtimes. Python control commands
reuse the Rust facade and shared authority schema without another storage path.

## 7. Correctness invariants

- **NS-I1 — Trusted embedding:** native tenant/context execute inside the
  internal-service trust domain without a per-operation HTTP authentication hop.
- **NS-I2 — Shared semantics:** HTTP and native frontends use the same selection,
  durability, publication, multipart and Dataset cleanup authorities.
- **NS-I3 — Non-authoritative observation:** topology inspection never changes
  placement or bypasses owner, generation or layout validation.
- **NS-I4 — Verified completion:** only successful EOF establishes a full read;
  a terminal failure cannot later become successful EOF.
- **NS-I5 — Final-owner accounting:** completion does not release caller-held
  capacity; the allocation and its charge survive every dependent owner.
- **NS-I6 — Safe shutdown:** closed admission and drained workers do not
  invalidate bytes still held by a caller or asynchronous copy.
- **NS-I7 — Bounded transport failure:** accepted RDMA failures never append
  TCP data or replay an ambiguous mutation.
- **NS-I8 — Explicit Dataset progress:** fetching, verified EOF and final buffer
  release cannot advance a read cursor; acknowledgement alone records progress.
