<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R242: Access SDK — Trusted internal S3 fat client and shared RDMA runtime

## Problem

Data-processing and AI systems inside the trusted cluster need to embed
storage, inspect storage topology, schedule work near data, and control
concurrency and buffer lifetime. Routing every operation through Access Server
adds an application-to-server socket and async hop and hides this topology.

A fat client is explicitly a trusted internal cluster component in the same
trust domain as Access Server, not an ordinary external-user SDK. It connects
directly to metadata and data services. It does not authenticate its own
operations by verifying SigV4 or receive the server's user-secret database.

`crowdb-access-s3` already owns metadata, conditions, retrieval, publication,
and multipart components, but complete operations remain partly coupled to
HTTP handlers. The independent `sdk/` workspace is a portable Dataset HTTP
client. Existing RPC buffers carry lease metadata, but completion and access
adapters still include copying paths. Complete these boundaries rather than
replacing accepted storage clients or transport.

The root contracts are [Access Server](../design/access-server/design-crowdb-access-server.md)
and [RDMA service interactions](../design/rpc/design-crowdb-rpc-rdma.md#7-service-interactions).

## Solution

1. Provide a shared native cluster runtime in the main Rust workspace. Reuse
   `crowdb-kv-client`, `crowdb-chunk-kv-client`, `crowdb-chunk-client`,
   `crowdb-diskio-client`, and `crowdb-rpc-ffi` for discovery, routing,
   TCP/RDMA connections, producer allocation, admission, and shutdown. Business
   handles share these resources. Keep the existing `sdk/` distribution
   independent of server/storage FFI dependencies.
2. Expose storage topology as a supported native API: service endpoints,
   node/disk relationships, Chunk-KV owners, and resolved chunk placement for
   caller scheduling and prefetch. Views retain existing source generations
   and freshness information, without promising a globally atomic snapshot.
   Stale views refresh or fail through existing routing/fencing rules.
   Observing topology cannot publish placement or bypass generation checks,
   durability, or publication rules.
3. Implement S3 first with typed bucket/object, HEAD/GET/range, PUT, listing,
   delete, copy, and multipart operations backed by `crowdb-access-s3`.
   Extract shared execution from `app/crowdb-access-server/src/s3/`.
   HTTP retains parsing, SigV4 verification, and HTTP/XML encoding; native
   callers supply explicit tenant and operation context. Both preserve the
   same object visibility, conditions, integrity, generation, mutation
   outcomes, and cleanup. Native execution needs neither a user-secret lookup
   nor an Access Server authentication round trip.
4. Separate business APIs from common data delivery. Reads return typed
   metadata and an ordered, pull-based host-buffer stream. Metadata or a
   delivered prefix does not establish full-read success; successful
   termination includes applicable checksum and length checks. A terminal
   error remains observable; later polling cannot turn failure into success.
   Writes accept bounded ordinary input or uniquely writable buffers frozen before
   submit. Input acceptance is distinct from durable object publication;
   only successful finalization establishes PUT or multipart completion.
   Ambiguous publication retains existing uncertain-outcome semantics.
5. Complete the existing opaque RPC buffer lease contract through C++/Rust
   completion and the native API. A lease exposes request identity, sequence,
   logical offset, actual length, capacity class, and release without verbs
   objects. Caller-owned storage stays valid until final release, including
   after cancellation or runtime shutdown. Eligible payload paths transfer
   ownership without a second SDK payload copy; decoding, alignment staging,
   and EC reconstruction retain necessary copies. Wire-control fragments are
   not object data.
6. Keep hot-path admission lock-free and bounded across requests, producers,
   and caller-held buffer capacity. Shared views charge an allocation once
   until final release; operation completion does not return held-buffer
   credits. Exhaustion pauses production or returns a typed capacity/deadline
   outcome without an unbounded waiting queue. Completion callbacks never
   block RPC workers. Cancellation stops new work and resolves once;
   uncertain in-flight storage stays retained or quarantined until safe.
7. Preserve TCP/RDMA path selection. Disabled RDMA uses TCP; eligible setup
   or known pre-payload failures may fall back with the same request identity
   under existing retry rules. Accepted/partial RDMA operations never append
   a TCP suffix or replay ambiguous mutations. This fallback does not select
   HTTP. GPU delivery initially means explicit host-to-GPU copy completion
   before host-lease release, with no GPU pointer or RDMA READ/WRITE in the
   base ABI.
8. Keep runtime, topology, and buffer APIs independent of S3 so Dataset can
   attach after S3 acceptance under [R220](R220-dataset-fat-client-plan.md).
   Dataset keeps Snapshot/Read Plan/Split and explicit acknowledgement in
   `crowdb-access-dataset`; it reuses the runtime and data delivery without
   encoding samples as S3 objects. Buffer release, operation completion, and
   Dataset acknowledgement are separate events. R242 delivers the shared
   foundation and S3; R220 owns the later Dataset adapter, measurements, and
   Python integration.

## Dependencies

- Accepted registered buffers/RDMA RPC and
  [Chunk-KV/access integration](../design/rpc/design-crowdb-rpc-rdma.md#7-service-interactions).
- Existing S3 library components, HTTP operation orchestration, ChunkIO
  read/write policies, and RPC FFI ownership conventions.
- Existing Dataset HTTP SDK packaging and boundary checks are regression
  controls, not a place to introduce native storage dependencies.
- R220 consumes the accepted runtime and S3-first API pattern. Its Dataset
  measurements and bindings do not gate R242 or require a second transport,
  topology cache, or buffer pool.
- Missing RDMA hardware uses the accepted bounded TCP path. Host-memory tests
  and simulated asynchronous GPU copies need no GPU; real-HCA acceptance is
  an explicit separate profile.

## Acceptance

- Given cluster endpoints and a tenant but no S3 secret database or HTTP
  listener, when native S3 executes an object operation, it reaches storage
  directly. Invariant: trusted internal execution. Integration test.
- Given two handles sharing a runtime, when they execute and close
  independently, they share connection/admission owners without invalidating
  the other's data. Invariant: shared runtime lifetime. Integration test.
- Given service, disk, catalog, and object-placement observations, when the
  caller inspects topology and schedules range reads, the API exposes their
  identities/freshness and returns the requested logical bytes. Invariant:
  supported topology surface. Integration test.
- Given a view whose owner or endpoint changes, when a request uses it,
  routing refreshes or returns a typed stale/fenced outcome without changing
  logical object identity or publishing placement. Invariant:
  non-authoritative topology. Integration test.
- Given identical supported bucket/object, copy, and multipart operations,
  when HTTP and native entry points encounter success, condition/range errors,
  conflicts, or publication failures, logical bytes, metadata, and semantic
  outcomes agree. Invariant: S3 frontend parity. Integration test.
- Given truncated data or a final checksum mismatch, when a caller drains a
  stream after receiving a valid prefix and polls after the error, termination
  remains failed rather than becoming successful EOF.
  Invariant: prefix delivery is not full-read success. Integration test.
- Given writable producer storage, when frozen input is submitted, callers
  cannot mutate it; final success waits for durability/publication and lost
  publication replies retain uncertainty. Invariant: input ownership and
  publication completion. Integration test.
- Given an eligible direct RDMA payload path, when a multi-buffer read
  completes, ordered owner-backed views preserve request, sequence, offset,
  and length without a second SDK payload copy. Invariant: caller-owned
  payload lifetime. Integration test.
- Given opaque leases and shared views, when access and final release cross
  FFI, ABI/access rules hold and each allocation is released once. Invariant:
  lease ABI ownership. Unit test.
- Given all admitted capacity is caller-held, when another buffer is needed,
  bounded backpressure resumes after final release or ends at its deadline;
  stream completion alone does not return held credits. Invariant: bounded
  SDK memory. Integration test.
- Given in-flight operations and held leases, when cancellation or shutdown
  closes new admission, completion resolves once and held bytes stay valid
  until release without reposting to destroyed transport. Invariant:
  cancellation and shutdown ownership. Integration test.
- Given an asynchronous host-to-GPU copy simulation, when the stream closes
  before copy completion, the host lease stays valid until copy completion and
  release. Invariant: copy dependency lifetime. Integration test.
- Given disabled RDMA or an eligible pre-payload failure, when native access
  runs, TCP preserves result/request identity; accepted or partial RDMA failure
  instead fails without a TCP suffix or ambiguous mutation replay. Invariant:
  transport parity and failure boundary. Integration test.
- Given the standalone Dataset SDK, when boundary and functional gates run
  alongside the native client, HTTP behavior and independent packaging stay
  intact. Invariant: independent HTTP SDK. Integration test.
- Given native and OCI S3 clients, when callers retain leases under pressure,
  no-device TCP and shared-HCA profiles record actual topology/transport,
  exact frontend results, bounded memory, and safe shutdown. Invariant:
  embedded storage integration. E2E test.

Run after registering the native crate:

```bash
pixi run -- cargo fmt --all -- --check
pixi run -- cargo test -p crowdb-sdk-native --all-targets
pixi run -- cargo test -p crowdb-access-s3 --all-targets
pixi run -- cargo test -p crowdb-rpc-ffi --all-targets
pixi run -- cargo test -p crowdb-access-server --all-targets
pixi run -- cargo clippy --workspace --all-targets --all-features -- -D warnings
pixi run check-dataset-sdk-boundary
pixi run test-dataset-sdk
pixi run test-e2e-s3
```
