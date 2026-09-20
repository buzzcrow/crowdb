<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R170: access server / S3 — Optional cuObject RDMA data plane

## Status

**Deferred until the R152–R166 basic TCP S3 milestone is correct.**
It is unblocked when immutable-generation GET/PUT, DiskIO direct-read planning,
admission, integrity, and failure injection are stable without acceleration.

## Problem

GPU clients need to move large S3 payloads without routing bytes through
AccessServer memory or the kernel TCP payload path. Mixing cuObject negotiation,
registered memory, DC resources, and distributed completion into basic S3 would
couple correctness delivery to optional NVIDIA hardware and libraries. Running
the cuObject data endpoint only in AccessServer would also reintroduce a network
and host-memory bounce for bytes physically owned by DiskIO nodes.

The compatible extension boundary and distributed flow are
`doc/design/access-server/s3/design-crowdb-access-s3-rdma.md`.
NVIDIA's documented architecture places control in the gateway and cuObjServer
payload operations in data nodes.

## Solution

1. Implement the extension in a separate `crowdb-access-s3-cuobject` library,
   `crowdb-transfer-ffi`, and C++ transfer component. Basic `crowdb-access-s3`
   exposes only compile-time-compatible request, immutable-generation, and
   completion hooks. Without the optional feature, no cuObject library is
   linked, no negotiation branch exists, and no RDMA resource is created.
2. Keep the S3 HTTP/control endpoint and operation coordinator in
   `crowdb-access-server`. Parse the opaque cuObject request token, authenticate
   the S3 operation, resolve one immutable generation, and obtain a fenced
   logical read/write plan. Never expose raw storage topology to the client.
3. Start one local `cuObjServer` endpoint and bounded channel/DCI plus registered
   host-buffer pools in each enabled `crowdb-diskio` process. DiskIO owns local
   registrations and completion polling; opaque cuObject handles are never
   exported across processes.
4. For GET or one S3 range GET, split the logical interval into non-overlapping
   spans. Send each owner an authenticated internal task containing operation
   and generation IDs, storage reference, exact length, client destination
   offset, expiry, opaque descriptor, and authority restricted to that span.
5. Each DiskIO reads its logical bytes directly into a local registered native
   buffer and calls `handleGetObject()` with the exact remote start. Mirror
   plans select a healthy replica. EC plans assign an executor that reconstructs
   logical bytes before transfer; raw parity shards never enter object offsets.
6. Permit spans targeting disjoint client intervals to execute and complete in
   parallel. AccessServer returns the successful RDMA reply only after every
   authenticated completion succeeds. The client cannot consume the full
   destination earlier.
7. For PUT, plan bounded destination writers before transfer. Each DiskIO uses
   `handlePutObject()` to pull its authorized client interval into a registered
   buffer, feeds that owner into the chunk/EC write pipeline, and reports durable
   completion. The basic publication protocol publishes metadata only after
   every data/parity write,
   checksum, seal, and completion succeeds.
8. Fall back to basic TCP only before any RDMA payload operation is submitted.
   After a partial transfer, fail the whole operation, mark the client buffer
   invalid for GET or the upload unpublished for PUT, and require a new checked
   request. Never splice TCP bytes into a partial RDMA operation.
9. Validate ConnectX-5 DC v1 and at least one newer supported adapter. Enforce
   cuObject's per-operation, SGE, channel, CQ, registered-byte, timeout, and
   device limits through configuration and basic admission control.
10. Bind every internal span task to tenant, operation, generation, direction,
    remote interval, byte limit, expiry, and nonce. Treat the client descriptor
    as opaque sensitive capability data; never log it or accept it as S3
    authorization by itself.

The required four-node 4 MiB GET is:

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

## Dependencies

- Depends on R152–R166 basic TCP S3 behavior; it does not change their wire,
  publication, range, integrity, or error contracts.
- Uses `crowdb-chunk-client`, `crowdb-diskio`, internal authenticated RPC, and
  native ownership/completion support from `crowdb-rpc-ffi`.
- Accelerated multipart upload additionally depends on R167 and remains
  disabled until both requirements land.
- NVIDIA cuObject server libraries, compatible drivers, and ConnectX-5-or-newer
  hardware are optional deployment dependencies. Unsupported deployments keep
  the basic TCP service unchanged.

## Acceptance

- Given a build without the cuObject feature, when basic S3 starts and runs its
  performance suite, assert no cuObject dependency, RDMA resource, negotiation
  branch, or metric worker is present. Invariant: optional acceleration has no
  implementation impact when excluded. Integration test.
- Given a 4 MiB GET split across four DiskIO nodes, when one client descriptor
  is forwarded with four disjoint 1 MiB destinations, assert the nodes transfer
  in parallel, write exact bytes at offsets 0–4 MiB, and AccessServer replies
  only after four successful completions. Invariant: payload never traverses
  AccessServer memory. E2E test.
- Given mirrored and EC-backed ranges including reconstruction, when RDMA GET
  runs, assert the client receives logical object bytes and never raw parity or
  stale-generation data. Invariant: acceleration preserves chunk-reader
  semantics. E2E test.
- Given RDMA PUT fragmentation across writers and EC stripes, when all pulls,
  checksums, parity, and seals complete, assert the basic publication protocol
  publishes one exact object;
  inject each failure and assert no partial generation is visible. Invariant:
  RDMA completion alone is never publication authority. E2E test.
- Given rejection before submission and failures after one or more span
  submissions, when fallback handling runs, assert only the former may use TCP
  and the latter invalidates the operation without mixing transports.
  Invariant: fallback cannot conceal a partial RDMA transfer. Integration test.
- Given ConnectX-5 DC v1 and a newer supported adapter, when multiple DiskIO
  endpoints use one descriptor concurrently, assert dynamic connection,
  remote-bound checks, every completion order, configured limits, and buffer
  release after completion. Invariant: the minimum hardware and distributed
  descriptor flow are proven rather than inferred. E2E test.
- Given replayed, expired, wrong-generation, overlapping, out-of-bounds, and
  tampered span tasks, when DiskIO validates them, assert rejection occurs
  before RDMA submission and no descriptor appears in telemetry. Invariant:
  control-plane delegation cannot broaden memory or object authority.
  Integration test.
- Given shutdown with queued and in-flight transfers, when AccessServer and
  DiskIO drain, assert admission stops first, all completions settle or fail,
  and channels/buffers are destroyed only after their final owner. Invariant:
  native lifetime exceeds every NIC operation. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3-cuobject --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run test-diskio-ct`
- `pixi run test-rpc-ffi`
- `pixi run tree-lint`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
