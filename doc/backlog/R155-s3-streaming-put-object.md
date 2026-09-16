<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R155: access server / S3 — Streaming PutObject

## Problem

Upstream Hyper allocates HTTP/1 request bodies into internal Rust buffers, and
the current chunk/RPC write path assumes contiguous `Bytes` at important
boundaries. Collecting a large object or copying every frame into fixed blocks
would multiply memory traffic. Unbounded producer queues would also let slow
storage exhaust service memory.

The receive design is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §4; fork
management is `doc/dev/hyper_fork.md`.

## Solution

1. Extend the pinned Hyper fork with an opt-in HTTP/1 body-buffer provider
   selected after header authentication/admission and before the first body
   poll. The provider lends successive writable payload regions; Hyper fills a
   region across partial socket reads and returns `Pending` when no region is
   available. CROWDB supplies the provider: the native implementation owns a
   bounded set of 1 MiB buffers subdivided into 64 KiB physical-frame slots,
   with frame header/footer bytes reserved around each payload region. A later
   provider may use registered or RDMA-pinned memory without changing Hyper or
   the writer contract.
2. Make the provider object-scoped. Once one slot is filled, it finalizes the
   frame header, checksum, and footer in the reserved bytes without moving the
   payload. Once a 1 MiB owner is full, or EOF finalizes its used prefix, hand
   that same owner to the chunk write pipeline. HTTP read boundaries are not
   storage frame boundaries. A bounded body prefix already present in Hyper's
   header buffer is represented by immutable views with a separate header and
   footer rather than copied.
3. Fan each immutable payload view out to two consumers over the same owner:
   the object-scoped ETag/Content-MD5/SHA-256 pipeline and the strip-scoped EC
   pipeline. The integrity state ends only with the object; EC state rotates
   with each strip. Neither pipeline creates a second payload allocation.
4. Extend the TCP RPC request path to accept bounded scatter/gather buffers and
   use vectored writes. Validate the configured view maximum below every
   platform, RPC, and send-queue hard limit; derive each frame limit from that
   value, remaining queue capacity, and flush policy. Flush before overflow.
   The normal 1 MiB owner is a single RPC buffer. Bounded scatter/gather is
   reserved for the header-read-ahead prefix and final edge cases; reject a
   descriptor shape that cannot fit instead of copying payload. Record owner,
   view, prefix, and fallback counters separately.
5. Reject early EOF, length overflow, unsupported streaming signatures,
   timeout, cancellation, and
   writer failure without invoking R154 publication. The final outcome is
   exactly `Success`, `Error { code, message }`, or `Timeout`. A definite
   chunk or KV error may best-effort delete newly written data (a small-object
   shared-chunk range or dedicated large-object chunks); timeout means the KV
   mutation may have applied and must not synchronously delete data.
6. Finish data/parity, seal dedicated large-object chunks, persist the upload
   result, and call R154 publication. Use the existing shared writer directly
   for small objects; R159/R168 own their later range reclamation.

## Dependencies

- Depends on R152 service/fork integration, R153 schema, and R154 publication.
- R164 defines accepted checksums and ETag inputs; before it lands, tests use a
  single required internal digest without claiming final S3 compatibility.
- Changes `crowdb-chunk-client`, `crowdb-rpc`, and `crowdb-rpc-ffi` buffer APIs.
- The Hyper/native allocator and RPC ownership modules may contain narrowly
  scoped audited unsafe code; the access-server and S3 operation layers remain
  `unsafe_code = deny`.
- R161 supplies global/per-tenant admission policy; this requirement still
  enforces local bounded credits if R161 has not landed.
- R170 owns every cuObject/RDMA PUT concern and is not part of this requirement.

## Acceptance

- Given an object much larger than the memory budget and arbitrary partial TCP
  reads, when PUT completes, assert exact bytes are published and peak owned
  buffers stay within configured credits. Invariant: object size does not set
  service memory. E2E test.
- Given frame splits around every block, chunk, and EC boundary, when PUT uses
  1 MiB provider owners, assert stored data, ETag, and parity match contiguous
  input with no payload copy or coalesce. Invariant: HTTP framing is not a
  storage boundary and integrity/EC share the same owner. Integration test.
- Given descriptor or pool exhaustion, when the client continues writing,
  assert Hyper stops reading until a credit returns and no unbounded fallback
  allocation occurs. Invariant: backpressure reaches the client. Integration
  test.
- Given cancellation, timeout, early EOF, or chunk failure at each pipeline
  stage, when the request ends, assert no object becomes visible and every
  owner is released once or retained by a durable recovery intent. Invariant:
  failed streaming input cannot publish or leak anonymous data. E2E test.
- Given the default fork provider is disabled, when upstream HTTP transcripts
  run against fork and base behavior, assert equivalent parsing and errors.
  Invariant: the opt-in extension does not alter upstream behavior. Integration
  test.
- Given a configured view limit at, below, and above the discovered hard limit
  and a nearly full send queue, when requests are framed, assert invalid config
  is rejected and valid flows flush before overflow without exceeding queue
  capacity. Invariant: tunable fragmentation can never violate transport hard
  limits. Integration test.

Required gates:

- `pixi run -- cargo test --manifest-path third-party/hyper/Cargo.toml --features full`
- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run test-rpc-ct`
- `pixi run test-rpc-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
