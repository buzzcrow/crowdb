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

1. Extend the pinned Hyper fork with an opt-in HTTP/1 pooled body path. After
   header authentication/admission and before the first body poll, select the
   native glibc-backed provider, read decoded payload directly into its buffers
   across partial socket reads, freeze before delivery, and return `Pending`
   when credits are absent. The pooled payload path does not allocate body
   buffers through the Rust runtime. Preserve owner metadata so R170 can add a
   registered provider separately without changing the basic writer contract.
2. Add safe immutable owner/view/chain types at the RPC/chunk boundary. A view
   retains a Rust or native allocation plus offset and length; splitting at
   block or EC boundaries does not copy. Preserve the existing one-`Bytes`
   fast path as an additive API.
3. Stream body chunks through bounded checksum, chunk writer, and EC pipelines.
   Never allocate or retain memory proportional to object length. Stop polling
   Hyper while downstream batch or memory credits are exhausted.
4. Extend the TCP RPC request path to accept bounded scatter/gather buffers and
   use vectored writes. Validate the configured view maximum below every
   platform, RPC, and send-queue hard limit; derive each frame limit from that
   value, remaining queue capacity, and flush policy. Flush before overflow.
   EC walks corresponding view cursors; coalesce once only when one operation
   cannot fit, and charge copied bytes.
5. Copy only a body prefix already read with HTTP headers into the first pooled
   buffer. Record that bounded copy separately. Reject early EOF, length
   overflow, unsupported streaming signatures, timeout, cancellation, and
   writer failure without invoking R154 publication.
6. Finish data/parity, seal dedicated large-object chunks, persist the upload
   result, and call R154 publication. Use the existing shared writer directly
   for small objects; R159/R168 own their later range reclamation.

## Dependencies

- Depends on R152 service/fork integration, R153 schema, and R154 publication.
- R164 defines accepted checksums and ETag inputs; before it lands, tests use a
  single required internal digest without claiming final S3 compatibility.
- Changes `crowdb-chunk-client`, `crowdb-rpc`, and `crowdb-rpc-ffi` buffer APIs.
- R161 supplies global/per-tenant admission policy; this requirement still
  enforces local bounded credits if R161 has not landed.
- R170 owns every cuObject/RDMA PUT concern and is not part of this requirement.

## Acceptance

- Given an object much larger than the memory budget and arbitrary partial TCP
  reads, when PUT completes, assert exact bytes are published and peak owned
  buffers stay within configured credits. Invariant: object size does not set
  service memory. E2E test.
- Given frame splits around every block, chunk, and EC boundary, when PUT uses
  buffer views, assert stored data and parity match contiguous input with no
  coalesce below the descriptor limit. Invariant: HTTP framing is not a storage
  boundary. Integration test.
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

- `pixi run -- cargo test --manifest-path third-party/hyper/Cargo.toml --all-features`
- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run test-rpc-ct`
- `pixi run test-rpc-ffi`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
