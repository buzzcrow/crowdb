<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# S3 Streaming PutObject Plan

Upstream: [R155](../backlog/R155-s3-streaming-put-object.md) and
[Access Server S3 design](../design/accessserver/design-crowdb-access-server-s3.md).

Goal: stream HTTP input through bounded chunk writers and, only after writer
completion returns locations, publish complete object metadata with one KV Put.

## Writer bridge

- [x] **Bridge chunk completion**: adapt `ChunkIoWriter` small and large write
  completion locations into the S3 object metadata data reference. Files:
  `lib/crowdb-access-s3/src/`, `lib/crowdb-chunk-client/src/`.
- [x] **Enforce publication ordering**: invoke the one object-key publication
  only after `on_finish` returns locations; failure invokes no KV mutation.
  Files: `lib/crowdb-access-s3/src/`.
- [x] **Classify final outcome**: return success, definite coded error, or
  timeout; on definite KV error cleanup receives typed dedicated chunks or
  shared ranges. Shared-range physical reclamation stays deferred on R95.
  Files: `lib/crowdb-access-s3/src/`.

## Streaming

- [x] **Bound input**: consume one HTTP frame at a time under writer credits,
  without polling Hyper for the next frame until the current one is accepted.
  Files: `lib/crowdb-access-s3/src/`.
- [x] **Preserve chunk writer paths**: shared small writers and prepared
  dedicated large writers both implement `ChunkIoWriter`, so S3 uses one
  backpressured body bridge without S3-side copies. Files:
  `lib/crowdb-access-s3/src/`, `lib/crowdb-chunk-client/src/`.

## Object buffer provider

- [x] **Replace frame allocator with an object provider**: make Hyper retain a
  writable region across partial reads, expose reserved-prefix/suffix payload
  regions, and install one provider only after PUT authentication, writer
  preparation, and admission. Files: `third-party/hyper/src/body/`,
  `third-party/hyper/src/proto/h1/`, `lib/crowdb-access-s3/src/native_buffer.rs`,
  `app/crowdb-access-server/src/s3/`.
- [x] **Finalize native physical frames in place**: allocate bounded 1 MiB
  owners, divide them into 64 KiB slots, fill only payload regions, and write
  storage header/footer into reserved bytes before handing a full owner or EOF
  prefix to the chunk pipeline. Files: `lib/crowdb-access-s3/src/`,
  `lib/crowdb-protocol/src/frame.rs`, `lib/crowdb-chunk-client/src/`.
  A full 1 MiB owner or EOF prefix reaches the large writer once; the writer
  finalizes each frame in place against its actual chunk and slices the same
  owner at chunk, strip, and block boundaries.
- [x] **Share payload with integrity and EC**: bind MD5/SHA state to the object
  body lifecycle and feed the same immutable payload views to the strip-scoped
  incremental parity state. Files:
  object provider lifecycle and feed the same immutable payload views to the
  strip-scoped incremental parity state. Files:
  `lib/crowdb-access-s3/src/integrity.rs`,
  `lib/crowdb-chunk-client/src/worker/`,
  `lib/crowdb-common/rust/src/ec.rs`.
  The integrity pipe hashes Hyper's owner-backed payload views. The large-write
  EC worker folds each arriving block directly into
  parity, including a zero-padded short tail, and no longer retains every data
  shard for a second full-strip read at finish. It consumes scattered owner
  views and DiskIO receives those views without payload assembly.

## RPC buffer views

- [x] **Bound the transport view chain**: extend one RPC data payload with a
  fixed-capacity immutable buffer chain, retain the single-buffer fast path,
  and make writev, partial-write restoration, metrics, and release walk the
  same descriptors. Keep the batch-wide descriptor count below the transport
  hard limit without a fallback allocation. Files: `lib/crowdb-rpc/include/`,
  `lib/crowdb-rpc/src/`, `lib/crowdb-rpc/tests/`.
- [x] **Expose safe Rust chains**: add an owning `BufferChain` and bounded
  client call API which transfers every owner exactly once and rejects empty
  or oversized chains before submission. Files: `lib/crowdb-rpc/ffi/src/`,
  `lib/crowdb-rpc/ffi/tests/`.
- [ ] **Carry edge views to DiskIO**: use the single-owner fast path for normal
  1 MiB buffers and the bounded chain only for header read-ahead and final edge
  shapes; never coalesce PUT payload. Add copy/view accounting. Files:
  `lib/crowdb-diskio-client/src/`, `lib/crowdb-chunk-client/src/`.
  Normal native owners now use the single-owner path and writer boundary
  slicing reaches DiskIO as views. Header read-ahead becomes one short frame
  with separate metadata/payload views, then reception resumes with native
  owners. Explicit copy/view counters remain.

## Tests and gates

- [~] **Integration tests**: assert writer failure does not publish and writer
  completion produces one object-key KV Put. Files:
  `lib/crowdb-access-s3/tests/*_test.rs`.
- [x] **Required gates**: run affected S3, chunk client, RPC, and Hyper gates.
  Files: workspace.

## Gate notes

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets` and
  `pixi run -- cargo test -p crowdb-chunk-client --all-targets` pass.
- Hyper's stable supported feature set passes with
  `pixi run -- cargo test --manifest-path third-party/hyper/Cargo.toml --features full`.
  Its `--all-features` command is not a stable gate: it enables the upstream
  `nightly`, `ffi`, and `tracing` features, which explicitly require nightly
  or `hyper_unstable_*` compiler configuration.
