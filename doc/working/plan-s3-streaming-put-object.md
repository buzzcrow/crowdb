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

## Tests and gates

- [~] **Integration tests**: assert writer failure does not publish and writer
  completion produces one object-key KV Put. Files:
  `lib/crowdb-access-s3/tests/*_test.rs`.
- [ ] **Required gates**: run affected S3, chunk client, RPC, and Hyper gates.
  Files: workspace.

## Gate notes

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets` and
  `pixi run -- cargo test -p crowdb-chunk-client --all-targets` pass.
- Hyper's stable supported feature set passes with
  `pixi run -- cargo test --manifest-path third-party/hyper/Cargo.toml --features full`.
  Its `--all-features` command is not a stable gate: it enables the upstream
  `nightly`, `ffi`, and `tracing` features, which explicitly require nightly
  or `hyper_unstable_*` compiler configuration.
