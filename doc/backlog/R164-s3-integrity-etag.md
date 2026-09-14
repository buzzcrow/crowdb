<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R164: access server / S3 — Object integrity, checksum, and ETag contract

## Problem

Chunk integrity protects stored blocks but does not define S3-visible checksum
headers or ETag semantics. Treating ETag as an unspecified chunk hash would
break client conditions and future multipart compatibility. Streaming must
compute integrity without collecting the object or adding hidden copies.

The integrity flow is
`doc/design/accessserver/design-crowdb-access-server-s3.md` §§2–5.

## Solution

1. Define the supported request checksum algorithms and exact S3 response
   headers for the first milestone. Validate a supplied checksum incrementally
   over logical object bytes before publication.
2. Define single-part ETag independently from physical chunking, EC layout,
   encryption, or HTTP frame boundaries. Persist algorithm/version metadata so
   future formats do not reinterpret old values.
3. Walk `BufferChain` views directly for PUT digests and verify chunk-reader
   integrity before yielding GET bytes. Integrity code cannot require one
   contiguous object allocation.
4. Persist final logical length, checksum values, and ETag in the immutable
   generation used by PUT, HEAD, GET, and conditional request evaluation.
5. On mismatch or corrupt metadata/data, fail before publication or terminate
   the read with a correlated internal integrity event; never return a
   successful response containing known-corrupt bytes.

## Dependencies

- Depends on R153 metadata fields and R155 streaming input.
- R156/R157 consume persisted values; R163 maps integrity failures.
- Multipart ETag semantics are deferred to R167 and cannot change single-part
  values retroactively.

## Acceptance

- Given known vectors and randomized fragmented views, when every supported
  checksum and ETag is computed, assert equality with contiguous reference
  implementations. Invariant: buffer boundaries do not affect integrity.
  Unit test.
- Given a correct and incorrect request checksum, when PUT finishes input,
  assert only the correct object can reach R154 publication. Invariant: a
  declared checksum is verified before visibility. E2E test.
- Given HEAD, full GET, range GET, and conditional requests for one generation,
  when attributes are evaluated, assert they use the same persisted ETag and
  checksum contract. Invariant: one generation has one integrity identity.
  Integration test.
- Given corrupt stored bytes or inconsistent metadata length, when GET reads,
  assert success is not reported and the integrity event identifies the
  internal request without leaking topology. Invariant: known corruption is
  never served as valid data. E2E test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-chunk-client --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
