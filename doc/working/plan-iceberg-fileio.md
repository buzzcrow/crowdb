# Iceberg FileIO Plan

Upstream: [immutable FileIO requirement](../backlog/R180-access-iceberg-fileio.md).

Goal: publish immutable native file identities with bounded streaming and durable
multipart, without general S3 authority or premature physical deletion.

R179 remains open for the recorded latency decision and later table admission
integration. Independent FileIO work proceeds under the approved ordering.

## Execution

- [x] **Canonical location**: introduce typed table prefixes and exact relative
  keys, lower-case unpadded base32 catalog IDs and lower-case hex table IDs.
  Keep S3 URI keys distinct from HTTP percent decoding; reject escape rather than
  normalize. Files: `src/file.rs`, `src/file/location.rs`, location tests.
- [~] **File records and keys**: extend the versioned envelope with bounded native
  file authority and exact-location binding; separate file kind, content format,
  digest, length, inline payload and chunk root. Bind every record to identities.
  Files: file model/key modules, record codecs, protocol schema and codec tests.
- [ ] **Seal and publication**: validate complete input and fixed-size hints;
  select inline only for eligible metadata within 16-KiB stored/64-KiB compression
  limits. Publish exact-location bindings conditionally, retaining losing uploads
  for future reclamation. Files: file repository/writer and fault tests.
- [ ] **Streaming reads**: bounded chunk writes, full and single-range reads,
  response credits and cancellation. Files: file reader/writer, server body path.
- [ ] **Delegation and HTTP**: short-lived catalog/table/prefix-scoped operation
  and byte limits, no DELETE; isolated S3-shaped routing and errors. Files: file
  credentials/S3 compatibility and server FileIO modules, real HTTP tests.
- [ ] **Multipart state**: independently bounded durable sessions/parts/bytes/TTL;
  recover completion, duplicate uploads and logical abort without physical delete.
  Files: file multipart modules, record schema and crash/restart tests.
- [ ] **Projections**: generation-local bounded derived JSON pages and canonical
  fallback on every invalid projection. Files: metadata projection modules/tests.
- [ ] **Format validation**: bounded Avro blocks, v1/v2/v3 inheritance and row IDs,
  deletion vectors and fixed-size Parquet/ORC/Avro/Puffin hints. Files: format
  validation/probing and streaming fixtures.
- [ ] **Acceptance**: official FileIO, real chunks/restarts, concurrency/lost
  responses, all boundary tests; run fmt and lint independently. No full feature
  advertisement or closure until the complete requirement passes.

## Files And Verification

- Library: `lib/crowdb-access-iceberg/src/{file,record,metadata_projection}/`.
- Protocol: `lib/crowdb-protocol/src/fbs/iceberg.fbs` and generated module.
- Server: `app/crowdb-access-server/src/iceberg/` and integration tests.
- Unit: exact location round trips, rejected aliases/escapes, byte boundaries,
  key binding, codec corruption, range parsing and immutable publish conflicts.
- Integration: bounded stream retention, real chunk range crossings, lost replies,
  multipart recovery, projection fallback and format block boundaries.
- E2E: pinned official clients using only delegated immutable operations; general
  S3 metadata remains isolated. Preserve the separate R179 latency blocker.
- Gates: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`, affected
  server/protocol tests, `pixi run -- cargo fmt --all -- --check`, `pixi run rs-lint`.
  Prefix server-spawning tests with `pixi run clean-env &&`.
