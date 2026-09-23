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
- [x] **File records and keys**: extend the versioned envelope with bounded native
  file authority and exact-location binding; separate file kind, content format,
  digest, length, inline payload and chunk root. Bind every record to identities.
  Files: file model/key modules, record codecs, protocol schema and codec tests.
- [x] **Immutable publication primitive**: stage an immutable FileId record before
  exact-location CAS; equal digest/length/kind/format returns the original file,
  conflicts never overwrite and losing candidates remain discoverable. Shared
  authoritative context checks fence retired catalog access. Four fault/concurrency
  tests cover lost stage/publication replies, collisions and corrupt bindings.
  Files: file repository, shared context helper and repository tests.
- [~] **Seal and publication**: validate complete input and fixed-size hints;
  select inline only for eligible metadata within 16-KiB stored/64-KiB compression
  limits. Publish exact-location bindings conditionally, retaining losing uploads
  for future reclamation. Files: file repository/writer and fault tests.
  Inline selection and bounded LZ4 decoding are implemented: metadata can remain
  raw through 16 KiB or compress from at most 64 KiB; other file kinds always use
  the chunk variant. Five record tests cover codec/key/tag/corruption boundaries.
  The publication primitive requires already sealed chunk input; no HTTP route
  exposes it until the streaming seal pipeline verifies canonical bytes/formats.
  Standard PUT cannot infer equality-delete usage from bytes alone; the exact
  HTTP kind binding awaits the R177 decision below. Format parsing is independent.
- [x] **Bounded chunk streaming**: store at most 256-KiB leaves and 256 child
  references per directory, with at most eight directory levels. Persist directory
  bytes in chunks, not KV; bind every directory to file/catalog/table identity,
  digest, child heights and byte coverage. Pull reads keep one leaf and produce
  at most 64-KiB frames without speculative reads. Full reads verify the file digest.
  Files: file blocks/directory/range/reader/writer and streaming tests.
- [x] **Streaming JSON structure**: validate metadata JSON through a bounded
  pull-reader bridge and serde's ignored-value parser, never materializing the
  metadata graph. Independently enforce file bytes, active workers, raw UTF-8,
  object root and nesting before parser scratch can grow. Cancellation preserves
  admission until the worker exits; no new locks or whole-file allocation.
  Three tests cover large strings, split Unicode, malformed input, caps and
  cancellation. This does not replace table/schema semantic validation.
  Files: file JSON sealer/reader/scan modules and JSON tests.
- [x] **Durable chunk publication boundary**: native blocks call opt-in
  `SharedObjectWriter::finish_durable`; existing small-write completion remains
  asynchronous. Confirm the readable cursor before exposing each block. The real
  file-tree test exposed the old early-completion mismatch; no reader retry or
  timeout change was used. Three focused chunk tests verify waiting, an older
  pending advance and metadata failure; old asynchronous tests remain required.
  Files: chunk shared writer/pipeline publication and small-object tests.
- [ ] **Streaming HTTP integration**: bound response credits and cancellation
  over the native pull reader. Files: server FileIO body path.
  A verified Hyper body adapter now emits at most 16-KiB frames, starts storage
  reads only on body polling and holds one shared admission credit until completion
  or cancellation. Three tests cover partial ranges, exact size hints, bounded
  reads, errors and dropping an in-flight response. Listener routing is pending.
- [x] **Delegation tokens**: sign bounded claims for catalog/activation epoch,
  table, principal, nonce, exact operations, expiry and separate request/file byte
  limits. Derive per-grant S3 credential material without a credential registry;
  reject altered tokens and stale contexts before authorization. Four focused
  tests verify cross-server reconstruction, scope, expiry and independent limits.
  Files: file credential/token modules and credential tests.
- [ ] **Delegation and HTTP**: short-lived catalog/table/prefix-scoped operation
  and byte limits, no DELETE; isolated S3-shaped routing and errors. Files: file
  credentials/S3 compatibility and server FileIO modules, real HTTP tests.
  Native request authentication now reconstructs one grant's credentials and
  reuses only the shared SigV4 verifier, never general S3 credential/metadata
  authority. Three server tests cover header and presigned requests, exact grant
  expiry, tampering, duplicate fields and byte caps. HTTP routing, streaming limit
  enforcement and credential vending through table endpoints remain unimplemented.
  Path-style request parsing now recognizes only native exact-object operations
  and multipart subresources, decodes percent escapes once and rejects duplicate
  parameters, path escape, ordinary buckets and file DELETE. Four parser tests
  pass; it is not yet attached to a public listener or durable multipart driver.
- [ ] **Multipart state**: independently bounded durable sessions/parts/bytes/TTL;
  recover completion, duplicate uploads and logical abort without physical delete.
  Files: file multipart modules, record schema and crash/restart tests.
  Resumable writer foundations persist a bounded frontier in a chunk and return
  one fixed-size checkpoint root. Digest checkpoints bind file identity and use
  the existing RustCrypto SHA-256 compression function; no new crypto dependency,
  unsafe code or toolchain requirement. Three digest tests compare padding,
  update/restart boundaries and a million-byte vector against the standard hasher.
  Three writer tests cover resumed partial leaves, directories, orphan retention,
  failed checkpoint writes, corruption and wrong identities. Durable session/part
  authority, completion freezing and recovery workers remain unimplemented.
  Real native storage also passes checkpoint restoration through a newly connected
  chunk client before final publication and the existing Chunk-KV restart checks.
  Next steps: define immutable per-session limits and phase invariants; add scoped
  session/part authority codecs; serialize admission and part replacement through
  durable CAS journals; freeze bounded completion pages; checkpoint completion
  progress by byte budget; recover abandoned sessions without physical deletion.
  Staged-tree reads now validate physical identity/bytes without constructing a
  fictitious complete-file format record. Two tests cover multipart fragments,
  ranges, wrong owners, empty digests and invalid bounds.
  The assembly byte engine now copies at most one configured window from one
  selected part, checkpoints both target and current-part SHA-256 progress and
  binds resumptions to selection/part identity. Four tests verify recovery,
  empty parts, exact concatenation, part-digest mismatch, lost writes and caps.
  This engine requires a frozen selection and CAS journal supplied by the next
  persistence layer; it does not yet authorize or publish multipart uploads.
  Session/part models now validate separate part/file/staged-byte limits, TTL,
  identity/revision, selection binding and Open/Completing/Publishing/Published/
  Aborted phase coherence. Four model tests cover normal and invalid transitions;
  Session/part FlatBuffers records now use independent catalog key scopes, bind
  decoded identities to keys and reject unknown phases, invalid revisions and
  oversized digest checkpoints. Three persistence tests cover every phase,
  partial assembly, corruption and cross-domain keys. CAS mutation journals and
  runtime admission remain next; codecs alone do not admit uploads.
- [ ] **Projections**: generation-local bounded derived JSON pages and canonical
  fallback on every invalid projection. Files: metadata projection modules/tests.
- [ ] **Format validation**: bounded Avro blocks, v1/v2/v3 inheritance and row IDs,
  deletion vectors and fixed-size Parquet/ORC/Avro/Puffin hints. Files: format
  validation/probing and streaming fixtures.
  Canonical Parquet and Puffin framing probes now derive bounded footer locations
  without trusting stored hints or allocating advertised footer sizes. They check
  magic, signed Puffin lengths, reserved flags and cross-leaf reads. Four tests
  pass; this is not footer decoding, semantic validation or complete file sealing.
  ORC probing reads at most 255 postscript bytes and validates protobuf framing,
  footer/metadata spans and optional postscript magic. Three additional tests cover
  unknown fields, legacy header magic, maximum size and malformed wire inputs.
  Avro OCF framing now pulls one encoded block at a time with independent header
  bytes, metadata entries, encoded block bytes and record-count limits. Positive
  and sized negative metadata maps, sync markers, overflow and cancelled readers
  are checked across leaf boundaries. Null and raw-deflate codecs now enforce an
  independent decoded-byte cap and reject truncated or concatenated streams.
  Schema resolution, optional codecs and manifest v1/v2/v3 validation remain.
  A constant-state manifest inheritance resolver now handles v1 zero sequences,
  added-only sequence inheritance, explicit ages, upgraded existing-file row IDs,
  data/delete separation and checked row-ID advancement. Five semantic tests pass.
  It is not yet connected to Avro schema decoding or table commit admission.
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

## Verified Checkpoint

- 161 library tests pass, covering namespace, file records, range/streaming,
  credentials, JSON, format framing, Avro blocks/codecs, manifest inheritance,
  digest/writer checkpoints, staged assembly and multipart models/records.
  Focused native request authentication, pull-body and request parsing tests pass
  with Iceberg enabled and the general S3 listener feature disabled.
- Native file-tree publication, full read, a range crossing leaf boundaries and
  Chunk-KV restart pass against real ChunkDB/DiskIO using the separate
  `iceberg_file_storage_test` target. This verifies storage bytes, not Parquet
  semantics or the pending FileIO HTTP and official-client contract.
- Command: `pixi run clean-env && CROWDB_RUNTIME_ROOT="$PWD/.crowdb-runtime/ephemeral/iceberg-file-storage" pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_storage_test -- --nocapture`.

## Blocked

Only standard-FileIO semantic kind binding awaits a high-level decision, recorded
in R177. The backed-up table specification's Equality Delete Files section puts
usage in manifest `content`/`equality_ids`; ordinary FileIO writes only a path and
bytes. Inferring kind from `.parquet` or schema alone is unsound. A physical
storage-family record plus generation-bound usage preserves standard clients;
per-file upload intents preserve early semantic kind but require adaptation.
Continue credentials, format parsers, multipart storage and projections; do not
expose guessed kind classification or claim complete writable FileIO acceptance.
