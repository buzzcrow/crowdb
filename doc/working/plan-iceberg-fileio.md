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
- [ ] **Seal and publication**: validate complete input and fixed-size hints;
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
  The upload adapter now independently admits at most 64 concurrent bodies, checks
  declared and actual byte ceilings, consumes at most one 64-KiB HTTP frame at a
  time and awaits each bounded native writer operation before polling again.
  It verifies content length and optional signed SHA-256 before returning a staged
  tree; cancellation, transport/storage errors and digest mismatches never publish
  authority. Four server tests cover round-trip bytes, all failure classes,
  backpressure and credit release while retaining uncertain orphan blocks.
  Trailer/checksum-streaming compatibility, grant intersection and listener
  integration remain pending; this primitive does not perform semantic sealing.
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
  failed checkpoint writes, corruption and wrong identities.
  Real native storage also passes checkpoint restoration through a newly connected
  chunk client before final publication and the existing Chunk-KV restart checks.
  Next steps: reserve global admission; connect semantic sealing/publication;
  recover abandoned sessions without physical deletion.
  Staged-tree reads now validate physical identity/bytes without constructing a
  fictitious complete-file format record. Two tests cover multipart fragments,
  ranges, wrong owners, empty digests and invalid bounds.
  The assembly byte engine now copies at most one configured window from one
  selected part, checkpoints both target and current-part SHA-256 progress and
  binds resumptions to selection/part identity. Four tests verify recovery,
  empty parts, exact concatenation, part-digest mismatch, lost writes and caps.
  This engine uses the frozen selection and CAS journal below; it does not itself
  authorize or publish multipart uploads.
  Session/part models now validate separate part/file/staged-byte limits, TTL,
  identity/revision, selection binding and Open/Completing/Publishing/Published/
  Aborted phase coherence. Four model tests cover normal and invalid transitions.
  Session/part FlatBuffers records now use independent catalog key scopes, bind
  decoded identities to keys and reject unknown phases, invalid revisions and
  oversized digest checkpoints. Three persistence tests cover every phase,
  partial assembly, corruption and cross-domain keys. Codecs alone do not admit uploads.
  The native multipart repository now persists initial sessions and reserves one
  part mutation by session CAS before replacing its part authority. Its bounded
  before/after snapshot permits recovery after every reservation, part write and
  fence-clear reply loss. Counts and current staged bytes change once, stale
  helpers cannot rewrite later revisions, and abort waits for a pending mutation
  before fencing further writes. Five tests cover insert/replacement crash points,
  competing abort, exact expiry, resource limits and retained completion evidence.
  This is not public admission: global credits, upload streaming, duplicate-part
  HTTP responses and global admission remain to be connected.
  Completion now freezes an ordered revision/digest selection in immutable payload
  pages before a session CAS fences further part replacement. At most 10,000 entries
  occupy 420,007 encoded bytes; each work step verifies that bounded selection and
  one selected part, copies one configured byte window and CASes its checkpoint.
  Four tests cover maximum selection framing, missing/changed parts, abort, invalid
  work limits and lost replies at selection and every progress boundary across
  repository instances. The assembled tree remains private pending semantic
  sealing; this does not implement the final HTTP Complete response or publication.
  A four-session recovery scan now settles one pending part mutation or performs
  one assembly byte window per visit. Expired open/completing sessions are logically
  aborted after pending part mutations settle; parts and checkpoints remain intact.
  Finished assembly is reported as awaiting semantic sealing, not as published.
  Four sweep tests cover multi-page progress, cross-instance visits, exact expiry,
  retained bytes, invalid/foreign cursors and corrupt pages before any mutation.
  Each native listener now runs the multipart sweep alongside namespace recovery.
  Per-session time budgets use the persisted catalog request bound; a timed-out
  session is deferred without preventing later entries in the same page. The
  outer page budget bounds scans and context checks; context changes reset cursors.
  Two additional tests verify timeout limits and that a blocked first part read
  cannot starve a later session's expiry or persist unfinished assembly bytes.
  A caller-provided sealed record is now frozen as an immutable payload before
  the publication phase CAS. Recovery replays the exact seal through immutable
  file publication and records the selected FileId, including an existing equal
  file's original identity. A proven unequal immutable location records a terminal
  Conflicted phase; ambiguous storage/context failures remain recoverable instead.
  Five publication tests cover all five lost-write boundaries, restart replay,
  equal/different locations, abort races, corrupt intent and lost conflict replies.
  This does not infer HTTP file kind or replace canonical format validation.
  Native part listing now reads one upload-scoped storage page of at most 256
  records, using numeric part markers and preserving gaps. A requested maximum up
  to 1000 may return a smaller truncated page. Session checks bracket the scan;
  pending mutations, expiry, terminal phases, stale snapshots and corruption fail
  closed rather than returning mixed part state. Five tests cover pagination,
  independent limits, adjacent uploads and a session mutation during the scan.
  S3 XML response encoding and per-part LastModified capture remain HTTP work.
  Global admission now persists independent session/byte limits and one bounded
  CAS journal. A session reserves its staged-byte ceiling before authority creation;
  only terminal sessions release it, retaining a policy/sequence-bound receipt.
  Recovery helps a pending precreation journal before scanning and later returns
  terminal credits, without deleting parts or introducing process-local locks.
  Nine model/record/driver tests cover separate limits, every create/release lost
  write, concurrent admission, policy mismatch, duplicate release and stale helpers.
  Public HTTP admission/configuration remains to be connected. Capacity of retained
  physical orphans remains the separate R177 trial-policy decision.
- [ ] **Projections**: generation-local bounded derived JSON pages and canonical
  fallback on every invalid projection. Files: metadata projection modules/tests.
- [~] **Format validation**: bounded Avro blocks, v1/v2/v3 inheritance and row IDs,
  deletion vectors and fixed-size Parquet/ORC/Avro/Puffin hints. Files: format
  validation/probing and streaming fixtures.
  Canonical Parquet and Puffin framing probes now derive bounded footer locations
  without trusting stored hints or allocating advertised footer sizes. They check
  magic, signed Puffin lengths, reserved flags and cross-leaf reads. Four tests
  pass; this is not footer decoding, semantic validation or complete file sealing.
  Puffin footer reading now bounds both encoded and decoded metadata to at most
  1 MiB, caps blob/field/property collections and rejects duplicate properties,
  overlapping or escaped blob ranges and invalid deletion-vector descriptors.
  Plain JSON and one sized LZ4 frame are supported; concatenated/truncated frames,
  bad checksums and expansion beyond the output ceiling fail closed. The existing
  LZ4 dependency's frame feature supplies checksum verification. Four tests cover
  canonical reads, compression, resource caps and exact manifest-to-footer
  offset/length/referenced-file/cardinality matching.
  Deletion-vector validation now re-reads the canonical descriptor and streams
  portable Roaring arrays, bitsets and runs without collecting deleted positions.
  It validates lengths, magic, CRC-32, ordered keys, container offsets, signed
  64-bit position bounds and exact cardinality. One bounded container directory
  and a 16-KiB input frame suffice; independent blob/bitmap caps bound work.
  Four tests cover each container family, boundaries, corruption with valid CRCs,
  descriptor count mismatch and resource caps. Snapshot-wide uniqueness, matching
  actual data-file row counts and commit/sealing integration remain pending.
  ORC probing reads at most 255 postscript bytes and validates protobuf framing,
  footer/metadata spans and optional postscript magic. Three additional tests cover
  unknown fields, legacy header magic, maximum size and malformed wire inputs.
  Avro OCF framing now pulls one encoded block at a time with independent header
  bytes, metadata entries, encoded block bytes and record-count limits. Positive
  and sized negative metadata maps, sync markers, overflow and cancelled readers
  are checked across leaf boundaries. Null and raw-deflate codecs now enforce an
  independent decoded-byte cap and reject truncated or concatenated streams.
  Writer-schema binary layouts now compile to bounded graphs with named recursive
  references. Decoded validation checks primitive widths/UTF-8, unions, enum indexes,
  exact collection byte counts and complete block consumption without retaining
  datum graphs. Independent schema-byte/node/edge and datum-depth/work limits reject
  even zero-byte recursive or huge null collections. Six layout tests pass.
  `AvroRecords` compiles the container's own schema once and validates one decoded
  block per pull; two integration tests verify corruption, bounds and cancellation.
  Root scalar projection now selects at most 64 int/long/string fields by Iceberg
  field ID, not writer names/order, and borrows strings from one decoded block.
  Nullable unions work in either branch order. Every skipped field still receives
  binary validation under the same block-wide work/depth limits; malformed IDs,
  duplicate IDs, missing selections and trailing bytes fail closed. Four cursor
  tests pass. Optional selections preserve absent values as unknown, and selected
  writer types are exposed before reading any records. Typed manifest-list pulls
  now validate canonical same-table locations, positive lengths, spec IDs,
  sequence ordering, version-dependent required/unknown counts and v3 delete/data
  row-ID separation. Four tests cover renamed/reordered fields, missing/null
  values, empty-list schema types and poisoned cursors. Partition-summary semantics,
  table spec membership and typed data-file/inheritance integration
  remain separate; list decoding does not yet prove those cross-file invariants.
  Nested scalar paths now traverse records and nullable records, with at most 64
  selections, 16 IDs per path and 16,384 compiled field visits. Shared named record
  layouts cannot expand the projection without a bound. Selected and skipped
  fields share one block-wide work/depth budget; null parents clear child slots
  without retaining prior-record values. Four nested tests pass. Array/map semantic
  projection is not implemented; their binary layout is still fully validated.
  Reader-schema resolution, logical/manifest field semantics and optional codecs
  remain separate; this does not advertise complete manifest v1/v2/v3 validation.
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

- 223 library tests pass, covering namespace, file records, range/streaming,
  credentials, JSON, format framing, Avro blocks/codecs, manifest inheritance,
  digest/writer checkpoints, staged assembly and multipart models/records.
  Focused native request authentication, pull-body and request parsing tests pass
  with Iceberg enabled and the general S3 listener feature disabled.
- Native file-tree publication, full read, a range crossing leaf boundaries and
  Chunk-KV restart pass against real ChunkDB/DiskIO using the separate
  `iceberg_file_storage_test` target. This verifies storage bytes, not Parquet
  semantics or the pending FileIO HTTP and official-client contract.
  The same native fixture now persists a multipart reservation, settles it through
  the recovery scan, freezes its selection and checkpoints seven assembled bytes.
  After Chunk-KV restart and a new chunk client, recovery completes the exact bytes
  while the file location remains unpublished. Logical abort retains that state.
  The expanded fixture passes in 35.96 seconds; Iceberg E2E-feature clippy passes.
  The fixture also starts the actual Iceberg listener and observes it settling
  and aborting an expired pending upload without client recovery calls. The first
  attempt exposed a synthetic root with no management journal; initialization now
  uses the real management repository. The expanded fixture passes in 35.92 seconds.
  It now admits the expired upload through durable global credits and observes
  the real worker settling its part, aborting, releasing credits exactly once and
  retaining its part authority. The expanded fixture passes in 37.40 seconds.
- Command: `pixi run clean-env && CROWDB_RUNTIME_ROOT="$PWD/.crowdb-runtime/ephemeral/iceberg-file-storage" pixi run -- cargo test -p crowdb-access-server --features iceberg-e2e --test iceberg_file_storage_test -- --nocapture`.

## Handover — 2026-09-23

Stop at the verified nested-projection task boundary at the user's request, so a
cheaper mode can resume. This is not requirement completion or a new blocker.
No user-guide edits, public FileIO exposure, new unsafe exceptions, locks or
physical deletion were added. Resume with the next task below, not a rewrite of
the landed storage primitives. The broader ordering is in
`plan-iceberg-functional-catalog.md`; human choices remain in R177.

### Immediate continuation

- [ ] **Typed manifest entries**: add `src/manifest/entry.rs` and
  `tests/manifest_entry_test.rs`. Reuse `AvroProjection::paths`,
  `AvroFieldPath { ids, required }`, `field_types()` and `ManifestInheritance`.
  Root IDs are status `0`, snapshot `1`, data-file record `2`, data sequence `3`,
  file sequence `4`. Nested paths include `[2, 134]` content, `[2, 100]` path,
  `[2, 101]` format, `[2, 103]` record count, `[2, 104]` byte length,
  `[2, 140]` sort order, `[2, 142]` first row ID, `[2, 143]` referenced file,
  `[2, 144]` DV offset, and `[2, 145]` DV size. Check required/null/type rules
  against each writer version before consuming data; bind paths to the expected
  native table. Keep one entry, not a growing vector. Resolve inheritance only
  after all checks for that entry pass, so a failed entry never advances row IDs.
  Do not guess semantic file kind from extension. Test v1 missing sequence/content,
  v2 added-only inheritance, v3 row IDs, malformed status/content, foreign paths,
  null required values, poison-after-error and unchanged resolver on failure.
- [ ] **Collections and manifest metadata**: scalar projection does not yet
  expose equality IDs, metrics maps, partition tuples or partition summaries.
  Extend bounded traversal only as needed; do not deserialize full datum graphs.
  Check field IDs plus array `element-id` and map `key-id`/`value-id` metadata,
  including Iceberg's logical-map array representation. Decode equality IDs and
  metrics under independent entry/work bounds, checking against the table schema.
  Validate OCF version/schema/partition-spec/content metadata; the actual manifest
  version is not necessarily the table or enclosing manifest-list version.
  Position deletes ignore sort order; do not reject solely for a non-null value.
  Files: Avro schema/projection children, manifest modules, focused fixtures.
- [ ] **End-to-end block semantics**: compose `AvroRecords::next()` with the typed
  projections over each decoded block; retain the inheritance resolver across
  blocks. `AvroRecords::schema()` borrows the reader, so drop a borrowed projection
  before the next mutable pull, or design an owned bounded compilation handle
  rather than using unsafe/self-referential state. Add native leaf-crossing OCF
  tests, null/deflate blocks, renamed/reordered fields, cancellation and corruption.
  Existing `file_avro_test.rs`, `common/file_blocks.rs` and
  `common/manifest_list.rs` are fixtures to reuse. Do not report complete manifest
  acceptance while collection or cross-file checks remain missing.
- [ ] **DV cross-file checks**: reuse `read_puffin_metadata` and
  `validate_deletion_vector`; those already verify exact descriptor reference,
  span, cardinality, portable bitmap structure, maximum position and CRC.
  Still connect manifest fields to that validator, compare maximum position with
  the referenced data-file row count, and enforce one DV per data file per
  snapshot using bounded cross-file state. Snapshot-wide validation belongs in
  commit admission, not a whole-snapshot in-memory collection in the file reader.
- [ ] **Finish other independent FileIO work**: metadata projection fallback,
  semantic seal orchestration, delegation vending, multipart HTTP composition and
  official client acceptance remain unfinished. Use the existing execution tasks
  above; the standard-PUT semantic-kind decision blocks only its dependent wiring.

### Reuse and integration boundaries

- `src/file/avro/schema/projection.rs` and `projection/compile.rs`: root or nested
  scalar cursor; required means schema presence, not a non-null runtime value.
  Missing optional paths and null parent records produce `AvroScalar::Null`.
  Consumers enforce typed required values; skipped fields still undergo binary
  validation. Malformed/duplicate IDs in traversed records fail closed. Primitive
  strings borrow the current bounded block. This is not general schema evolution
  or complete global Iceberg field-ID validation.
- `src/manifest/list.rs`: typed manifest-list cursor, canonical same-table paths,
  length/spec-ID checks, v1 zero sequences, v2/v3 required counts, v3 optional row
  IDs and delete separation. It does not verify spec membership, summaries,
  referenced file existence or snapshot-wide lineage. The list writer version is
  explicit; do not infer it from the current table version.
- `src/file/multipart_credits.rs`: durable global session/reserved-byte admission
  with a single pending CAS journal; `settle` repairs uncertain reservation or
  terminal release. `MultipartRecovery` helps that journal before scanning four
  sessions. Call admission before exposing an upload; low-level repository test
  fixtures can still be uncredited. Release only retained terminal receipts.
  These are logical active credits, not cumulative orphan/disk capacity accounting.
- `src/file/multipart_repository/` and recovery/list modules already implement
  journaled part replacement, frozen selection, resumable assembly, frozen seal
  publication/replay, bounded listing and native background recovery. Do not
  implement a second state machine in HTTP handlers. Public XML/error wiring,
  LastModified, grant/byte intersections and standard-client completion retry
  details still need work; an invalid frozen selection currently requires abort.
- Server `src/iceberg/file_upload.rs`, `file_body.rs`, `file_auth.rs` and
  `file_request.rs` provide bounded transport, SigV4 grant authentication and
  operation parsing. They are not a publicly composed FileIO service. Upload
  rejects trailers and does not yet support AWS streaming-checksum framing.
- Formats: JSON validation is structural; Parquet/ORC probes verify framing and
  fixed-size hints, not complete footer semantics. Puffin metadata is bounded
  plain JSON or one sized LZ4 frame. Avro only has null/raw-deflate codecs.
  Canonical bytes remain authority; missing/corrupt projections must fall back.
- Preserve the R179 500-ms acceptance blocker; do not increase timeouts or add
  caller retries to claim it passes. R177 also records standard PUT kind binding,
  release engine profiles and no-GC deployment capacity policy. No new human
  decision was needed for the Avro projection tasks.

### Resume verification

- Latest library gate: `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
  passes 223 tests. `pixi run rs-lint` and
  `pixi run -- cargo fmt --all -- --check` pass. These latest changes are library
  and test code only; the previously recorded native E2E run is not a new run.
- Start the next change with focused `--test avro_nested_projection_test`,
  `--test avro_projection_test`, `--test manifest_list_test`,
  `--test manifest_inheritance_test` and the new entry target, then the full library
  gate and separate lint/fmt gates. Use `pixi run` for every executable.
- For server transport changes, run
  `pixi run -- cargo test -p crowdb-access-server --no-default-features --features iceberg --test iceberg_file_upload_test --test iceberg_file_body_test --test iceberg_file_auth_test --test iceberg_file_request_test`.
  Native worker/storage changes also require the real-stack command in the
  checkpoint and `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- The declared workspace MSRV is 1.75, but the already locked `lz4_flex 0.11.6`
  and its newly enabled frame dependency `twox-hash 2.1.3` declare 1.81. Current
  Pixi toolchain gates pass; Rust 1.75 was not verified. Do not silently claim
  that older toolchain or downgrade unrelated dependencies as part of the decoder.

## Blocked

Only standard-FileIO semantic kind binding awaits a high-level decision, recorded
in R177. The backed-up table specification's Equality Delete Files section puts
usage in manifest `content`/`equality_ids`; ordinary FileIO writes only a path and
bytes. Inferring kind from `.parquet` or schema alone is unsound. A physical
storage-family record plus generation-bound usage preserves standard clients;
per-file upload intents preserve early semantic kind but require adaptation.
Continue credentials, format parsers, multipart storage and projections; do not
expose guessed kind classification or claim complete writable FileIO acceptance.
