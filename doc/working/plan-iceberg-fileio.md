# Iceberg FileIO Plan

Upstream: [immutable FileIO requirement](../backlog/R180-access-iceberg-fileio.md).

Goal: publish immutable native file identities with bounded streaming and durable
multipart, without general S3 authority or premature physical deletion.

Current integration status is maintained in
[the functional catalog plan](plan-iceberg-functional-catalog.md#current-requested-tasks-13).
Native table create/commit, selected-file proofs, draft credentials and recovery
are connected. Historical checkpoints below do not supersede that status.
R179 remains open for its recorded latency decision and complete acceptance.

## Execution

- [x] **Official SDK completion compatibility**: verify AWS Complete semantics
  and official client source before changing transport behavior. Stream the XML
  declaration and periodic whitespace while the existing durable completion
  driver runs; encode late failures inside the HTTP 200 XML body. Keep a bounded
  work deadline and cancel foreground work when the response is dropped. Replace
  the connection's absolute lifetime with an inactivity deadline so active
  responses can deliver their terminal XML. Add body cancellation/deadline tests
  and a real-stack official AWS SDK test before claiming compatibility.
  Sources: AWS `API_CompleteMultipartUpload`, Apache Iceberg `S3OutputStream`,
  and botocore's special-case HTTP 200 error handling.
  Files: server `file_complete.rs`, `body.rs`, `file_http/multipart.rs`,
  `http.rs`, connection adapter and server integration tests.
  Apache Iceberg 1.11.0 with bundled AWS SDK 2.44.4 passes the real-stack
  ordinary PUT, 6-MiB multipart, HEAD, GET, seek and late-error path. Its default
  signed checksum trailers exposed the missing streaming verifier; its escaped
  ETags exposed the XML parser's literal-quote assumption. Both are corrected
  without changing the client's checksum/chunked defaults. This is a pinned
  FileIO baseline, not full catalog or release-profile acceptance.

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
  The publication primitive requires already sealed chunk input. Native PUT and
  Complete now call the streaming seal pipeline before publishing; selected
  manifest/table-use validation remains pending.
  Standard PUT does not infer equality-delete usage from bytes. The approved
  physical-format authority retains ambiguous kind as unbound; selected metadata
  and manifest uses are validated at load and commit admission.
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
  reads, errors and dropping an in-flight response. Listener routing is connected.
  The upload adapter now independently admits at most 64 concurrent bodies, checks
  declared and actual byte ceilings, slices each received HTTP frame into at
  most 64-KiB writes and awaits storage before polling again.
  It verifies content length and optional signed SHA-256 before returning a staged
  tree; cancellation, transport/storage errors and digest mismatches never publish
  authority. Four server tests cover round-trip bytes, all failure classes,
  backpressure and credit release while retaining uncertain orphan blocks.
  Signed/unsigned AWS checksum streaming is now implemented. The listener now
  applies intersected grants and seals canonical bytes before publication.
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
  expiry, tampering, duplicate fields and byte caps. HTTP routing and streaming
  limit enforcement now use these primitives; credential vending through table
  endpoints remains unimplemented.
  Path-style request parsing now recognizes only native exact-object operations
  and multipart subresources, decodes percent escapes once and rejects duplicate
  parameters, path escape, ordinary buckets and file DELETE. Four parser tests
  pass; listener and durable multipart driver composition are connected.
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
  Part mutation now persists `modified_ms` in the FlatBuffers part record. The
  repository stamps it from the accepted mutation time, including replacement;
  lost-write replay retains that value. List decoding rejects zero or out-of-session
  timestamps before XML serialization. Existing pre-field part records decode with
  a zero timestamp and fail closed; there is no public multipart endpoint or
  deployed compatibility promise for those experimental records.
  Global admission now persists independent session/byte limits and one bounded
  CAS journal. A session reserves its staged-byte ceiling before authority creation;
  only terminal sessions release it, retaining a policy/sequence-bound receipt.
  Recovery helps a pending precreation journal before scanning and later returns
  terminal credits, without deleting parts or introducing process-local locks.
  Nine model/record/driver tests cover separate limits, every create/release lost
  write, concurrent admission, policy mismatch, duplicate release and stale helpers.
  Public HTTP admission/configuration remains to be connected. Capacity of retained
  physical orphans remains the separate R177 trial-policy decision.
- [x] **Multipart response encoding**: the server now has bounded S3-shaped
  Create/List/Complete success XML and typed error XML; UploadPart returns a
  quoted SHA-256 ETag header, ListParts emits the same ETag, LastModified from
  persisted part time, exact requested MaxParts and continuation marker. Complete
  requires a Published session, matching FileRecord and a caller-supplied HTTP
  object URL. XML text is escaped; abort returns 204. Six server tests cover XML
  escaping, pagination, dates, errors and incomplete publication. Files: server
  `file_response.rs`, library multipart model/codec/repository/list, protocol schema,
  library/server tests. HTTP dispatch/official client use still await composition.
- [x] **Grant and byte intersection**: `FileTransferAdmission` checks a verified
  grant's operation, table, principal, upload ID, validity window and active
  multipart credit against configured service and durable session ceilings.
  `check_create` preflights the global policy and session budget before the caller
  performs `MultipartAdmission::reserve`; `receive` and `read_body` enforce
  intersected declared and actual stream/range bytes before publication or read.
  Three focused server tests cover each limit, foreign scope, missing/released
  credit, expired grant, upload backpressure and bounded range reads. Files:
  server `file_admission.rs` and server tests. Public listener routing and
  credit-reservation wiring still await complete HTTP composition.
- [x] **Projections**: generation-local bounded derived JSON pages and canonical
  fallback on every invalid projection. Files: metadata projection modules/tests.
  `ProjectionStore::put` derives raw top-level JSON children from already sealed
  canonical bytes; SHA-256 must match the authoritative FileRecord. Optional
  construction is capped at 2 MiB, 64 children and 1024-byte field names. Larger
  metadata remains readable through the ordinary bounded canonical stream.
  Scope 14 keys bind catalog/table, generation, JSON digest, projection version,
  child and page. A checksummed root (at most 32 KiB) describes deterministic
  children; their exact JSON bytes occupy immutable pages of at most 32 KiB.
  Child digests and exact page sizes are checked before selected bytes escape.
  Children publish before the root; failures return false and cannot gate file
  publication. Lost-write retries converge through immutable compare-exchange.
  `select` returns bounded selected child bytes only after all requested children
  verify; absent, corrupt, wrong-identity/version, oversized or unavailable required
  projection records return a fresh canonical FileReader. An empty selection
  always streams the byte-identical complete file. Invalid canonical records and
  corruption encountered during fallback remain errors. Hits do not probe unused
  canonical blocks; unrequested children are not read.
  Ten focused tests cover multi-page values, exact whitespace, no canonical block
  reads on hits, identity/version bounds, every missing/corrupt page, unavailable
  storage, write-loss replay, oversized inputs/records and malformed keys.
  Load/commit integration still belongs to R181/R182: callers must supply the
  selected generation's authenticated FileRecord and consume fallback streams.
  This is not a whole-file materialization path or full metadata semantic validator.
- [ ] **Format validation**: bounded Avro blocks, v1/v2/v3 inheritance and row IDs,
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
  table spec membership and complete collection/cross-file semantics
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
  Typed scalar entry projection now connects Avro decoding to that resolver and
  preserves its state across blocks. Required values, path scope, format/content,
  DV descriptor bounds and row-ID overflow fail before advancing the failed entry.
  Five entry tests and two chunk-backed null/deflate block integration tests pass.
  Partition/equality-ID/metrics semantics, canonical DV reference verification and
  table commit admission remain pending; this is not full manifest validation.
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

- The earlier 230-test library checkpoint covered namespace, file records, range/streaming,
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

The initial handover boundary was typed scalar manifest-entry decoding plus
cross-block inheritance. Subsequent work added bounded equality-ID list decoding,
schema element-ID checks, typed OCF manifest metadata, bounded metric maps,
historical context, typed partition/bound semantics and a bound streaming reader. This is not requirement
completion or a new blocker.
No user-guide edits, public FileIO exposure, new unsafe exceptions, locks or
physical deletion were added. Resume with the next task below, not a rewrite of
the landed storage primitives. The broader ordering is in
`plan-iceberg-functional-catalog.md`; human choices remain in R177.

### Immediate continuation

- [x] **Typed scalar manifest entries**: implemented `src/manifest/entry.rs`,
  `entry/decode.rs` and `tests/manifest_entry_test.rs`, reusing `AvroProjection::paths`,
  `AvroFieldPath { ids, required }`, `field_types()` and `ManifestInheritance`.
  `ManifestEntryState` owns inheritance across blocks and rejects a mismatched
  projection version/table. Root IDs are status `0`, snapshot `1`, data-file record `2`, data sequence `3`,
  file sequence `4`. Nested paths include `[2, 134]` content, `[2, 100]` path,
  `[2, 101]` format, `[2, 103]` record count, `[2, 104]` byte length,
  `[2, 140]` sort order, `[2, 142]` first row ID, `[2, 143]` referenced file,
  `[2, 144]` DV offset, and `[2, 145]` DV size. The v1 deprecated block-size field
  `[2, 105]` is also required. Types are checked at compile time and required values
  at pull time. Paths bind to the native table; no extension-based kind guessing.
  Scalar/binary failures and inheritance errors never advance that entry's row IDs.
  Position deletes ignore sort order. Puffin entries require v3 position-delete
  content, a referenced file and an in-file offset/size pair. Full equality-delete,
  partition and metric semantics are explicitly not asserted by `ManifestScalarEntry`.
  When adding those checks, perform them before `inheritance.resolve`, not after
  yielding the entry. Five tests cover versions, malformed values, null records,
  explicit versus inherited row IDs, overflow, descriptors and poisoned cursors.
- [x] **Bounded equality-ID lists**: array projection now retains `element-id`
  and exposes a validated encoded integer list. The manifest entry projection
  requires element ID 136 when field 135 exists. Equality deletes require a
  nonempty list of at most 4096 positive, unique IDs; other content rejects a
  non-null list. Positive and sized negative Avro blocks, over-limit lists,
  wrong schema IDs and inheritance-safe failures have focused tests. Membership
  in the table schema and presence in the delete file still need table/file
  context; this is partial collection validation.
- [x] **Typed manifest writer metadata**: `ManifestMetadata::parse` reads bounded
  OCF properties, derives the writer's v1/v2/v3 version and data/delete content,
  requires version-specific schema/spec IDs and checks the bounded schema and
  partition-spec JSON roots. It rejects mismatched schema IDs. The existing
  chunk-backed, two-block stream fixture now carries and parses real OCF manifest
  properties before constructing inheritance state. Table schema/spec membership,
  nested JSON semantics and full list-to-manifest consistency remain.
- [x] **List/header inheritance context**: `ManifestEntryState::from_list` checks
  same-table location, content and available partition-spec ID before using the
  list's snapshot, sequence and row-ID sources. A v1 manifest can still use a
  newer enclosing list; missing optional v1 spec ID cannot be compared. Exact
  list location/length to opened file identity and table schema/spec membership
  remain for full cross-file validation.
- [x] **Bounded metric maps**: integer-keyed Avro logical maps now require exact
  key/value field IDs and non-null integer keys with long/bytes values. Selected
  values borrow validated block bytes; `AvroMetricMap::visit` independently caps
  items and encoded bytes. `ManifestMetrics` owns at most 4096 entries and 1 MiB
  of value payload across all six maps per entry. Duplicate/nonpositive keys,
  negative counts, null-plus-NaN count overflow/excess, malformed block framing
  and wrong types/IDs fail before inheritance. Nested counts may exceed file row
  count. Null and empty maps remain distinct. Six new tests and expanded
  null/deflate, cross-leaf/block fixtures pass. No locks or unsafe were added.
  Numeric interpretation of binary bounds and schema membership remain dependent
  on typed table context; these maps alone do not establish full metric semantics.
- [x] **Historical schema/spec context**: `ManifestContext` indexes nested IDs,
  required ancestry and collection ancestry, validates primitive parameters and
  version gates, and derives partition transform result types. Limits: 1 MiB JSON,
  32 nesting levels, 4096 fields and 256 partition fields. v1 missing partition IDs
  use sequential IDs from 1000. Header definitions bind to trusted historical
  schema/spec definitions, not current table IDs. Optional trusted schema history
  retains dropped metric/equality columns under independent history/work/byte caps.
- [x] **Partition tuples**: `AvroTuple` retains writer logical annotations and
  validates required parent records, exact tuple IDs and bounded values.
  `ManifestEntryProjection::with_context` checks logical types, decimal fixed
  precision/scale, timestamp zone/precision, nulls, bucket/truncate domains and void.
  Unknown transforms preserve bounded values without asserting filtering semantics.
  Tuple failures precede inheritance, including null unpartitioned records.
- [x] **Typed metrics/equality semantics**: the contextual projection validates
  retained column IDs, NaN applicability and equality-field eligibility, including
  collection ancestry. Bounds check encodings and ordering for scalar types and
  geospatial points, including numeric promotions, signed decimals, signed zero,
  UTF-8 and geography dateline wrapping. Position-delete reserved columns are
  recognized. Variant bounds now use the bounded object decoder below.
- [x] **Partition summaries (task 3)**: decode bounded field-summary arrays by
  field ID, bind ordering/types to the historical partition spec, and check
  summary flags and bounds against streamed entries before reader completion.
  Cover null/NaN, signed zero, malformed layouts, limits and poisoned cursors.
  `ManifestListEntry::partitions` preserves absent/null/empty arrays. Each list
  record admits at most 256 summaries and 1 MiB encoded summary bytes.
  `ManifestReader` checks bound containment for all entry statuses and exact
  null/known-NaN flags at EOF; unknown transforms retain bounds without using
  them for filtering. Four new tests plus the full library gate pass (277 tests).
- [x] **Variant bounds (task 4)**: validate bounded concatenated Variant metadata
  and primitive-valued bounds objects, normalized paths, paired types and order.
  `entry/variant.rs` and its `primitive`/`path` children accept metadata v1,
  all offset widths, unordered value storage and optional one-sided paths.
  Limits are 1 MiB encoded bytes, 4096 dictionary/object entries, 4096 path bytes
  and 32 path segments. Same logical types compare exactly, including integer/
  decimal encodings and micro/nanosecond timestamps; float/double and timestamp
  zones remain distinct. Null/NaN bounds, nested object/array values, malformed
  offsets, duplicates and unsupported type IDs fail before inheritance advances.
  Five focused tests and the full library gate pass (282 tests).
  Encoding reference: [Parquet Variant](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md);
  path reference: [RFC 9535 normalized paths](https://www.rfc-editor.org/rfc/rfc9535.html#section-2.7).
- [ ] **Remaining format semantics**: full default-value validation,
  encryption key metadata and split offsets remain.
  Actual data/delete-file field presence and true bounds against data require
  format/file context. No complete manifest/seal acceptance is claimed here.
- [x] **Scalar block integration**: `manifest_entry_stream_test.rs` composes
  `AvroRecords::next()` with a projection per decoded block and shared
  `ManifestEntryState`. Tests cross 64-byte stored leaves and Avro block boundaries
  for v1/v2/v3 with both null and raw-deflate codecs; a binary-valid but semantically
  invalid later block leaves prior row-ID progress unchanged. Drop the borrowed
  projection before the next mutable reader pull; no unsafe/self-referential state
  is needed. `common/manifest_entry.rs` provides typed schema and OCF datum fixtures.
  These use the test block store, not a new real-ChunkDB process acceptance run.
- [x] **Bound manifest reader**: `ManifestReader::open` binds exact native location,
  length, kind/format, parsed header and trusted schema/spec context. `next_entry`
  retains one decoded block, checks added/existing/deleted counts and rows, live
  minimum sequence and sequence ceilings. EOF is required before `is_complete`.
  Cancellation/errors poison the reader; reopening canonical bytes starts fresh.
  Candidate inheritance is installed only after semantic and list-total checks.
  Tests cover v1/v2/v3, null/deflate, 64-byte leaves, multiple records per block,
  multiple blocks, bad later entries, wrong identity/spec, totals and cancellation.
  These are chunk-backed library tests, not new real-server or client E2E acceptance.
- [x] **DV cross-file validator (task 5)**: `SnapshotDvValidator` connects live
  manifest entries and immutable FileRecords to canonical Puffin descriptor and
  bitmap verification. It checks exact reference/span/cardinality, sequence and
  partition applicability, and maximum position strictly below data record count.
  Partition equality normalizes NaNs, preserves signed zero, and handles numeric
  precision promotion. Inputs are sorted by referenced canonical key; one previous
  key detects duplicate/out-of-order targets even across different Puffin files.
  Catalog epoch, table, snapshot, sequence and manifest-list FileId bind the scope.
  Count/aggregate-byte/per-blob/per-bitmap limits are independent. Early finish,
  corruption, errors and cancellation cannot return success or advance progress.
  Eight new tests include contextual manifest decoding, chunk-backed canonical
  Puffin reads, multiple blobs, cancellation and boundary failures.
- [ ] **Snapshot admission integration**: the future commit enumerator must
  exhaust all candidate manifest readers, produce every live DV/data pair in
  referenced-key order under bounded external storage, and supply the trusted
  exact total to `SnapshotDvValidator`. Recheck catalog/head fencing before CAS.
  Do not derive that total from an untrusted summary or count all position-delete
  entries as DVs. Merge/replace prior position deletes and verify actual data-file
  row counts through format context. These are R182 composition work, not a
  whole-snapshot in-memory collection or a second file-reader state machine.
- [ ] **Finish other independent FileIO work**: metadata projection load/commit
  integration, selected-use validation, delegation vending and official client
  acceptance remain unfinished. Use the existing execution tasks above.

### Reuse and integration boundaries

#### Handover after contextual manifest validation

- The four approved slices now have a contextual library path: build the trusted
  `ManifestContext` from the corresponding table schema/spec, optionally attach
  bounded trusted schema history, then use `ManifestReader::open`. Do not use the
  legacy `ManifestEntryProjection::new` as full semantic acceptance; it remains
  the deliberately partial scalar/collection API for existing callers.
- Reader completion verifies this pipeline and list totals, not whole-snapshot
  correctness or content-file truth. Callers must exhaust the reader and handle
  final EOF errors. Unknown transform values are retained for reads; write
  admission must reject unknown transforms. Variant bounds now decode bounded
  primitive-valued objects; actual bound truth still requires data-file context.
- Full delete-file column presence and actual bound correctness require file
  context, not only manifest schema. The sorted DV validator is implemented;
  snapshot enumeration, prior-delete replacement and admission still belong in
  commit processing and remain complex work.
- Multipart response formatting, durable part LastModified and grant/service
  limit intersection are implemented as bounded server/library components.
  Metadata projection storage/fallback is also implemented; table-load wiring
  remains pending with table heads. None requires replacing the metric decoder.

#### Handover after metadata projection fallback

- `src/metadata_projection/{model,repository}.rs` implements the optional derived
  store, independently of canonical publication. Use `put` only on bounded,
  already sealed canonical input; false must never reject publication. No public
  HTTP route or table load has been wired, and R180 remains unfinished.
- `MetadataRead::Selected` contains exact raw JSON values for requested top-level
  fields (including object/array children). `MetadataRead::Canonical` contains a
  boxed streaming reader of the whole original JSON; callers must choose their
  bounded parse/stream behavior, not reinterpret it as selected-field bytes.
- No eviction or physical deletion was added. Generation-local derived pages may
  leak until R183 just like other unreachable staged data. Keys never cross the
  catalog/table/generation/digest/version boundary, and no new lock or unsafe
  exception is needed. Admission/fencing still belongs to the calling catalog
  operation; this optional store is not an alternate authority.

#### Handover after multipart responses and limit intersection

- `MultipartRepository::reserve_part` stamps `modified_ms` from its accepted
  `now_ms` before persisting the pending mutation. The FlatBuffers field is
  append-only; committed parts with zero/invalid time fail closed. A replacement
  gets its own LastModified, and recovery preserves the original pending value.
- `MultipartResponses` formats Create, UploadPart, ListParts, Complete, Abort and
  typed S3 errors. UploadPart and ListParts share quoted SHA-256 ETags; Complete
  requires the exact Published session and selected FileRecord and receives a
  trusted public HTTP object URL from its caller. XML escaping and stable marker
  semantics are covered by focused server tests. Request XML parsing and public
  dispatch are now connected as described in the checkpoint below.
- `FileTransferAdmission::authorize` consumes a verified grant and parsed native
  request, checks table/operation, principal/upload/session credit/expiry, and
  intersects service/session byte ceilings. `check_create` is a preflight against
  the durable global policy; it does not reserve credits. The caller must invoke
  `MultipartAdmission::reserve` before exposing an upload ID. `receive` and
  `read_body` call the existing bounded adapters with the intersected ceilings.
  Before dispatch, validate Ready catalog context and authenticate SigV4 using
  `authenticate_file_request`; afterward, reload the current session/policy and
  use only these checked transfer paths. The native listener now applies them.

#### Checkpoint after SDK-shaped FileIO routes and sealing

- Native path-style S3 requests now enter the Iceberg listener separately from
  general S3 and bearer catalog routes. Signed PUT, HEAD, GET, Range, create,
  upload-part, list-parts, Complete and abort use the existing grant, durable
  multipart, immutable publication and recovery components. Unsupported bucket
  and object operations remain unavailable. Complete XML parsing is bounded,
  checks the S3 namespace and ascending part numbers, resolves current durable
  revisions and enforces the 5-MiB nonfinal part rule.
- PUT and Complete seal complete canonical bytes before publication. Container
  magic and full format validation choose JSON metadata or an unbound Avro,
  Parquet, ORC or Puffin record; neither filenames nor upload headers classify
  data versus delete use. `FileKind::Unbound` is a storage authority, not a
  declared Iceberg use. Selected metadata/manifest use must be checked in
  R181/R182 before table head publication.
- Native chunk small writes accept at most the protocol frame payload, 65,502
  bytes. Upload and multipart assembly/recovery use the same block size. The
  HTTP adapter slices arbitrary received frames into bounded writer calls; the
  listener's Hyper buffer setting is not a hard body-frame ceiling.
- A real-stack manual SigV4 test passed ordinary PUT/HEAD/GET/Range and a
  5-MiB-plus multipart upload, durable ListParts, Complete replay and GET. It
  took 136 seconds, so the current 300-second Complete/connection deadline is
  not yet sufficient evidence for large-file official-client acceptance.
  The pinned S3FileIO baseline and streaming checksum support now pass as detailed
  below. Table-side delegated credential vending and the wider client/engine
  matrix remain pending. Do not advertise full FileIO.

#### Checkpoint after official Java FileIO compatibility

- `FileCompleteBody` sends the XML declaration first, then 10-second whitespace
  heartbeats, then exactly one success or error document. A 300-second work
  deadline yields a terminal `SlowDown` document; disconnect drops foreground
  work, leaving durable recovery in charge. Connections expire after 300 seconds
  without successful I/O, not after a fixed total lifetime. Shutdown draining
  remains bounded to 300 seconds. These are resource limits, not evidence of
  acceptance for arbitrarily large objects.
- `SigV4Verifier::verify_streaming` explicitly opts native uploads into AWS
  streaming seed verification. The ordinary verifier still rejects streaming.
  `FileUploadBody` verifies every signed chunk, the zero-length terminal chunk,
  declared trailing checksum and trailer signature. Unsigned trailer uploads
  and signed uploads without trailers also have focused tests. CRC32, CRC32C,
  CRC64NVME, SHA-1 and SHA-256 are supported; unknown checksum algorithms fail
  closed. Encoded bytes and decoded lengths are bounded separately; parser lines
  are capped at 1 KiB and output frames at 64 KiB. Only successful EOF can return
  a publishable upload tree. Trailers carried as arbitrary HTTP trailer frames
  remain rejected; AWS trailers are decoded from the aws-chunked payload.
- AWS's published signed CRC32C trailer example verifies independently of our
  test signer. Negative cases cover changed seeds/chunks/checksums/signatures,
  extra bytes, truncation, duplicate framing headers, frame splits and encoded
  byte limits. Complete XML accepts predefined and numeric references in ETags
  under the same 66-byte decoded bound and still rejects external entities.
- Reproduce the official client test with
  `pixi run -e iceberg-e2e test-java-iceberg-fileio-e2e`. The environment pins
  Java 21 and Maven 3.9 through `pixi.lock`; the fixture pins Iceberg 1.11.0 and
  its AWS bundle. Credentials enter the test process through stdin, not command
  arguments. Successful execution took 177.74 seconds including stack startup
  and Maven cleanup. Maven reports the official SDK's remaining daemon threads
  during in-process cleanup; the Maven process exits successfully.
- Remaining scope: catalog credential vending, selected-use validation, table
  heads/load/lifecycle, candidate snapshot admission and commits. Multipart
  additional-checksum persistence/Complete fields and the wider release-client
  matrix are not covered by this baseline. The existing immutable digest remains
  SHA-256; checksum verification does not redefine ETags or file authority.
- Verification passed: affected S3/server `--all-targets` tests with the Iceberg
  feature, the pinned Java real-stack test, manual SigV4 PUT/Range/multipart and
  Complete replay (137.15 seconds), workspace fmt and `pixi run rs-lint`, and
  explicit Iceberg-E2E-feature clippy. The no-default-feature encoding tests also
  pass. No new unsafe scope, lock or physical deletion path was introduced.
- Primary references:
  [AWS Complete](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html),
  [AWS signed chunks](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html),
  [AWS signed trailers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html),
  [Iceberg S3OutputStream](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/aws/src/main/java/org/apache/iceberg/aws/s3/S3OutputStream.java).

#### Handover after partition summaries, Variant bounds and DV binding

- Summaries are decoded by `AvroRecordArray` and `ManifestListProjection` and
  validated against historical spec order/types by `ManifestReader`. Exhaust to
  EOF before treating totals or null/NaN flags as verified. Unknown transforms
  retain bounded values but do not supply filtering semantics.
- Contextual entry projections now validate Variant bound objects automatically.
  They retain canonical bytes in metrics and do not expose a general-purpose
  Variant document materializer. Bound accuracy against actual data remains a
  separate format-level check.
- Use `SnapshotFile { entry, record, context }` only for fully validated live
  entries and canonical records loaded from the selected snapshot. Construct one
  `SnapshotDvValidator` per frozen `SnapshotDvScope`, pass matching scope on each
  `check`, and require `finish`. Sort by native relative-key UTF-8 byte order.
  The validator keeps one key, counters, one bounded footer and bitmap window;
  its success covers supplied pairs, not enumeration completeness or publication.
  Caller-provided row counts currently come from manifests, not Parquet/ORC
  footer decoding. No public seal/commit endpoint is enabled by these helpers.
- Changed modules: `src/file/avro/schema/{record_array,projection}.rs`,
  `src/manifest/{list,summary,reader,deletion_vectors}.rs`,
  `src/manifest/deletion_vectors/binding.rs`, and
  `src/manifest/entry/variant.rs` with `variant/{path,primitive}.rs`.
  Focused tests: `manifest_summary_test`, `manifest_reader_test`,
  `manifest_variant_test`, `manifest_dv_test`, `manifest_dv_partition_test`,
  `deletion_vector_test` and `puffin_metadata_test`.
- Complete XML parsing, HTTP composition and physical seal orchestration are
  now connected. Next complex slices are selected-use validation, candidate
  snapshot enumeration/admission, selected table heads and atomic commits.
  Credential vending and official-client acceptance remain separate; R179's
  latency decision remains recorded in R177.

- `src/file/avro/schema/projection.rs` and `projection/compile.rs`: root or nested
  scalar cursor; required means schema presence, not a non-null runtime value.
  Missing optional paths and null parent records produce `AvroScalar::Null`.
  Consumers enforce typed required values; skipped fields still undergo binary
  validation. Malformed/duplicate IDs in traversed records fail closed. Primitive
  strings borrow the current bounded block. This is not general schema evolution
  or complete global Iceberg field-ID validation.
- `src/manifest/list.rs`: typed manifest-list cursor, canonical same-table paths,
  length/spec-ID checks, v1 zero sequences, v2/v3 required counts, v3 optional row
  IDs, bounded summaries and delete separation. Trusted context and the bound
  reader validate summary types and actual partitions. Referenced file existence
  and snapshot-wide lineage remain separate. The list writer version is
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
  implement a second state machine in HTTP handlers. New response formatters and
  intersected admission helpers, public routes and physical sealing are connected.
  Selected-use validation and standard-client compatibility still need work;
  an invalid frozen selection currently requires abort.
- Server `src/iceberg/file_upload.rs`, `file_body.rs`, `file_auth.rs`,
  `file_request.rs`, `file_response.rs` and `file_admission.rs` provide bounded
  transport, SigV4 grant authentication, operation parsing, response formatting
  and limit checks. They are composed in the native FileIO listener. Upload
  rejects arbitrary HTTP trailer frames; `FileUploadBody` validates AWS checksum
  trailers inside the signed/unsigned aws-chunked payload before publication.
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
  passes 290 tests (including partition-summary, Variant and DV-boundary tests). Protocol
  `--all-targets` passes after the schema addition. Fmt, workspace lint, and
  Iceberg-feature clippy pass.
- Server compatibility gates also pass: default `--all-targets` (2 tests) and
  `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`
  (30 tests). The no-default-feature response/admission tests pass (6 tests).
  Real-stack `iceberg-e2e` file storage passes in an isolated runtime root;
  the default persistent runtime already holds unrelated RPC port claims.
- Projection changes start with `--test metadata_projection_test`; format changes
  start with focused `--test avro_nested_projection_test`,
  `--test avro_projection_test`, `--test manifest_list_test`,
  `--test manifest_inheritance_test`, `--test manifest_entry_test` and
  `--test manifest_entry_stream_test`, `--test manifest_context_test`,
  `--test manifest_semantic_test` and `--test manifest_reader_test`, then the full library
  gate and separate lint/fmt gates. Use `pixi run` for every executable.
- For server transport changes, run
  `pixi run -- cargo test -p crowdb-access-server --no-default-features --features iceberg --test iceberg_file_upload_test --test iceberg_file_body_test --test iceberg_file_auth_test --test iceberg_file_request_test`.
  Native worker/storage changes also require the real-stack command in the
  checkpoint and `pixi run -- cargo clippy -p crowdb-access-server --features iceberg-e2e --all-targets -- -D warnings`.
- The declared workspace MSRV is 1.75, but the already locked `lz4_flex 0.11.6`
  and its newly enabled frame dependency `twox-hash 2.1.3` declare 1.81. Current
  Pixi toolchain gates pass; Rust 1.75 was not verified. Do not silently claim
  that older toolchain or downgrade unrelated dependencies as part of the decoder.
