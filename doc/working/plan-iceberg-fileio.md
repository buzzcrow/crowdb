# Iceberg FileIO Plan

Upstream: [R180](../backlog/R180-access-iceberg-fileio.md).
Current integration: [functional catalog plan](plan-iceberg-functional-catalog.md).

Goal: close immutable native FileIO acceptance with bounded work and no physical GC.

## Completed summary

- Canonical identities, immutable publication/replay, bounded inline/chunk storage,
  streaming PUT/HEAD/range GET and fixed-size canonical-format probes.
- Durable multipart admission, part replacement, ordered completion, checkpointed
  assembly/sealing, list/abort/expiry, recovery and exact XML/ETag/LastModified.
- Native SigV4 credentials, streamed checksum trailers, intersected grant/session/
  service budgets and cancellation. Absolute connection lifetime includes streamed
  responses; network activity cannot extend persisted clear-safety bounds.
- Bounded manifest lists/entries and inheritance, contextual collections/metrics,
  partition summaries, Variant bounds, Parquet selected-file checks, Puffin/DVs,
  snapshot provenance and generation-bound commit validation.
- SDK-compatible ordinary S3 uploads remain semantically unbound where ambiguous;
  selected references validate FileKind. No custom kind header is required.
- Native Java S3FileIO and actual Parquet catalog commits/restarts pass. Production
  table/draft credential vending is connected. REFS loads now use optional validated
  generation-local projections; ALL and commit validation retain canonical parsing.
- Official Java uploads data and equality deletes with the same S3FileIO output
  calls under neutral `objects/*.parquet` paths, then selects both successfully.
  Reusing the equality file as data, position deletes or wrong equality IDs returns
  the SDK's server-side BadRequest exception and preserves the exact metadata head.

## Remaining execution

- [x] **Projection integration**: connect optional generation-local construction
  to committed metadata and selected loads. Validate table/generation/digest/
  projection version; partial publication cannot block canonical reads or commits.
  Test equivalent validation and byte-identical fallback, not merely a cache hit.
  Files: `src/metadata_projection/`, `src/table/load.rs`, commit integration.
  - Build disposable pages lazily only after a committed head's canonical metadata
    passes the existing parser. A separate versioned validation receipt binds the
    immutable selection and exact parser limits; generic JSON projection writes
    cannot authorize skipping semantic validation.
  - REFS loads may reuse validated pages without decoding the full metadata object.
    Still stream and hash canonical bytes, check current head/namespace afterward,
    and fall back to the original parser for missing/corrupt/foreign receipts or
    pages. ALL responses preserve exact canonical bytes. Commit validation never
    consumes these receipts. Do not add cross-generation caches or claim measured
    performance improvement; those remain deferred.
- [x] **Selected-use coverage**: partition-statistics schema/rows/inventory,
  retained history and SDK publication pass with R182. Ordinary rewrite semantics
  follow the confirmed engine boundary; file, sequence, partition, position and DV
  validation remain enforced. Independent work/byte/entry limits pass.
- [x] **Cross-instance recovery acceptance**: map every R180 multipart/publication
  crash and lost-response case to library or real-stack evidence; add missing
  two-listener native cases. Verify same-location equal/different writes, frozen
  completion recovery, abort/expiry and retained orphan evidence.
  Files: multipart/repository/recovery tests, native FileIO fixture.
  Native PUT/Complete process-kill acceptance passes all 44 before/after boundaries
  with the final completion path and asynchronous background-credit assertions.
- [x] **Credential lifecycle acceptance**: native two-listener refresh, genuine
  signed short-lived grant expiry, read-only rejection, rename/drop/recreation,
  exact table-prefix isolation and maintenance fencing pass. Ordinary vending
  retains its configured lifetime; no test clock or token forgery is used.
  Files: server credential/auth tests, Java/native fixtures.
- [x] **Official FileIO matrix**: data/equality-delete selected-use E2E passes;
  unsupported S3 operations, immutable replay/conflict and table-prefix isolation
  also pass through the actual SDK. The final three-fixture SDK batch, raw signed
  requests and native Chunk-KV restart pass with the completed implementation. Cover files uploaded
  through identical ordinary S3 operations and rejected wrong selected uses.
  Confirm unsupported operations, path escapes, trailers, immutable conflicts and
  independent byte/count/concurrency budgets across the enabled SDK profile.
  Files: `iceberg_file_http_test.rs`, SDK fixtures, admission tests.
- [~] **Close R180**: run all acceptance cases, focused/full tests and gates;
  update in-scope design and remove requirement/index/plan only when complete.

## Current diagnostics

- The raw signed multipart test retains its existing 10-second request bound and
  5-MiB first part. It failed while reading Complete's response after the absolute
  connection deadline; no timeout, payload size or caller retry was changed.
- Captured native IO shows two avoidable costs: foreground/background completion
  windows disagreed, and a 64-KiB checkpoint split each 65,502-byte native block,
  producing an extra 34-byte write. Both paths now share a block-aligned window
  below the existing 1-MiB assembly ceiling. Background per-step byte work changes
  from 64 KiB to 1,048,032 bytes; its session/page deadline and single-step limit
  remain unchanged. This is not a claim that background work stayed identical.
- Chunked JSON sealing now relies on its existing full validating reader's digest
  check rather than rereading the entire object first. Corrupt blocks, wrong full
  digests and malformed JSON remain rejected; focused tests assert one storage pass.
- A sequential reader retains only its current verified leaf-directory page,
  avoiding its repeated storage read for every child. It retains at most one
  additional bounded directory, never prefetches, and still validates new pages,
  each leaf and the complete digest. Multi-level/read-count tests pass.
- A fixed repeat batch exposed remaining foreground/recovery duplication despite
  one passing raw request run. Runtime recovery now observes at most four session
  revisions on one tick and rechecks that page on the next, copying only unchanged
  completion revisions. Progressing foreground work is deferred; expiry, journal
  settlement, publication and CAS checks stay authoritative. Observations are
  bounded, disposable and never locks or leases. Verify active-progress deferral,
  unchanged-session recovery, pagination, expiry and retired contexts separately.
- Phase diagnostics after these changes measured approximately 6.07 seconds of
  sequential assembly plus 3.51 seconds of sealing for the unchanged 5-MiB fixture;
  this explained the remaining sensitivity to its 10-second bound. Assembly now
  overlaps exactly one next 16-KiB frame read with the current writer push inside
  the same bounded step. It spawns no tasks, returns no checkpoint on either IO
  error, and drops the other future on failure. Deterministic rendezvous tests
  verify overlap, byte order and read/write failure recovery. No timeout changes.
- Final raw signed PUT/range/multipart regression passed a fixed three-run native
  batch with its original 10-second bound and 5-MiB first part. Full library/server
  gates, two-listener fault/lifecycle acceptance and all three official Java native
  fixtures pass against the final implementation.
- The new native fault matrix initially asserted synchronous credit release at
  Complete response time. A kill after the release-journal CAS exposed that invalid
  test assumption: published-file replay is immediate, while the documented credit
  cleanup may finish in background recovery. The fixture now observes recovery
  through read-only loads within five seconds, retains all identity/publication
  assertions, and additionally requires no pending journal and zero session/byte
  credits. It sends no extra Complete request to drive cleanup. No production
  cleanup contract or request timeout was changed for this correction.
- Broad throughput, cache architecture and engine benchmarking stay deferred.
  These fixes address captured repeated work, not a latency-guarantee adjustment.

## Constraints and reuse

- Reuse `FileRepository`, streaming readers/writers, `MultipartRecovery` and
  existing durable journals; do not add a second HTTP-owned publication path.
- Persisted file authority, not hints or projections, controls selection.
  Incomplete EOF, corruption, cancellation and uncertain storage cannot yield proof.
- ORC selected semantics belong to deferred R186; encrypted data remains unsupported.
- Physical cleanup belongs to deferred R183. Multipart credits bound active work,
  not total retained storage. Capacity uses existing provisioned disks and chunk
  allocation failure; R183 owns full-capacity failure/recovery acceptance. No
  separate Iceberg quota or pre-full write-stop threshold is required.
- Native table admission and credential wiring are already implemented; old
  “future enumerator” and “vending disconnected” handovers were removed.
- Workspace declares Rust 1.75, but locked LZ4 frame dependencies have a higher
  MSRV; only the Pixi toolchain was verified. Do not claim Rust 1.75 acceptance.

## Verification

Acceptance-to-evidence audit:

- Inline/compression boundaries and mandatory chunk kinds: `file_record_test`,
  `file_seal_test`, `file_stream_test`, native file-storage restart fixture.
- Bounded GET/ranges/backpressure: `file_stream_test`, `iceberg_file_body_test`,
  `iceberg_file_upload_test`, raw signed HTTP and actual S3FileIO seek/range reads.
- Cross-instance immutable publication and response loss: `file_repository_test`,
  `multipart_publication_test`, native PUT/Complete listener process-kill matrix.
- Multipart duplicate/replaced parts, abort/TTL and independent budgets:
  `multipart_repository_test`, `multipart_completion_test`, `multipart_recovery_test`,
  `multipart_recovery_budget_test`, `multipart_observed_recovery_test`, admission
  and credit-journal tests. Physical deletion is never implied by expiry.
- Valid/partial/corrupt/foreign projections and equivalent selected loads:
  `metadata_projection_test`, `table_projection_test`, existing conditional-load
  head/namespace race tests. Commit proofs still parse canonical metadata directly.
- Bounded v1/v2/v3 Avro inheritance, lineage and delete semantics: manifest-list,
  entry-stream, inheritance, collection/metrics, Variant, DV and snapshot suites;
  selected Parquet, position/equality-delete and partition-statistics suites.
- Official operation restrictions and identical data/delete S3 uploads:
  `TestIcebergFileOperations`, `TestIcebergSelectedFiles`, raw route/signature/
  trailer tests; native credential refresh/expiry/rename/drop/clear fixture.
- Current gates pass: 616 library tests, 70 Iceberg-enabled server tests, default
  server all-targets, 14 no-default transport tests, fmt, workspace lint and explicit
  Iceberg-E2E all-target clippy. Final native/Java and Chunk-KV restart batches pass;
  the fixed additional native fault-matrix repeat also passes all 44 scenarios.

- Focus changed format/manifest/projection/multipart tests first, then
  `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- Server: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`.
- Native storage and Java environment commands are centralized in the
  [functional verification section](plan-iceberg-functional-catalog.md#verification-and-execution-notes).
- No-default transport regression:
  `pixi run -- cargo test -p crowdb-access-server --no-default-features --features iceberg --test iceberg_file_upload_test --test iceberg_file_body_test --test iceberg_file_auth_test --test iceberg_file_request_test`.
- Gates: `pixi run -- cargo fmt --all -- --check`, `pixi run rs-lint`,
  and Iceberg-E2E all-target clippy from the shared verification section.
