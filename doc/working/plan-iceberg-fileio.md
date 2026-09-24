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
  table/draft credential vending is connected. Projection primitives and canonical
  fallback tests pass, but table loading still uses the canonical-only path.

## Remaining execution

- [ ] **Projection integration**: connect optional generation-local construction
  to committed metadata and selected loads. Validate table/generation/digest/
  projection version; partial publication cannot block canonical reads or commits.
  Test equivalent validation and byte-identical fallback, not merely a cache hit.
  Files: `src/metadata_projection/`, `src/table/load.rs`, commit integration.
- [x] **Selected-use coverage**: partition-statistics schema/rows/inventory,
  retained history and SDK publication pass with R182. Ordinary rewrite semantics
  follow the confirmed engine boundary; file, sequence, partition, position and DV
  validation remain enforced. Independent work/byte/entry limits pass.
- [ ] **Cross-instance recovery acceptance**: map every R180 multipart/publication
  crash and lost-response case to library or real-stack evidence; add missing
  two-listener native cases. Verify same-location equal/different writes, frozen
  completion recovery, abort/expiry and retained orphan evidence.
  Files: multipart/repository/recovery tests, native FileIO fixture.
- [ ] **Credential lifecycle acceptance**: test native timed refresh, expiry and
  clear fencing; later compose rename/drop lifecycle with exact prefix authorization.
  Existing Java provider cache/expired-seed and draft-isolation tests are not a
  complete timed native expiry matrix.
  Files: server credential/auth tests, Java/native fixtures.
- [~] **Official FileIO matrix**: cover data and equality-delete files uploaded
  through identical ordinary S3 operations and rejected wrong selected uses.
  Confirm unsupported operations, path escapes, trailers, immutable conflicts and
  independent byte/count/concurrency budgets across the enabled SDK profile.
  Files: `iceberg_file_http_test.rs`, SDK fixtures, admission tests.
- [ ] **Close R180**: run all acceptance cases, focused/full tests and gates;
  update in-scope design and remove requirement/index/plan only when complete.

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

- Focus changed format/manifest/projection/multipart tests first, then
  `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`.
- Server: `pixi run clean-env && pixi run -- cargo test -p crowdb-access-server --features iceberg --all-targets`.
- Native storage and Java environment commands are centralized in the
  [functional verification section](plan-iceberg-functional-catalog.md#verification-and-execution-notes).
- No-default transport regression:
  `pixi run -- cargo test -p crowdb-access-server --no-default-features --features iceberg --test iceberg_file_upload_test --test iceberg_file_body_test --test iceberg_file_auth_test --test iceberg_file_request_test`.
- Gates: `pixi run -- cargo fmt --all -- --check`, `pixi run rs-lint`,
  and Iceberg-E2E all-target clippy from the shared verification section.
