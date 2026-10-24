<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access Server Simplification Plan

Upstream: [Dataset SDK](../backlog/R220-dataset-fat-client-plan.md),
[Access design](../design/access-server/design-crowdb-access-server.md).

Goal: reduce Access request RPCs, copies, CPU work and shared contention; remove
coordination without a necessary business contract and retain justified authority.
This is a persistent cleanup inventory requested by the user; remove completed
items after recording their verified outcome in the owning design.

## Agreed Dataset deletion contract

- Mark deletion durably, reject new requests, then reclaim after a persisted
  grace deadline. No Access Server/client acknowledgements or reader registry.
- Requests admitted before the mark may continue during the grace period.
  Reclamation may interrupt unusually long reads; return an error, never a
  successful truncated response or a different snapshot.
- Default grace: five minutes. It is a delay, not a promise that a running job
  can issue new requests forever. Retries/restarts must not reset the deadline.
- Retention, live snapshot ownership, publication fences, physical allocation
  identity, in-flight I/O and caller-owned buffers remain distinct protections.
- A read checks authoritative availability at admission. A stale cache is not
  sufficient to admit a new read after deletion. Read paths perform no snapshot
  activity writes, heartbeats or asynchronous release writes.

## Approved stateless read contract (2026-10-24)

- The user explicitly removed persisted read sessions. This supersedes the
  earlier inventory's proposals to retain durable ack/resume/remote close.
- Requests carry the query and continuation. The continuation binds Dataset,
  immutable Snapshot, Split revision, projection, worker partition and shuffle.
  Processing checkpoints belong to the caller; fetching/releasing a buffer does
  not commit progress anywhere. Reusing a continuation replays its batch.
- Remove session records, persisted selected-ID pages, preparation progress,
  version reservations, TTL renewal, ack/close routes and per-frame state reads.
  Do not replace them with a required in-memory session or sticky routing.
- Keep bounded work, cancellation, integrity and buffer ownership. New requests
  still check snapshot deletion. A continuation is not an authorization grant.
- Review remaining persistent mutations by use case and total RPC cost before
  changing their business semantics. Strict nonempty namespace deletion remains
  approved; durable publication and shared data ownership need separate review.

## Execution

- [ ] **Confirm remaining persistence contracts**: review P1–P14 below before
  changing resource/recovery semantics. In particular confirm disposable draft
  work, upload restart behavior, exact retry responses and global quotas. Strict
  nonempty namespace deletion is already decided and remains unchanged.
- [ ] **Reduce remaining request costs**: execute the still-applicable findings
  1–13 below; session-only findings are superseded by the completed removal. Measure total reads/mutations and serial
  dependency depth, not only CAS counts. Consume returned CAS observations on
  justified retries; preserve explicit expected-version failures.
- [ ] **Measure real services**: rebuild matching sources and run mixed-size/restart
  acceptance after the confirmed simplifications. No throughput claim from static RPC counts.

### Verified session cleanup

- Replaced persisted Dataset read sessions with `DatasetReads` and caller-owned
  continuation tokens. Removed remote ack/resume/close, reader TTL/configuration,
  selected-ID/progress pages and legacy cursor storage. HTTP v1/v2 binding uses
  `POST reads/bind`; `POST reads/next` replays a continuation. Existing v1 direct
  batch reads remain `POST reads`. Native and Python callers use the same shared
  query semantics, with client-side processing checkpoints.
- Added zero-write regressions for bind, 96 concurrent fetches, cross-instance
  replay and legacy scan continuation. Covered immutable Snapshot/Split binding,
  worker coverage, cancellation, deletion admission and bounded token size.
- Passed Dataset core all-target tests and the final focused query regressions;
  Access Server and native SDK all-target tests; independent HTTP SDK tests;
  affected all-feature clippy, independent SDK clippy and both fmt checks.
  Parsed both OpenAPI documents and resolved every local reference.
- The HTTP SDK wheel was rebuilt and installed into an isolated environment.
  Its Split/read round trip passed against the real HTTP listener, with the
  consumer script copied outside the repository.
- A full Access run initially exhausted disk during linking, before tests;
  deleting only rebuildable incremental cache restored space. A subsequent
  payload test exposed a conflicting `reads` route; the independent `reads/bind`
  route fixed it, and the complete Access suite then passed.
- This validates session removal, not full R220 performance acceptance. The
  container mixed-size/restart workload and local-HCA comparison remain pending;
  an OCI image from baseline `878ee901` does not contain these changes.

The review below describes baseline `878ee901`. Its session preservation and
remote-close alternatives are superseded by the approved contract above; those
entries remain evidence of the cost being removed, not current requirements.

## Remaining persistence review (2026-10-24)

Scope: Access Server adapters plus Dataset, S3 and Iceberg metadata repositories.
`persistent` here means durable application metadata, including read-triggered
cache fills. A local atomic, resource permit or transport connection is not a
persistent read session. Chunk allocation/WAL/Paxos durability remains storage
infrastructure and is outside this Access semantic cleanup. Counts are logical
metadata operations; nested validation/replication and payload IO are additional.
These items are review proposals, not authorization to remove business guarantees.

- **P1 — Iceberg GET can persist an optional projection**. `TableLoader::representation`
  in `lib/crowdb-access-iceberg/src/table/load.rs` calls `ProjectionStore::put_document`
  on a refs-mode cache miss. `metadata_projection/repository.rs` writes each child
  page and root with create-CAS; `validated.rs` rereads the root and writes a
  validation receipt. For P child pages this fill alone adds `(P + 2)C + 1G`,
  on top of reads, parsing and hashing. Concurrent GETs repeat that durable work.
  This is derived data, not publication authority. Proposal: remove synchronous
  persistent fills from reads; compare a bounded local cache with publication-time
  construction. Review decision: whether a durable projection is needed at all.
- **P2 — Dataset Composition workflow**. `composition/{begin,edit,state,evaluation,
  evaluate_step,prepare,cleanup}.rs` stores draft revisions, edit receipts,
  evaluation windows/rows, candidate pointers and cleanup progress. Use case:
  editing a large derived Dataset and recovering across Access restarts without
  redoing all preparation. Each window/row/receipt adds separate create-CAS and
  progress replacement, with validation GETs; this is not one publication CAS.
  Proposal: client-owned editing specification and replayable/disposable
  preparation, retaining only immutable candidate data plus publication authority.
  Decision: may an unfinished draft/evaluation be lost and restarted? If yes,
  remove durable editing/progress first; do not remove committed candidates.
- **P3 — Dataset Split workflow**. `split/{lifecycle,state,work,evaluation,prepare,
  recovery,cleanup,initial}.rs` similarly persists edits, evaluation progress,
  overlap examples, membership work pages and fences. Published membership and
  revision identity are business data; draft progress is auxiliary recovery state.
  Proposal: compute from client-owned rules, publish immutable revision once,
  restart incomplete evaluation instead of journaling every window. Same decision
  as P2; retain frozen historical membership and explicit currentSplit CAS.
- **P4 — Dataset uploads**. `upload.rs`, `upload/{parts,index,ownership,retirement,
  reclaim}.rs` retain upload descriptor, part intent/result/index, completion,
  owner list, retired-owner IDs and abort/cleanup status. Trigger: multi-request
  payload upload, retry after lost storage replies, sharing one payload across
  candidates/Snapshots. Create currently costs `1C`; part acceptance adds intent,
  part receipt and index mutations plus repeated state reads. Owner mutation is
  `1G + C` attempts on one upload; observed conflicts must feed the next attempt.
  Permanent retired-owner IDs can grow, and the part index serializes writers.
  Proposal: separate immutable part results from upload-wide state; assess replacing
  the growing owner list/index with per-owner/per-part records and bounded discovery.
  Decisions: is resumable upload across restarts required, and may an incomplete
  upload be discarded? Shared published payloads must not be reclaimed accidentally.
- **P5 — S3 multipart**. `metadata/multipart_repository{,/parts,/completion,
  /publication,/terminal}.rs` stores upload identity, immutable parts/index,
  completion/abort state and publication evidence. These implement S3 multipart
  operations, not login/read sessions. Baseline normal new part is `7G + 4C` at
  the helper boundary (finding 5); payload and replica IO are extra. First simplify
  redundant state reloads and pass observed/applied values through helpers.
  Removing upload durability changes resume/list/complete semantics and requires
  explicit review; don't silently break S3 clients while removing Dataset reads.
- **P6 — Iceberg multipart and durable admission credits**. `file/multipart_repository/`
  and `file/multipart_credits{,/settle}.rs` contain upload state plus a catalog-wide
  sessions/byte reservation and pending-credit journal. Such a shared counter
  serializes otherwise independent transfers. The current Access file upload path
  uses its active upload-head flow; legacy durable-credit code must be traced by
  caller before attributing its cost to active requests (finding 7).
  Proposal: remove unreachable credit machinery; use bounded local producer
  admission for resource protection. Decision for any reachable global budget:
  are hard cross-server quotas a required contract, or is per-server admission enough?
- **P7 — Iceberg file writer recovery**. `file/{writer,write_intent,injection}.rs`
  and writer checkpoints persist physical ownership/offsets, exact write intents,
  completed file records and adoption receipts. Current active part publication
  costs `1G + 1C` after admission; intent/checkpoint paths have separate costs.
  Purpose: distinguish committed bytes from readable but unconfirmed bytes after
  timeout/crash, and avoid adopting recycled storage. Proposal: retain physical
  generation/fencing and immutable completion; review whether incomplete writer
  checkpoints can be discarded/replayed. Acceptance must inject lost storage replies.
- **P8 — Iceberg HTTP retry ledger and body copies**. `operation/{retry,result,payload}.rs`
  and Access `iceberg/{table_write,namespace_write}` persist request binding,
  original body pages, result body and terminal status. `RetryLedger::begin` checks
  context and locates a binding before create-CAS; finish checks context, locates
  the binding, reads/writes result and updates binding again. Large bodies add
  page reads/writes; replay may repair a status marker. Boolean CAS loses the
  conflict observation and adds a confirmation GET. Proposal: first reuse observed
  values; then merge retry evidence with the actual mutation journal or commit
  record. Decision: must every retry reproduce the original response, or may an
  ambiguous reply return a conflict/unknown outcome for client reconciliation?
- **P9 — Iceberg namespace/table journals and reservations**. `namespace/{journal,
  reservation,admission,publication,drop_fence,drop_finish,update_recovery}.rs`,
  `table/lifecycle/` and `commit/create/` persist name reservations, parent entries,
  namespace/table generations and recovery phases. Multiple records/RPCs implement
  multi-key create/drop/rename recovery. **Nonempty namespace deletion must fail**,
  already confirmed by the user; do not ask again or replace it with eventual
  emptiness. Review phase journals and duplicated context reads independently.
  Removing a journal must still preserve strict parent admission and name uniqueness.
- **P10 — Dataset publication and explicit resource controls**. `authority.rs`,
  `authority/publication/`, `split/{head,publication,recovery}.rs` persist Dataset
  identity, immutable Manifest/Snapshot/Split, latest/stable pointer, retention
  marker, commit attempt/receipt and visibility evidence. User mutations need
  durable results; head CAS prevents a stale expected generation from publishing.
  Extra attempt/receipt/visibility records are separately reviewable overhead.
  Proposal: minimize the commit record set while preserving lost-reply outcome
  discovery; evaluate whether the separate retention API is actually used.
  Dropping persistent resource data is not justified by removing read sessions.
- **P11 — S3 object/bucket publication**. `publication.rs`, `bucket.rs`,
  `metadata/{store,namespace}.rs` store object version/location/user metadata,
  bucket identity and deletion markers. Unconditional PUT/DELETE should use the
  overwrite contract without a preliminary GET/CAS; conditional operations retain
  their expected-version check. Bucket tombstone replay still has an avoidable
  bool-CAS/put-if-absent sequence (finding 4). These are resource writes; review
  unnecessary mutation stages rather than replacing persistence with volatile state.
- **P12 — Dataset inspection indexes**. `inspection/{index,preparation,legacy,
  retirement,reclaim}.rs` store schema/member/field projections, summary and
  preparation/reclamation progress. They enable bounded reads without decoding a
  full Manifest, but build cost is serial and multiplied per row (finding 9).
  Explicit legacy preparation remains a mutation. Proposal: keep useful immutable
  indexes with publication; compare batched/co-located writes and restarting
  interrupted builds, removing auxiliary progress where redo is acceptable.
- **P13 — Cleanup/GC tasks and tombstones**. Dataset `authority/access/`, upload
  cleanup and Split/Composition cleanup; Iceberg `gc/{repository,budget,worker}`;
  S3 multipart terminal cleanup persist claim/phase/cursors and deletion fences.
  Mark + delayed reclamation is already approved for snapshots. Proposal: use
  idempotent bounded scans and local cursors where restart only repeats work;
  review durable global counters and per-page progress. Keep enough physical
  identity/fencing that a delayed delete cannot free a newly reused range.
  Decision: accept recomputation after cleanup-worker restart and approximate
  progress counters? Distinguish that from a repeated destructive operation.
- **P14 — Catalog lifecycle and credential authority**. Iceberg
  `catalog/{repository,storage}.rs` persists active context/root/epoch; Access
  `credentials{,/ensure}.rs` persists user credentials in Group-0. These are
  administrative identity/revocation boundaries, not per-read sessions. Review
  repeated root/context GETs and redundant initialization writes; deleting these
  records would change authentication or catalog-reset safety. S3 session-token
  credential fields alone are not evidence of server-side session persistence.

Execution priority after confirmation: P1 read-triggered writes; P2/P3 disposable
workflows; P8 duplicate retry persistence; P4/P5 upload amplification; P6/P13 global
coordination. Every item must name its required invariant, exact normal/conflict/
unknown-outcome RPC path, and a smaller alternative before implementation.

### Cost still present in the new stateless read

- No persistent reader record or selected-ID page is written. Continuations do
  not eliminate snapshot/index/locator reads, payload IO or digest work.
- A partially consumed shuffle group is reconstructed; small `batch_size` with
  very large `group_size` can therefore increase metadata RPCs. Repeated selection
  windows also reload schema/membership validation. This is an explicit remaining
  performance review item, not a claimed throughput improvement. Compare bounded
  client-side planning/group buffering against server re-evaluation before choosing
  a default; do not reintroduce mandatory read-session storage to cache that work.
- Query, payload admission and inspection can repeat deletion/visibility checks.
  Consolidate within a request only after preserving the new-request deletion
  boundary. Native/HTTP streaming no longer reads a session record per frame.
- Legacy `DatasetReadService::scan_surface` still reconstructs all planned windows
  from the Manifest before selecting one cursor window. Removing its persisted
  cursor does not remove that CPU/memory amplification; migrate callers to the
  bounded inspection path or replace the planner in a separate measured change.
- Old `DS\\0RS\\0` and cursor records are no longer consulted or written. Physical
  purge needs a separately scoped maintenance operation and coordinated removal
  of old writers; no bulk deletion was executed during this cleanup.

## Performance review: counting rules and current evidence

- Static review baseline: `878ee901`. The prior implementation passed Dataset,
  Access Server, S3 and native component/integration suites, namespace admission
  and drop suites, workspace all-feature clippy and fmt. Its local OCI image
  built successfully. Full mixed-size workload/restart was not run on that
  revision before the user requested this broader review; do not report it as
  accepted performance.
- `G` means one Access metadata point read, `B` one bounded read, `S` one scan,
  `C` one conditional mutation, and `W` one unconditional mutation. `C(None)`
  is create-if-absent, not a read-modify-write retry. Counts below describe the
  named function path, not an entire HTTP request unless explicitly stated.
- These are logical storage calls, not physical wire-packet counts. Dataset `B`
  currently becomes a one-key `scan_bounded`; routing, range crossings, topology
  discovery, replica reads, Paxos and WAL can add messages and work. Measure
  actual client RPC sends and server replication separately.
- A definite CAS conflict returns the value observed when the mutation was
  evaluated. It can already be stale by the time it arrives. Use it immediately
  to recompute an allowed retry; repeated competition can still fail, so preserve
  the existing request deadline and bounded retry budget. Never wait for three
  failures before using it. The three-conflict test injects competing writes to
  verify this behavior, not a production batching/backoff rule.
- A lost reply is not a definite conflict. It may require exact-operation replay
  or a confirmation read. An explicit expected revision/selected generation must
  return a conflict when invalidated, rather than silently applying user intent
  to a different base.
- Add request-scoped traces/counters in test wrappers and existing metrics, not
  per-RPC production logging or a global profiling lock. Count GET/B/scan/CAS/
  PUT/delete, repeated keys, retries, bytes, concurrent operations, and serial
  dependency stages. Separately record copied bytes, allocations/retained bytes,
  digest/codec CPU, queue wait, active requests, wakeups and cleanup lag.

## Findings and proposed corrections

### 1. Repeated snapshot admission within one Dataset batch — confirmed

- Use case: read the next batch of a persisted session, with deletion rejecting
  new requests and admitted reads allowed to finish during grace.
- `DatasetSessions::next` reads session + deletion state + selected ID pages:
  `2G + P G`, where P is the number of ID pages. Native `ReadAdmission::admit`
  and HTTP `ReadAdmission::acquire` read deletion state again. For a nonempty
  batch, `ready_inspection -> inspection_snapshot_committed` reads the same
  deletion state a third time through `B`, before separate commit proof reads.
- Proposed: establish one request-local, snapshot-bound admission result and
  reuse it across session/locator/body setup. Public standalone entry points
  must still admit independently. Do not cache mutable deletion permission
  across requests or confuse it with immutable publication proof.
- Verify: count one authoritative deletion check per admitted request; race
  deletion at each boundary, direct/session and HTTP/native parity, missing and
  unpublished snapshots, retained bodies and failure after physical cleanup.

### 2. Remote session GET on each body pull — confirmed; contract-sensitive

- Use case: a separate client closes a session while another client is reading
  its current body. Existing code deliberately observes that close remotely.
- Native `BatchBody::pull -> ReadAdmission::check -> check_open` performs `1G`
  on each pull. HTTP `BatchState::next` does the same before even returning
  already-held `pending` bytes: frame header, payload, field end and stream end
  can each incur a session GET. This traffic grows with delivery fragmentation,
  not only logical requests or payload size. Ordinary direct batches have no
  session ID and skip this cost.
- Proposed: first eliminate duplicate checks within one logical delivery step
  if its existing cancellation boundary permits it. Separately evaluate whether
  remote close must interrupt an admitted body, or can reject the next request
  while local cancel stops the current body. The latter removes per-pull RPCs
  but is a distinct contract change, not implied by snapshot deletion approval.
- Verify: current remote-close tests, empty/pending/EOF/capacity-wait cases,
  plus RPC counts versus frame count. No new persistent reader registration.

### 3. Upload metadata repeatedly reloaded while reading — confirmed

- Use case: read an immutable completed upload referenced by a snapshot field.
- Native `FieldReader` loads upload metadata once, but every
  `read_part_stream` loads it again and then loads the part: `1 + 2P` metadata
  GETs for P parts, excluding snapshot/session/locator work.
- HTTP `BatchState` loads metadata before each part, and
  `read_part_verified` loads it again before the part record. Its normal chunk
  branch costs `3P + 1` GETs including the final end-of-field metadata probe,
  excluding per-frame session checks and payload reads.
- Proposed: pass a validated, request-owned upload descriptor to part readers;
  validate part index/length/digest against it. Reclamation may make later part
  I/O fail; it must never yield successful truncation or another upload's bytes.
  Keep mutable admission separate from immutable shape and physical identity.
- Also remove upload `create/complete/abort` readbacks that only reconstruct
  state already returned by a successful mutation or conflict; assemble parts
  for response only when the response needs them.
- Verify: per-field/per-part call budgets, shared owners, corruption, abort and
  physical reclamation races; held buffers remain valid after completion.

### 4. CAS observations still discarded by retry paths — confirmed

- Dataset `upload/index.rs::index_part` does `GET -> CAS` on every attempt.
  Concurrent different part numbers update one 128-part index page; merging
  into the returned page is necessary to preserve both, another GET is not.
- Dataset `upload/ownership.rs::{abort,cleanup,cleanup_part}` loops reload the
  same upload/part after confirmed CAS conflicts. Retain CAS to prevent abort
  racing a claim, a delayed write intent overwriting reclaim, or an owner update
  being lost. Recompute state from each returned observation.
- Dataset `authority/access/reclaim.rs::{fence_reclaim,retire_snapshot_record}`
  and `split/recovery.rs::invalidate_split_head` do the same. The head nonce
  change is a real publisher fence even if latest/stable IDs are unchanged;
  it is not removable as a no-op. Reuse observed head and preserve receipt help.
- Dataset `read_session/planning.rs::start` and `read_session/split/finish.rs`
  discard a session-create conflict value before entering `resume`; existing
  start also reads a record and then resumes through another read. Pass state
  through instead. Existing progress and upload-owner retry loops already use
  observations; keep them as the expected behavior.
- S3 `bucket.rs::create_bucket` discards a tombstone replacement CAS result
  through the bool wrapper, then performs another put-if-absent. Decode and use
  the returned active mapping/tombstone immediately. Keep name uniqueness CAS.
- Iceberg `RetryLedger::begin` restarts `ledger_locate` after a failed bool CAS;
  use the returned occupant, looking up an overflow key only when the competing
  operation actually belongs to a different collision slot. `finish` also GETs
  result bytes already supplied by a definite conflict.
- Verify: inject different numbers of actual conflicting writes, assert one
  initial read and zero same-key re-GETs solely for each definite conflict;
  preserve terminal/missing outcomes, explicit preconditions and lost replies.

### 5. S3 multipart helpers discard known state — confirmed

- Use case: publish a new part in an existing open upload; simultaneous parts,
  complete and abort share that upload's durable pending/accounting state.
- With no existing part/generation, no pending work, no conflict and no error,
  `put_stream_part` currently performs **7G + 4C** before returning:
  load index/session (2G), read part (1G), create immutable generation (1C),
  reserve session (1C), settle reload index/session (2G), read generation (1G),
  read pointer (1G), publish pointer (1C), clear pending (1C).
- `load_identity` reads an index even though the session key is derivable and
  `MultipartSessionRecord::decode` checks bucket/object/upload identity. Assess
  whether index presence is an independent contract or only listing metadata;
  remove it from point operations if the session alone is authoritative.
- Carry the known session, generation and pointer into settling; CAS still
  protects against a competing writer. Make `exchange` return typed observed
  state through its callers instead of a bool that forces `load` again.
- `publish_completion` has an extra object GET after definite CAS conflict.
  `finish_publication`, `abort` and `settle_pending_part` likewise reload the
  session after `exchange` discards the observation. Uncertain errors retain
  separate confirmation logic. Do not silently overwrite a changed predecessor.
- The four conditional writes have separate current responsibilities. Reducing
  them further requires proving quota accounting, same-part replacement and
  freeze/abort behavior, not simply replacing them with unconditional PUTs.
- Verify: call counts for new/replaced/replayed parts, concurrent distinct/same
  parts, complete-vs-abort, generation orphan cleanup and every lost-write reply.

### 6. Iceberg phase machines reload their own progress — confirmed

- Use case: namespace creation/drop/rename/property update and table lifecycle
  operations survive Access restart and helpers finishing another request.
- `NamespaceJournal::advance`, table lifecycle/create/commit journals and
  `CatalogRepository::cas` reduce CAS to bool. Their driver loops then load
  progress from storage after both successful advancement and conflicts.
  `NamespaceJournal::load` also checks ActiveRoot, so one reload is at least
  `2G`, before payload and authority reads. Preserve applied or observed typed
  state within the running request; restart still loads durable progress.
- `NamespaceRepository::load` costs `2 + 2D` GETs for an existing D-component
  namespace: two context checks plus mapping and authority for each component.
  Nested callers often resolve the same namespace/context again. Identify each
  required clear/drop boundary before introducing request-local reuse; a cached
  Ready result cannot grant indefinite mutation permission.
- `cleanup_marker` GETs the exact expected value before issuing a conditional
  replacement. The CAS itself rejects a changed value. Compare direct-CAS and
  existing-marker replay costs; carry known observations when available.
- Preserve the user's strict nonempty-delete guarantee. Parent admission,
  name reservation and publication proof cannot be removed merely because the
  recovery path has too many RPCs. Simplify state propagation first.
- Verify: count per phase; concurrent create/drop/rename, nonempty rejection,
  recreation, epoch change, helper races and lost replies at every durable step.

### 7. Active Iceberg paths versus legacy work — confirmed distinction

- `publish_direct_commit` already conditionally publishes one table head and
  examines the observed head on conflict. Keep optimistic table requirements;
  do not add its legacy multi-phase publisher to normal commit cost estimates.
- Active streamed `put_stream_part` uses one part GET plus one part CAS after
  admission, and returns a conflict instead of silently rebasing replacement.
  Inspect its callers for duplicate session/context lookup; do not introduce
  catalog-wide quota writes already absent from this path.
- Legacy multipart `part` checks `load` before and after reading a part; each
  `load` itself reads context + session, giving `5G` for this helper. Completion
  can nest further load/exchange checks. `exchange` checks context before and
  after CAS. Track these as legacy/recovery costs, with a migration/removal
  option if there are no live callers; never attribute them to streamed traffic.
- Native injection selects an immutable receipt, then publishes file authority;
  both have request/content identity and deletion replay constraints. Preserve
  winner selection while passing known descriptors and admission through.
- Verify: active route tracing, file replacement/deletion, retry identity,
  direct-file no-KV reads, old-session recovery and catalog clear/rotation.

### 8. Fewer writes can still mean more RPCs — confirmed trade-off

- Previous cleanup added preflight reads to Dataset immutable session metadata
  and Composition/Split receipt helping. A new value now costs `G/B + C`
  instead of just `C`; an existing identical value costs a read instead of a
  failed conditional mutation. Zero-write replay tests alone miss this trade-off.
- `reserve_session_version(version=2)` can read VERSION, check legacy SPEC, then
  call `immutable_session_metadata`, which reads VERSION again. Carry that first
  observation and avoid redundant preflights.
- Iceberg `PayloadStore::put_page` similarly performs GET then create-if-absent;
  a new P-page payload costs `P G + P C`, serially. Journal validation rereads
  payloads which the same request may just have produced.
- Proposed: distinguish definitely-new writes from replay/help paths using
  existing request-owned evidence, not a new durable flag. Compare direct
  create-if-absent, known-value reuse and read-before-write with total round trips
  and server work. Immutable CAS preserves identity collisions and cleanup seals;
  plain PUT is allowed only if divergent overwrite/resurrection is impossible.
- Verify: first request versus repeated helpers, collisions, late writers after
  cleanup, bounds and total GET+CAS counts as well as persistent mutation counts.

### 9. Serial fine-grained metadata I/O — confirmed shape, cost to measure

- `resolve_inspection_batch_locators` serially walks samples and fields: one
  membership read per sample, a field metadata lookup per ancestor searched,
  then a separate locator/inline-value lookup for each present value. Costs grow
  with samples × fields × ancestry depth. Schema and snapshot proofs add work.
- These bounded point reads use scans in `chunk_store/metadata.rs`, not a native
  bounded point-get RPC. Review protocol support before substituting an unbounded
  GET; byte/deadline bounds must remain enforced before materialization.
- `persist_inspection_rows` writes schema, membership, field metadata and value
  rows one at a time. For F schema fields, N samples and E present sample-field
  entries it emits `F + N + 2E` conditional row writes, then 3 summary/index
  writes, excluding manifest/publication records. Session ID pages and Iceberg
  payload pages are also written serially.
- Proposed: batch compatible point operations or use bounded concurrent reads/
  writes with a shared byte/work budget; reuse immutable request-local schemas
  and proofs. Consider co-locating metadata/locators only after measuring byte
  amplification and defining migration. Preserve output order and missing-field
  semantics; do not turn a sparse projection into an unbounded Manifest read.
- Verify: samples/fields/ancestry/page-count sweeps, RPC count and dependency
  depth, missing/tombstone fields, byte budgets and cancellation under concurrency.

### 10. Payload copies and full-part materialization — confirmed

- HTTP Dataset upload collects an entire part before writing. Byte-mode
  `ChunkPayloadStorage::read` concatenates receive chunks into `BytesMut` before
  HTTP delivery. Inline HTTP delivery uses `Bytes::copy_from_slice` even though
  the request already owns the value. Keep the native owner-backed path intact.
- Compatibility `UploadedChunkReader::read` adds another concatenation and then
  `output.to_vec()`, copying the assembled result again. Trace its active callers
  before prioritizing; it is not the normal native streaming reader.
- Generic S3 coalescing copies frames into a fresh Vec per receive window;
  native direct-receive is a separate path. HTTP read-ahead/native fallback and
  mixed-size partial frames need separate measurements, not a zero-copy label
  applied to all traffic.
- Proposed: move existing owners into outputs, stream retained payload owners
  where framing allows, avoid concatenation just to change container type, and
  keep bounded aggregation only where measurements justify it. A header-copy
  and payload-copy have different costs; count bytes, not only copy calls.
- Verify: address/owner lifetime, partial frames and unaligned sizes, caller-held
  buffers, cancel/drop/shutdown, peak physical allocation and explicit fallback.

### 11. Digest, serialization and allocation CPU — confirmed work, profile first

- `DatasetUploads::complete` serially rereads every accepted part. Each
  `verified_part` verifies its MD5, then completion hashes the bytes again for
  whole-upload MD5. Part MD5s cannot simply be combined into whole-upload MD5;
  removing this read requires durable trusted whole-content evidence, not
  trusting a caller checksum or weakening integrity.
- Native `VerifiedUploadPart::next` and `FieldReader::delivered` both hash
  delivered bytes for different scopes. When a field is exactly one verified
  part, investigate sharing the certificate; multipart fields still require
  their whole-field validation. Measure hash CPU separately from transport.
- Upload/session/draft CAS serializes records repeatedly; owner revocation lists
  grow to 4096 entries, part-index pages to 128 receipts. Conflicts repeat
  decoding/encoding of whole records. Record encoded bytes and CPU by size and
  contention; evaluate narrower records only if lifecycle atomicity survives.
- Inspection stores JSON field/locator values and clones sample/field metadata
  for response headers. Profile parsing, allocations and inline byte encoding,
  then pre-size or move owned values where justified. An Arc/Bytes clone does
  not imply a payload copy; do not optimize it based on syntax alone.
- Verify: CPU samples, allocations/request and bytes processed by each digest
  or codec; singleton/multipart, small/large and high-owner-count cases.

### 12. Upload expiry throughput and coupled cleanup — confirmed

- Current `start_cleanup` wakes every 15 seconds, runs snapshot cleanup first,
  then one upload page. `expire` scans 20 keys from `DS\0U\0`, including part
  and index records; it also revisits claimed and cleanup-complete authorities.
- With R stable scanned records, the nominal full-pass scheduling budget is
  `15 * ceil(R / 20)` seconds in the no-overrun case. At 100,000 records that is
  about 20.8 hours, before I/O delay. Five-minute grace is a minimum delay, not
  an upper cleanup bound. Removing the registry eliminated foreground writes
  but did not establish acceptable large-scale cleanup latency.
- One expired upload can enumerate up to 10,000 parts and clean them serially;
  the two-second scan timeout does not bound the entire cleanup step. A slow
  upload delays the next snapshot sweep on the same task. Multiple Access
  instances can also duplicate scans and cleanup work.
- Proposed: separate bounded progress budgets for discovery and reclamation;
  consume multiple pages while within time/bytes/I/O budget, skip child ranges
  where supported, retain incremental cleanup progress and yield fairly.
  Compare per-object expiry indexing with full scans only after accounting for
  any extra foreground write; do not restore one global registry contention key.
- Verify: sparse expired uploads among live/cleaned records, maximum-part uploads,
  repeated restarts, corrupt entries, multiple cleaners, cleanup lag and effect
  on foreground p99/CPU/metadata RPCs. Restart cursors must not lose obligations.

### 13. Admission, scheduling and lower-layer costs — measurement tasks

- Review count-based versus byte-based admission for mixed sizes, queue residence
  and capacity retained by slow consumers; differentiate overload rejection from
  insufficient in-flight work. Do not increase capacity just to raise benchmark
  throughput or reintroduce unbounded prefetched buffers/tasks.
- Native Dataset pulls one field stream at a time. Measure whether bounded
  prefetch hides storage RTT under real mixed sizes, and whether it increases
  retained-memory pressure or harms small-request p99. One thread can have
  multiple pending operations; the load generator's request/response discipline
  must be recorded.
- S3 native allocation credit has a 1-ms retry timer when other producers/CQ
  completions can release arena credit; Dataset metadata cancellation polls at
  10 ms. Measure wakeup overhead, stalls and cancellation latency before changing
  notification plumbing. Preserve missed-wakeup safety and avoid a new lock.
- Profile allocation arenas, serialization/hash time, scheduler occupancy,
  shared writer queues, batching dwell time, transport in-flight WR/byte depth,
  CQ processing, disk durability and metadata replication separately. Attribute
  a lower-layer bottleneck with traces before expanding this Access change into
  a storage or RDMA redesign.
- Include authentication/signing, error/logging paths and metrics overhead in
  endpoint CPU profiles. Retain authorization/integrity checks; consolidate
  duplicate computation or reuse already-validated request-local evidence.
- No direct production Mutex/RwLock was found in the audited Access libraries;
  that does not establish lock-free behavior in dependencies, FFI or the kernel.

## CAS use cases and disposition

- **Ordinary reads, fetch replay, payload delivery**: no persistent reader CAS.
  Keep only request admission, selected immutable identity and physical integrity;
  repeated GETs remain subject to findings 1–3 and 9.
- **Ordinary S3 object PUT/DELETE**: final object publication/deletion already
  uses one unconditional mutation; HEAD uses one object GET after bucket
  resolution. Do not add preflight CAS. Bucket names still need uniqueness and
  generation protection across delete/recreate.
- **Snapshot/table/Split head publication**: competing publishers select one
  current generation, respecting an explicit expected base. Keep one conditional
  publication point and exact lost-reply proof; do not silently rebase user edits.
- **Upload owners, claim versus reclaim**: two snapshots may reference one
  upload. Keep an atomic owner/state update so removing one owner cannot delete
  another's live data; permanent owner revocation rejects delayed claims.
- **Part intent/accept/index/complete**: prevent same-part writers, delayed I/O
  and abort/complete from publishing inconsistent data. Keep the necessary
  atomic state transitions; returned observations and known state eliminate
  redundant reads. Index-page sharing is limited to one upload and merits a
  separate contention measurement for parallel part uploads.
- **Session ack/resume/close**: same-session cursor/closed/deadline changes must
  not overwrite each other. CAS is scoped to that handle; independent sessions
  must not share mutable reader state. Normal fetch needs none.
- **Draft edits/checkpoints/cleanup seals**: explicit revision checks protect
  concurrent edits; seals reject a late writer after cleanup. Reuse known
  immutable receipts without inventing another mutable coordination record.
- **Iceberg namespace parent/name/lifecycle**: preserve strict nonempty delete,
  generation isolation and recovery. Ordinary table creation already avoids a
  parent mutation. Reduce supporting RPCs without removing the required boundary.
- **Idempotency bindings/immutable records**: create-if-absent selects one payload
  per operation/name and detects divergent identity reuse. Plain PUT is not a
  generic substitute; prove sole ownership, identical bytes and no cleanup seal
  before removing the condition. A failed create-if-absent still costs an RPC.
- **GC progress and conditional deletion**: prevent stale workers from overwriting
  newer progress or deleting a reused identity. Background workers need bounded
  work and fair progress, not reader leases. Legacy catalog credit accounting
  remains only where old records require settlement.
- Remote CAS inventory covers Dataset authority/publication/access, uploads,
  sessions, Composition/Split/inspection; S3 bucket and multipart repositories;
  Iceberg catalog/namespace/commit/table lifecycle, files/multipart/injection,
  retry/payload/projection and GC. Shared `crowdb-access-multipart` helpers do
  arithmetic/validation rather than issuing storage RPCs. Local delivery/arena
  atomics and metrics are excluded from remote CAS counts. Dynamic counts for
  every route and failure branch are still an execution task, not a static proof.

## Performance verification matrix

- Run direct read, session start/next/ack/resume/close; upload create/new-part/
  replay/complete/abort; Composition/Split edit/evaluate/publish/recover/cleanup;
  S3 ordinary objects and multipart; Iceberg active file/table/namespace paths,
  legacy recovery and GC. Include auth/admission entry costs in endpoint totals.
- Vary payload sizes, samples and fields, ancestry, part count, owner count,
  concurrency and retained buffers. Separate one shared key from independent
  objects. Include no contention, definite conflicts, response loss and restart.
- For each change record before/after logical storage calls, actual routed RPCs,
  serial stages, bytes, CPU, allocations, p50/p99 and backlog. Keep byte/digest
  correctness and resource limits fixed. Batch operations may lower RPC count
  without lowering backend I/O; report both.
- The unchanged full HTTP/native Rust/native Python workload and restart check
  remain the final acceptance. Do not compare phases from different source
  images or count an interrupted/partial run as a throughput result. Physical
  multi-host validation remains in its separate plan.

## Decisions applied to the inventory

- Dataset sessions retain explicit durable acknowledgement and remote close.
  Normal fetch/replay is read-only; start/ack prepares the next token. Only
  start/ack/resume extends admission. Legacy unprepared sessions upgrade once.
  See [Dataset read flow](../design/access-server/dataset/design-crowdb-access-dataset.md#102-read-flow).
- Composition/Split retain draft-scoped recovery checkpoints and cleanup fences
  for exact lost-reply recovery and late-writer exclusion. Source activity leases
  are absent; receipt helping avoids redundant CAS. No reader-shared journal was
  found. See [control lifecycle](../design/access-server/dataset/design-dataset-control.md).
- Dataset uploads no longer register in shared tail/pages or persist a scan
  cursor. Expiry directly scans upload authorities with local continuation.
  Permanent owner revocation protects other live snapshots; bounded part indexes
  remain to avoid one metadata point read per part during listing/completion.
  See [upload flow](../design/access-server/dataset/design-crowdb-access-dataset.md#101-upload-and-publication-flow).
- Iceberg catalog-wide credits are legacy recovery only. New streamed sessions
  already use independent session limits; no foreground reserve/release was
  found. Keep legacy recovery so old outstanding records can settle.
- **User decision, 2026-10-24**: nonempty namespace deletion must fail. Keep the
  parent admission protocol where required by creation/rename/drop recovery;
  do not replace it with cascading deletion. Ordinary table creation already
  avoids a parent authority mutation. See [Iceberg authority](../design/access-server/iceberge/design-crowdb-iceberg.md#2-authority-model).
- Ordinary Iceberg commits already use one head CAS; the legacy multi-phase
  publisher is not the normal HTTP path. Preserve GC/publication fencing because
  it prevents deleting referenced files, rather than only interrupting a reader.
- Iceberg streamed parts already publish independently by part number. S3 keeps
  upload-local accounting and the completion/part-pointer fence; independent
  uploads do not contend on one global record. Confirmed S3 CAS conflicts now
  use returned observed bytes, and part retry retains its latest loaded state.
  See [S3 data flow](../design/access-server/s3/design-crowdb-access-s3.md).
- Dataset session and upload-owner retry loops use returned observed state.
  Confirmed stale-base retries and uncertain transport reconciliation remain
  distinct. Explicit revision preconditions still return conflict; retrying an
  application mutation against a new base is not universally valid.

## Files and focused checks

- Dataset: `read_session/`, `upload/`, `composition/`, `split/state.rs`; Access
  Server Dataset cleanup wiring and publication; Dataset design/spec/OpenAPI.
- S3: `metadata/store.rs`, `metadata/multipart_repository{.rs,/parts.rs}`;
  owning design. Iceberg design corrections document existing foreground paths.
- Cost regressions: `session_cost_test`, `draft_cost_test`,
  `upload_registry_test`; lifecycle regressions retain restart, shared-owner,
  lost-response, invalid-content and cancellation assertions.
- Integration: affected Access Server/native/S3 suites; Iceberg namespace drop
  and admission tests retain the explicitly selected nonempty-delete contract.
- Gates: Rust fmt and workspace clippy through Pixi. Full workload and restart
  use `tools/sdk/dataset_workload.py` with its unchanged defaults and same-source
  image; physical multi-host acceptance remains in its independent plan.

## Inspection coverage and exclusions

- Inspected route ownership and coordination sites in
  `app/crowdb-access-server/src/` and production sources of
  `lib/crowdb-access-{dataset,s3,iceberg,multipart}/src/`, including shared credentials,
  admission, streaming, Dataset lifecycle, S3 metadata and Iceberg catalogs.
  This inventory identifies candidates; it is not a measured proof that every
  listed write is unnecessary.
- No direct `Mutex`/`RwLock` usages found in those five production source trees.
  Dependencies and FFI may contain locks; audit them only when profiling points
  there. Atomic resource counters are not automatically removal candidates.
- S3 ordinary object PUT/DELETE already use unconditional point mutations;
  ordinary GET has no persistent reader registration. Bucket-name deletion CAS,
  explicitly conditional writes and multipart completion have different scopes.
- Iceberg `FileReader` verifies physical directory ownership and block digests
  without registering each reader. Preserve stale/reused-location rejection.
- Credentials, immutable content publication, explicit retention, shared payload
  owners, authorization and physical allocation identity are not expendable
  merely because they persist state or use CAS.

## Verification for each cleanup

- Record the old guarantee, accepted replacement semantics, conflict scope,
  persistent GET/write counts and reader/request lifetime before implementation.
- Unit/integration: independent concurrent operations, deletion races, retries,
  restart, partial results, retained buffers and shared references.
- E2E: existing HTTP/native Dataset parity and mixed-size workload; use matched
  source/builds. For other domains run the affected existing protocol suites.
- Run affected tests and Rust fmt/clippy through `pixi run`. Do not increase
  retry bounds, weaken integrity assertions or call workload reduction a fix.
