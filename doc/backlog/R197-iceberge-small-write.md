<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R197: access-iceberg — Small writes and concurrent publication

#### Problem

The native Iceberg path writes application-level operation journals, retry
bindings and results, namespace admission markers, multipart credit records,
and file indexes in addition to Chunk-KV's durable WAL. A table creation
under a shared namespace repeatedly changes `NamespaceAuthority`, so creates
for different table names contend even though they do not change the same
table. In a small-cluster load of TPC-H and TPC-DS with 32 concurrent loader
workers, table creation exhausted its helping budget and returned `Busy` as
HTTP 503. Independent concurrent direct file PUTs succeeded. Terminal
multipart credit release also reported `Busy`; that is a separate shared-key
hotspot, not proof that chunk transfer caused the table-create 503.

An ordinary table update advances a commit journal through multiple KV writes,
publishes `TableHead`, then updates that head again to clear
`pending_operation`. HTTP retry handling writes an admission binding, a result,
and a completed binding. File publication writes both `FileRecord` and
`FileMapping` and repeats root, table, and reclamation GETs. Per-block
`FileWriteIntent` writes may amplify large uploads. These costs scale with
operations and blocks rather than with actual conflicts. See the
[native Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md),
[upload-flow analysis](../design/access-server/iceberge/design-crowdb-iceberg-upload-flow.md),
and [upload benchmark requirement](R196-access-upload-benchmark-regression.md).

Read amplification is also structural. `check_context()` reads `ActiveRoot`
on every call, and a table-create journal `load()` reads it both before and
after the journal GET; the create loop reloads that journal at each phase.
On the no-conflict direct PUT publication path, `FileRepository` performs at
least 13 catalog GETs and two CAS operations after bytes are prepared; the
HTTP entry first reads root and catalog authority. Exact file reads load a
location mapping, file record, deletion fence, and GC claim, in addition to
root checks. Table GET and HEAD, and table writes, also use a fixed
four-request `SpoolPermit` per listener; the read permit survives with the
response body. This rejects independent requests at a concurrency of five
even when metadata and storage have capacity. These are code-path lower
bounds and limits, not measured cluster latency contributions.

The current GC runtime schedules existing tasks, table purges, and retired
catalog work. Its live-table file-candidate machinery may reclaim a published
file solely because no retained snapshot references it. The native FileIO has
no ordinary file DELETE: DELETE currently only aborts a multipart upload.
Keep ordinary file DELETE unavailable: an isolated file request cannot cheaply
prove that retained snapshots do not reference the path. Removing foreground
recovery records still requires a way to reclaim incomplete physical writes
that never published a file record.

Table purge already has a durable marker and worker, but GC currently defaults
to disabled and rejects a retention setting shorter than seven days. That
floor is a CROWDB policy, not an Iceberg requirement, and prevents a requested
table purge from starting after a 20-minute delay.

#### Solution

Small-object transfer also follows the native large-object buffer ownership
model. In single-node mirror mode the routing boundary is strictly below
`disk_block_bytes * threshold_ratio` (normally 1 MiB * 0.9). A per-object
small writer owns checksum computation and retains the received owner until
completion. Receive calls may fill it incrementally; frame boundaries never
create independent object submissions. Reserve header/footer space before
receiving payload, prepare checksums in place, and hand the entire framed
owner to the shared pipeline once. The worker sets every frame's destination
chunk ID after placement and aggregates immutable owner views without copying
payload into a strip shadow. Receive owners allocate the exact object payload plus its frame overhead.
Retained owner views provide the strip prefix for recovery; repair or EC
conversion may materialize an image off the ordinary mirror write path.
HTTP header parsing may copy a prefetched body prefix; preserve that library
behavior and observe each copy with one bandwidth metric recording count
and bytes. Preserve whole-object placement, durable
readability, bounded retained owners, and terminal completion.

Shared chunk capacity and the number of strips prefetched per group are
service configuration, defaulting to 256 MiB chunks and 32 strips per group.
Start the next batched prefetch when half the current group remains. Append
each new group after the full preceding reservation, including its hidden
strips; confirmed readable capacity does not define the append position.
Keep batched allocation and refill ahead of demand;
do not allocate a strip per object. Multi-frame objects that fit a strip
must share its remaining capacity with other objects. Queue draining sends
available work promptly, bounded by strip space; it must not wait indefinitely
for a full strip. Treat the hash as a preferred pipeline hint; probe other pipelines with atomic
capacity reservation before applying backpressure. Preserve one pipeline's
write -> readable-cursor confirmation -> next batch ordering. Queued requests
aggregate naturally while publication is in flight; multiple pipelines provide
concurrency. Across strip boundaries, split immutable owner views rather than
copying payload or forcing a fresh strip. Resource allocation runs in background
prefetch tasks and outside chunk lifecycle guards; only publication retains the
existing short metadata guard.

The current gaps include small uploads bypassing native owner handoff,
per-frame payload/frame/shadow copies, forced strip rotation for multi-frame
objects, a blocking checksum task per tiny upload, and strip-prefetch settings
not exposed by the service. Benchmark 1 KiB and 512 KiB payloads separately.
Add a labelled aligned profile with 65,502-byte payloads, whose 34-byte frame
overhead yields exactly 64 KiB; it supplements rather than replaces those sizes.

Use one durable publication update for each independently mutable logical
resource. Prepare data and metadata outside the visible state, then publish
the result through that resource's key. A conditional update is needed only
when concurrent writers can change the same state and both results must be
preserved. Different table names, tables, file paths, and multipart sessions
must not modify a shared namespace or catalog counter on their normal write
path. Do not add a process lock or a shared admission key. Count all Chunk-KV
GETs, scans, CAS operations, and other writes on the request path, including
payload and recovery records; calling a CAS the only publication write does
not hide preceding KV writes.

> **Warning — destructive object cleanup.** S3 `DeleteObject` and
> `DeleteObjects` are explicit cleanup operations. Before calling either,
> the client must coordinate all writers and retries that could commit any
> requested path, including staged but unpublished metadata, and guarantee
> that no in-flight or future commit will reference that path. The server
> scans references in retained table metadata and refuses referenced or
> indeterminate files. It cannot detect an unpublished commit or enforce the
> client's future promise without adding commit-path coordination. Breaking
> this contract can leave a published snapshot pointing to a deleted file.
> Success means logical deletion was recorded; physical reclamation waits
> for admitted readers. Clients retaining old metadata beyond that delay
> cannot assume the deleted bytes will remain available.

S3 specifies the request and response format, not Iceberg reference safety.
CROWDB adds separate delete authorization and rejects referenced or uncertain
files. A batch request may return HTTP 200 with per-key failures; clients
must inspect every result before considering cleanup complete.

The following invariants define the design:

- **I1 — Independent progress.** Creating or updating different tables in
  one namespace does not write a common namespace record. Uploading different
  files and advancing different multipart sessions does not write a common
  catalog counter. A conflict on one resource cannot consume another
  resource's bounded helping or retry budget.
- **I2 — Single table commit point.** A table update prepares immutable
  candidate data and metadata, then performs one conditional `TableHead`
  update against the version it validated. A losing same-table writer must
  revalidate or report a conflict; it cannot overwrite a successful commit.
  No post-publication head write is needed to mark the result complete.
  The server does not traverse manifests or perform per-file catalog GETs to
  prove the existence, age, or contents of files named by a commit. Clients
  prepare those files before the metadata pointer is published.
- **I3 — Minimal foreground KV.** Do not persist an application-level phase
  for an operation whose outcome and retry identity can be determined from
  the published record and Chunk-KV WAL. Do not write per-file GC candidates
  or age markers on upload. Every additional foreground KV write must have a
  documented correctness purpose that cannot be derived from existing durable
  state. A foreground mutation may GET only to validate catalog identity and
  authorization at entry, obtain the version of a genuinely shared resource
  it will conditionally update, or resolve a CAS conflict or uncertain
  response. GC may GET before reclaim. Reuse the entry observation and CAS
  conflict value; remove other normal-path `check_context`, journal, session,
  head, and post-publication GETs. Do not retain an additional GET by default:
  bring its concrete correctness case for a separate decision first.
- **I4 — Stable published files.** A published file remains available
  even if no table snapshot references it, unless the authorized client
  requests cleanup or a table drop explicitly requests purge. Neither a
  one-day age nor a missing table reference authorizes automatic deletion.
  GC may reclaim incomplete physical allocations, abandoned multipart fragments, and
  internal preparation that never produced a published file descriptor,
  after an age and ownership proof. Reclaim coordination occurs in the
  background, only when needed for deletion, not on every upload. Separately
  authorized S3 `DeleteObject` and `DeleteObjects` requests may remove a
  published file after scanning retained table references. The caller must
  guarantee that no in-flight or future commit will reference any requested
  path; the scan alone cannot enforce that promise. Normal FileIO
  credentials do not grant object deletion. A successful table drop with
  `purgeRequested=true` creates one durable, delayed purge task for the
  original table ID; an ordinary drop without purge keeps its files.
  Catalog retirement retains its existing separate cleanup path.
- **I5 — Explicit object semantics.** Table metadata files used by retained
  snapshots remain readable. Direct PUT and multipart completion publish a
  location only when its key does not already exist, using a conditional KV
  update without a preceding existence GET. A conflict cannot replace the
  published bytes; use the returned existing record to resolve an exact
  retry. The S3-facing failure and retry responses must state this enforced
  create-only contract, including its difference from an unconditional S3
  PUT.
- **I6 — Bounded resources without fixed serialization.** Limit actual
  buffered bytes, active chunk IO, and response memory with measured budgets;
  do not reject independent metadata requests solely because four other
  requests are active. Preserve backpressure when a real resource budget is
  exhausted.

Implement the following work as one measured requirement:

1. In `lib/crowdb-access-iceberg/src/commit/create/` and the relevant
   namespace lifecycle code, remove per-table `NamespaceAuthority`
   admission and cleanup writes. Let table-name ownership resolve same-name
   creates. Namespace drop may miss a concurrently creating table: do not add
   a cross-resource reservation solely to prevent that race. The later GC
   scan must find and process any leftover table or file state. A dropped
   namespace must not make an old in-flight table silently visible under a
   newly created namespace identity.
2. In `lib/crowdb-access-iceberg/src/commit/` and
   `app/crowdb-access-server/src/iceberg/table_write.rs`, make the final
   `TableHead` conditional update the sole commit point for a table update.
   Preserve operation identity and enough base-version evidence in existing
   published state to resolve a lost response without a separate steady-state
   phase journal. Remove redundant post-commit head settlement and reduce
   retry-ledger writes when the same result can be reconstructed. Keep
   bounded conflict handling for same-table writers and exact retry behavior
   for requests whose result is already known. Remove the server's deep
   snapshot, manifest, and data-file existence/format proof from the normal
   commit path. Validate the request's metadata update and its base table
   version in memory; publish the resulting metadata pointer once.
3. In `lib/crowdb-access-iceberg/src/file/` and the native FileIO endpoint,
   make one location-keyed record contain the complete published file
   descriptor. Direct exact-path reads then use one catalog key; table-scoped
   path-prefix scans can enumerate these same records without joining a
   separate file-ID index. Preserve integrity checks and snapshot-pinned
   bytes; publish with one `compare_exchange` expecting no location record,
   and resolve conflicts from the returned value without a fresh GET. Apply
   the same no-overwrite rule to multipart completion. Adapt GC and any
   file-ID lookup to the location-keyed scope. Native
   listing remains governed by R194's authorization and pagination decision.
   Audit `FileWriteIntent` against the actual chunk allocation WAL: remove or
   coalesce per-block KV intent writes only if that WAL or another existing
   durable record proves owner, location, and creation time for recovery and
   GC. A failed write must not produce an untraceable physical allocation.
4. In `lib/crowdb-access-iceberg/src/file/multipart_admission.rs` and
   `multipart_credits.rs`, remove catalog-wide reserve/release mutations.
   Bound each session and use physical capacity as the aggregate limit. Keep
   per-session terminal and retry semantics, with no global serialization of
   unrelated uploads.
5. In `lib/crowdb-access-iceberg/src/gc/` and
   `app/crowdb-access-server/src/iceberg/gc_runtime.rs`, remove automatic
   live-table orphan deletion of published files and the corresponding
   foreground deletion-claim/fence GETs in `file/repository.rs`. Keep a
   bounded, resumable scanner for incomplete physical allocations and
   abandoned multipart or internal preparation. Use creation time already
   present in durable ownership records and a configurable delay before
   reclaim; never treat absence from a table snapshot as sufficient reason
   to delete a published file. Prove an allocation has no published owner
   before physical deletion. Bound each scan step's KV and chunk IO,
   preserve progress across restarts, and expose scanned, deferred,
   reclaimed, and failure counts without per-object foreground writes. Keep
   ordinary FileIO credentials free of object-delete rights; multipart abort
   remains a separate operation for unpublished parts. Provide separately
   authorized S3 `DeleteObject` and `DeleteObjects` using the same cleanup
   engine. Batch requests use S3's per-key result format and accept at most
   1,000 exact keys. The caller asserts that no in-flight or future commit,
   including a staged or retried commit, will reference those paths. Group
   keys by table, scan all retained
   snapshot and metadata references under that table's selected head, and
   refuse each referenced or indeterminate key.
   For each proven-unreferenced key, conditionally change its location
   record to a deletion state before deferred physical reclamation; do not
   free blocks while an admitted reader may still use them. Missing keys
   receive S3-compatible success. A mixed batch reports individual failures
   in its response. The cleanup scan and deletion writes are charged only
   to the explicit cleanup request, never to a normal PUT, GET, or commit.
   Preserve Iceberg REST drop semantics: only `purgeRequested=true` creates
   a durable `TablePurgeTask` after the table head is tombstoned. Use the
   existing purge GC path to admit one task bound to that table ID and
   catalog activation. Begin its bounded cleanup work 20 minutes after the
   durable drop. Add the drop timestamp to the existing purge marker so
   delayed task admission does not restart the clock. Remove the
   seven-day floor specifically for `PurgeTable`; retain separate safety
   policy for incomplete-write and retired-catalog reclamation. Run the
   purge worker by default without enabling automatic live-table orphan
   deletion. Protect requests already admitted before the drop and defer
   physical ranges still owned by active multipart work; a valid old FileIO
   credential does not guarantee that a purged object remains readable after
   cleanup. After the delay and any required reader protection, reclaim all
   published and unpublished files, multipart remains, and associated table metadata in
   bounded, resumable pages without checking snapshot reachability inside
   the dropped table. A normal drop with `purgeRequested=false` creates no
   purge task and retains its underlying files. Same-name recreation uses a
   distinct table ID and must never be swept by the prior table's task.
6. In `app/crowdb-access-server/src/iceberg/body.rs`, `table_read.rs`, and
   `table_write.rs`, replace the fixed four-request `SpoolPermit` gate with a
   budget tied to the bytes or other resources it actually protects. Hold
   permits only while that resource is owned; do not keep a metadata request
   slot for the full response lifetime. In catalog, commit, file, and
   multipart repositories, pass the entry context and already selected
   records down the call chain. Remove repeated root, head, session, journal,
   reclamation, and successful-post-publication GETs outside I3's allowed
   cases. A root change after admission may leave inaccessible old-catalog
   preparation for GC; repeated root reads cannot make a cross-key
   publication atomic. Flag any read that appears indispensable beyond I3's
   list for user review before preserving it.
7. Extend the real-protocol workload and focused fault tests to report KV
   reads, writes, scans, conflicts, HTTP outcomes, throughput, and p50/p95/p99
   latency per upload, create, commit, and multipart phase. Compare matching
   small-cluster profiles before and after the change. No extra work is
   allowed on the common path merely to improve the observed 32-worker case.

#### Dependencies

- The [native Iceberg design](../design/access-server/iceberge/design-crowdb-iceberg.md)
  describes the existing authority, commit, and GC mechanisms. This
  requirement changes those mechanisms; reconcile the permanent design
  during implementation.
- R196 supplies a reusable protocol benchmark. If it is not yet implemented,
  use a focused `pixi run` TPC-H/TPC-DS loader and native HTTP workload with
  the same per-operation counters and retained logs; do not postpone the
  correctness and contention tests.
- R195 changes the shared upload transport. Measure publication and storage
  metadata separately from transfer so either implementation order remains
  diagnosable. R194's possible object listing must use the resulting file
  visibility rule and is not a prerequisite.
- The [Iceberg REST drop-table contract](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
  defaults `purgeRequested` to false. Preserve that distinction while
  scheduling delayed purge only for an explicit true request.
- Chunk-KV WAL and chunk allocation recovery are prerequisites for removing
  any per-block `FileWriteIntent`. Until the durable ownership proof is
  verified, retain those intents and report their measured cost.

#### Acceptance

- Given 32 concurrent TPC-H/TPC-DS table creates in one namespace and an
  equal workload spread across namespaces, run the small cluster; assert no
  namespace-key write occurs per table, independent tables make progress,
  and neither profile returns a contention-derived 503 (I1). E2E test.
- Given two same-name creates and two different-name creates, race them;
  assert one same-name winner, correct retry/conflict results, and no
  different-name serialization (I1). Integration test.
- Given namespace drop overlapping an in-flight table create, force the
  drop scan to miss that create; assert the drop completes without a new
  shared admission protocol, the stale table is not visible under a reused
  namespace identity, and a later scanner identifies its leftover state
  (I1, I4). Integration test.
- Given two writers based on one table head, commit different changes;
  assert one conditional head update succeeds, the other revalidates or
  conflicts, and no successful update is lost or followed by a settlement
  head write (I2). Integration test.
- Given an uploaded file referenced by a new snapshot, commit the table;
  assert the server does not traverse its manifest chain or perform
  per-file catalog GETs, writes no per-file committed status, and publishes
  only through the final table-head update (I2, I3). Integration test.
- Given a response lost around the final head update, retry with the same
  identity; when its generation remains current, the server may recognize
  success, while a later head that hides it may yield an uncertain result.
  Assert no retry silently reapplies the update and no permanent phase
  journal is written (I2, I3). Integration test.
- Given distinct-path direct uploads and multipart sessions, run concurrent
  PUT, part, completion, and retry requests; assert no catalog-wide credit
  write, correct exact-path outcomes, preserved referenced bytes, and bounded
  per-request GET/write counts (I1, I3, I5). E2E test.
- Given concurrent direct PUT and multipart completion for the same path,
  assert only one file descriptor is published by a location-key CAS, the
  losing request cannot replace its bytes, and an exact retry is decided from
  the CAS conflict value without a separate GET (I3, I5). Integration test.
- Given direct PUT and GET of a published file while background cleanup
  examines unrelated incomplete writes, assert neither request reads a GC
  claim or deletion fence, the file remains readable, and GC does not
  reclaim it for lack of table references (I3, I4). Integration test.
- Given S3 `DeleteObject` without a multipart upload ID and a normal FileIO
  credential, assert the native FileIO rejects the operation without changing
  the published file; multipart abort still reclaims only its unpublished
  parts (I4). Integration test.
- Given a separately authorized S3 `DeleteObject` for an unreferenced file
  and then a missing path, assert both use the batch cleanup's reference
  checks and S3-compatible single-object success responses; a retained
  snapshot reference or indeterminate scan must fail without deletion (I4).
  E2E test.
- Given a separately authorized S3 `DeleteObjects` request containing an
  unreferenced upload, a file referenced by a retained snapshot, an unknown
  key, and a key whose reference scan cannot finish, assert per-key success
  or failure, no deletion of the referenced or uncertain keys, and deferred
  physical reclamation of only the unreferenced upload (I4). E2E test.
- Given cleanup of one table alongside unrelated table commits and uploads,
  assert those normal paths perform no cleanup-related GET or write and do
  not wait on a shared cleanup key; the caller's no-future-reference promise
  is an explicit precondition for the selected table (I1, I3, I4). Integration
  test.
- Given a table drop with `purgeRequested=false`, drop the table and advance
  the GC clock; assert no table purge task is admitted and its published
  files are not automatically reclaimed (I4). Integration test.
- Given a table drop with `purgeRequested=true`, restart after the head
  tombstone and again during the GC scan; assert one durable task for the
  original table ID begins work no earlier than 20 minutes from the durable
  drop, even when task admission is delayed; protect admitted readers and
  defer active multipart ranges before reclaiming all table-scoped files in
  bounded pages without relying on snapshot reachability (I4). Integration
  test.
- Given a new table with the same namespace and name while the old table's
  delayed purge runs, assert the old task never reclaims the new table ID's
  records or physical blocks (I1, I4). Integration test.
- Given published files in one table, read an exact path and scan a path
  prefix; assert each file is represented by one complete location-keyed
  descriptor, the exact lookup needs one file-record GET, and the prefix
  scan needs no file-ID join. Preserve R194's separate listing authorization
  and pagination decision (I3, I5). Integration test.
- Given five or more concurrent independent table reads and writes, including
  a slow response consumer, assert requests proceed while actual byte and IO
  budgets are available, and only exhaustion of those budgets applies
  backpressure (I6). E2E test.
- Given a direct PUT, exact file GET, table create, and multipart part upload
  without conflicts, count and classify catalog operations per request;
  assert mutation-path GETs occur only for I3's approved purposes and no
  post-success root or journal reload remains. A concurrent catalog
  retirement may leave old-catalog orphan preparation, but cannot publish it
  into the new catalog identity (I3, I4, I6). Integration test.
- Given a crash or uncertain response at each chunk allocation and file
  publication boundary, restart and scan; assert every physical allocation
  is either referenced or recoverably owned, and no referenced block is
  reclaimed (I3, I4). Integration test.
- Given an uncommitted published file, a file in an older retained snapshot,
  a current file, an active multipart upload, and an unpublished physical
  allocation, advance time and scan in bounded pages; assert all published
  files remain readable, the active upload is protected, only proven
  incomplete physical storage is reclaimed, and progress survives restart
  within configured IO budgets (I4). Integration test.
- Given a file uploaded more than a day ago and referenced for the first
  time by a later table commit, assert it was not automatically deleted or
  rejected for age; the final table-head update is the only commit
  publication (I2, I4). Integration test.
- Given identical before/after cluster profiles at 1 and 32 clients, compare
  KV operation counts, conflicts, throughput, and p50/p95/p99 latency by
  phase; assert the common path uses fewer metadata operations and the
  report exposes any throughput or latency regression (I1–I3). E2E test.

- Given single-node one-mirror mode and 1 KiB, 512 KiB, and labelled
  65,502-byte payloads, receive each over multiple socket reads; assert one
  object owner handoff with respectively 1, 9, and 1 contiguous frames, correct
  checksums and late chunk ID assignment, no payload coalescing on native
  mirror success, and correct readback. Integration test.
- Given a shared chunk with small and multi-frame objects, drain queued work;
  assert multi-frame objects reuse remaining strip capacity, exact locations
  remain independent, and completion never exposes unreadable bytes.
  Integration test.
- Given default small-write configuration, fill half a strip reservation group;
  assert 256 MiB chunk capacity, a 32-strip initial prefetch, next-group prefetch
  at 16 remaining strips, and cancellation of unused reservations on retirement.
  Integration test.

Run `pixi run rs-fmt-check`, `pixi run rs-lint`, `pixi run test-access-iceberg`,
`pixi run test-access-server`, and the focused small-cluster Iceberg loader and
GC fault workloads through `pixi run` for the implemented scope.
