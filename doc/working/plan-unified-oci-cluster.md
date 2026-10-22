<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified OCI Cluster Implementation Plan and Handoff

Requirement: [R243](../backlog/R243-deploy-unified-oci-cluster.md).
Goal: ordinary OCI lifecycle, authenticated rediscovery, current layered
readiness, attached workloads and scoped cleanup.

## Current handoff

- **R243 remains incomplete.** Keep the requirement/backlog entry and outstanding
  acceptance work below. The sustained all-DiskIO recovery case now passes;
  this does not certify every runtime, protocol or formation/recovery acceptance.
- 2026-10-22: user authorized continuation past the prior retry limit and asked
  for diagnosis of the long-standing failure. User subsequently requested F03
  and explicitly authorized continuing other R243 tasks afterward, starting T04.
  User now explicitly requests completion of every remaining R243 task.
- [x] **F04 — Publish lease recovery of an unmaterialized split child**.
  The real Chunk-KV benchmark reproduced a directory/writer disagreement:
  the target reclaimed epoch 2, but a retained split marker plus tail overlay
  prevented publication from catalog epoch 1. Lease-expired recovery now accepts
  that exact source assignment with its complete artifact; epoch, range,
  readiness and release-proof validation remain intact. The focused test fails
  before the fix with `range catalog transition identity was reused` and all
  14 domain-monitor tests pass afterward. Real native run
  `.crowdb-runtime/artifacts/r243-bench-chunk-kv-6/` passes 30,000 mixed requests
  with zero errors, split/balance, unchanged restart bounds, recovered readiness
  and retained-byte reads. Gate logs: `r243-overlay-transfer-{before,after}.log`,
  `r243-bench-chunk-kv-6.log`, `r243-storage-clippy-3.log` below the T29b gates root.
  Files: KV-server `background/domain_monitor/chunk_kv/catalog.rs` and
  `tests/common/catalog_publication.rs`.
- [x] **F05 — Grow the successor after stream rollover**.
  A request larger than the initial 1 MiB mirror strip worked in the first
  chunk but failed after rollover with `new chunk cannot fit the append`.
  Extracted allocation/growth/rotation into `stream/capacity.rs`; the successor
  now grows before admission, retaining its chunk limit and publication fences.
  The new `rollover_growth_test` fails before the fix and verifies both chunks
  through a cold read-only handle afterward. All 52 stream tests pass; fmt and
  all-target storage clippy pass. Real MemDisk random-read/replay/GC workloads
  now complete with zero errors and no watchdogs in
  `.crowdb-runtime/artifacts/r243-bench-chunk-stream-2/`; the unchanged old
  performance/metadata assertions still fail and are not reported as a pass.
- Branch `task-stable`; local commits only, no push authorized.
  Earlier storage fixes: `532af33e`; common CLI fixture: `be0efd32`.
- Failed fixture data/logs and earlier retained J/L data remain preserved.
  All test-owned containers and labeled bridges are cleaned; successful
  disposable case volumes were deleted by the independent owned cleanup helper.
- Preserve user edits in `container/single-node-container/dev-container.sh` and
  `e2e-test/clients/ecosystem/README.md`; exclude them from requirement commits.
- No compatibility requirement. RDMA uses host networking.
- [x] **F03 — Separate background health sampling from status reads** (A15/A26/A27).
  User requires backend-owned health snapshots: frontend/business request failures
  cannot mutate global health; querying health must not trigger dependency I/O.
  One Access worker samples every second with an 800 ms bound and publishes an
  immutable snapshot through ArcSwap. Successful observations replace failures;
  after three seconds evidence expires to Unknown, never automatically Ready.
  Stop remains an immediate lifecycle fence. Worker lifetime follows the listener;
  concurrent worker starts are fenced without locks.
  Web protocol reads and service diagnostics also move to one independent backend
  sampler; node-observation reads only return its current snapshot, preserving
  original timestamps and operation IDs. Aggregate current-intent fencing remains.
  Files: Access `s3/health{,/observation}.rs`, dispatcher/main wiring;
  shared S3 health enum; Web node worker/state/main and Access diagnostics;
  Access/Web health cache tests and design.
  Unit/integration: query bursts add zero probes, independent recovery without
  reads, one worker/in-flight probe, expiry/pending Unknown, stop and teardown.
  Verified: 23 affected Rust integration tests, 159 Python fixture tests,
  Rust fmt and Access/S3/Web all-target clippy pass. Registered sustained
  all-DiskIO fault passed on image S: independent cached health recovered in
  5.13 seconds, retained old/new reads passed on all members without Access
  restart, and explicit DiskIO stop/restart correctly fenced cached evidence.
  The final fixture status is pass; independent owned cleanup completed.
- [x] **F02 — Verify current Access readiness after backend recovery** (A15/A23/A26/A27).
  Independent dedicated endpoint samples confirmed stale metadata/chunks
  Unavailable after backend recovery; listener/authentication/native-pool/cleanup
  were Ready and backlog was zero. Only the Access serving failed requests stuck.
  `record_dependency_outcome` latched request failures; Web skipped protocol
  reads until Access was already Ready. A ListBuckets read could not reset the
  chunk flag even if the Web gate were removed.
  Removed request-derived storage health. Each dedicated health response now
  reads the routed metadata namespace and ChunkDB/physical DiskIO independently,
  with separate 750 ms deadlines and an 800 ms outer bound. Results are local to
  that response. Credential freshness, listener/live state and cleanup backlog
  retain independent admission checks. Web records exact flags/read failures
  even for HTTP 503, while stop/delete and operation ownership gates stay intact.
  No new locks, mutation/probe objects, forced Ready or Access restarts.
  Files: Access `s3/{dependencies,health,operations,dispatcher}.rs`, `main.rs`;
  S3 `auth{,/sigv4,/snapshot}.rs`; chunk client `client/readiness.rs`;
  Web `node/observation/services{,/access}.rs`; E2E
  `console_cli_{fixture,recovery}.py` and `scenarios.py`.
  Focused Access readiness/listener/dispatch, credential cache, SigV4, S3 metrics
  and Web observation/health tests pass (22 unique tests). Four affected crates
  pass all-target clippy; Rust fmt and 159 Python fixture tests pass.
  New registered case `console::cli_container::terminal_chunk_writer_recovery`
  passed on image R: all three paused DiskIOs produced non-ready Access evidence;
  after resuming, cluster check recovered in 8.13 seconds within the unchanged
  180-second budget. One terminal ChunkKV writer replayed; all Access PIDs,
  cluster UUID, accepted inputs and container IDs stayed fixed. Old/new object
  reads passed on all three members. Both existing CLI interruption/lifecycle
  cases also passed, including explicit DiskIO stop and current readiness after
  restart. CI inventory verifies 389 named flows, not their execution.

## Previous retry-limit handoff

The requirement workflow requires confirmation after five source-driven failed
end-to-end recovery rounds. Harness setup failures are excluded from this count.

- M: DiskDB restart succeeded; terminal ChunkKV handles never replayed.
- N: writer restart missed retained split compatibility handles.
- O: restart exposed competing dead-owner transfer fencing an active split.
- P: transfer guard exposed false unavailable markers on acknowledged WAL prefixes.
- Q: false markers removed; real writer restart/replay and owned ranges recovered,
  but Access remained Initializing and aggregate check exited 1 after 180 seconds.
- Failing case: `console::cli_container::terminal_chunk_writer_recovery`.
  Command: `pixi run python .crowdb-runtime/artifacts/r243-q-chunk-kv-recovery/run.py`.
  Inner failure: `cluster check --wait 180 --json`, phase `degraded`,
  `required-service-not-ready` for `node/3/service/access-3` and all its protocols.
- Recommended continuation: F02 active read-only Access dependency checks, with
  exact diagnostics and a recovery regression. Traffic-driven recovery or clearing
  health flags without fresh evidence would hide the current readiness defect.
- User confirmed continuation on 2026-10-22. The F02 registered sustained-fault
  case passed; prior rounds remain historical diagnosis, not current blockers.

## Remaining implementation tasks

Only mark tasks complete after their acceptance is registered and verified.
Existing implementation can be reused; these are the outstanding parts.

- [x] **T04 — Finish common target coverage** (A14/A28).
  Named/endpoint-only UUID-checked targeting and native signed S3/workloads work.
  All retained hardware/KV/S3/workload commands now require a selected target
  before dispatch. Their Group-0 connection has no system-address fallback and
  never loads a launch registry. Remaining process executors use an explicitly
  separate legacy context pending T29; they cannot accept attached targets.
  The 15-family negative matrix covers missing selection with a listening
  legacy endpoint, conflicting UUIDs, unreachable management and native RPC.
  - [x] T04a: common dispatch/connection coverage and registered plain-Docker
    three-node attachment. Saved and endpoint-only targets, native KV/S3 reads
    on every node, fixed processes/containers, no runtime receipt and rejected
    endpoint-only stop pass. The fixture declares reachable public protocol URIs.
    32 affected CLI tests, all-target clippy, fmt and 159 fixture tests pass;
    CI inventory verifies 390 named flows (execution is separate).
  - [x] T04b: retained CLI E2E callers now run against normal admitted Docker
    nodes with explicit targets. Observation (including topology and stale local
    state), cross-member hardware mutations, store/group lifecycle and actual
    data put/get/delete/scan passed as registered container cases. Removed
    registry-specific/retired process-path cases; their remaining public-surface
    contract is rejection before runtime access. Shared bootstrap/supervisor
    correctness remains independently covered.
  Owners: CLI `commands/cluster/target.rs`, `commands.rs`, shared deployment target.
- [ ] **T05–T07 — Finish formation negatives and interruption coverage**
  (A03–A05/A19–A21). One/three-node normal startup works. Verify missing/extra/
  duplicate/foreign peers, incompatible credentials/policies, simultaneous
  coordinators, sealed-manifest conflicts and lost replies without rebootstrap.
  A concrete diagnostic gap found during T04 fixture setup: invalid discovery
  seed URLs reuse `NodeManagementError::PhysicalHost`, incorrectly reporting
  physical-host identity. Split the diagnostic and validate seeds before node
  effects; include malformed/credential-bearing seeds without leaking values.
  Owners: Web auto-join/initialization, shared bootstrap/admission, monitor tests.
  - [x] T05a: seed validation now has a dedicated diagnostic and runs before
    policy acceptance, identity creation or listener/service effects through
    `accept-policy`, `run`, and the standalone management entry point. Tests:
    `node_management_inputs_test`, `node_management_test`, `node_run_inputs_test`;
    12 focused tests pass, as do fmt and all-target clippy. Gate logs are under
    `.crowdb-runtime/artifacts/r243-t29b-gates/` (`r243-monitor-inputs-tests.log`,
    `r243-final-fmt.log`, `r243-final-clippy.log`). Files: monitor `node/management.rs`,
    `node/runtime.rs`, `node_args.rs`, `tests/node_management_inputs_test.rs`.
- [ ] **T15 — Complete editable startup acceptance** (A01/A02).
  Management starts while setup is pending. Migrate remaining readonly/legacy
  callers; verify authorized edits and actual rejection causes in both topologies.
- [ ] **T15a–T15d — Finish member trust and retained recovery** (A32/A33).
  Initial authenticated member pins, SSH-verified rediscovery, duplicate live
  identity rejection and identity-bound replica routing are implemented. Fresh
  three-node Docker changed-IP recovery with retained S3 bytes passed.
  **Gap:** later non-voter admission/revocation trust distribution and non-voter
  changed-IP recovery. Current trust file is immutable initial membership;
  `recovery::accept/check/ready` special-case admitted nodes and discovery loads
  the initial PreparedBootstrap. Extend only through confirmed Group-0 authority
  and admission operation/host-key pins, never discovery TOFU. NodeRegistry does
  not currently store the authenticated host key. Preserve retirement fencing.
  Verify wrong key/bootstrap, cancellation, copied IDs and missing/corrupt trust;
  add actual S3/Iceberg/Dataset and host-network recovery coverage.
  Owners: shared `deployment/recovery*`, `registry.rs`, Web `node/admission.rs`,
  monitor `node/runtime/{admission,recovery}*`, KV identity-route tests.
- [ ] **T16 — Complete current dependency acceptance** (A15/A23/A26/A27).
  Group0/disk+binding → owner → space layers, current operations, stateless idle
  ChunkKV and owned-range writer readiness exist. Finish service stop, voter loss,
  stale evidence, mixed failures and bounded endpoint checks with non-repair proofs.
  F01 addresses an uncertainty/readiness gap; do not conflate free bytes with
  allocatability. Preserve detailed successful facts as well as errors.
- [ ] **T19–T20 — Add common CLI host/containerd lifecycle** (A08–A10/A29/A31).
  Docker bridge lifecycle/ownership receipts work. Common CLI still lacks host
  and containerd adapters. Reuse production resource/device/port/interface/
  credential validation from `container/oci-image/{run-node.py,runtime.py,storage.py}`.
  Seal daemon/context/endpoint, exact resources/mounts/image and owner before
  effects; namespace is a backend detail, never application cluster identity.
  Verify applied cgroups/host network and one node per physical host.
- [ ] **T25 — Register direct Docker/Compose reference acceptance** (A01/A03/A25).
  Examples/helpers exist. Execute documented one/three-node commands and Compose
  without host bootstrap helpers, with published access to all three protocols.
- [~] **T29 — Finish removing legacy public paths and migrate their callers**
  (A10/A12–A14/A17).
  - [x] T29a: remove public `launch`, `--registry`, system-address fallback,
    `cluster init/local-deploy/destroy/clean` and `kv server`, including dead CLI
    executors. Migrate retained observation, hardware, logical topology and KV
    data semantics to normal three-node Docker acceptance. Full CLI tests pass
    (38 executed, one separate pending-free test ignored), as do fmt, all-target
    clippy (CLI and E2E clients), E2E test compilation, 159 fixture tests and
    CI ownership checks (381 named flows). Five
    registered real Docker cases passed with final timings and scoped cleanup.
    Retired process-path
    assertions become command-tree rejection tests; shared bootstrap and
    supervisor correctness tests remain independent owners.
  - [ ] T29b: migrate profiling/benchmark/sanitizer fixture ownership and flags.
    Current implementation uses a `test-util` native example plus private
    `tools/benchmark/fixture.py` controller. The worker owns exact PID/start
    identities, seals genuine Group-0 bootstrap, and supplies a target to normal
    product clients. Server tunables, memory backends and group reset stay in
    this explicit measurement fixture. Benchmark/profiling/sanitizer callers
    no longer invoke retired product lifecycle commands. Console CI explicitly
    builds and runs its native ownership/workload smoke.
    - Native ownership/read/scan/write/reset, foreign token rejection, Group-0
      reset rejection, stale-report rejection and owner-exit cleanup pass:
      `.crowdb-runtime/artifacts/measurement-fixture/59b0c23a793f4f92a65d1f05595e681b/`.
    - KV read/scan/write scripts pass reduced-duration measurements; DiskDB
      allocate/mix and ChunkDB 1T/16T pass with exact space accounting and
      retained per-invocation metrics. Roots: `r243-bench-kv-read-3`,
      `r243-bench-kv-scan`, `r243-bench-kv-write`, `r243-bench-diskdb-2`,
      `r243-bench-chunkdb` below `.crowdb-runtime/artifacts/`.
    - Root causes fixed during validation: stale `--json` and DiskDB `--mode`
      flags, swallowed command errors, old log globs, and bootstrap replacing
      process-local PID fields. The first ownership smoke caught leaked native
      services; their exact PID/start/path-verified cleanup receipt is retained
      at `.crowdb-runtime/artifacts/r243-regression-pre-fix-cleanup.json`.
      Earlier measurement successes do not certify cleanup before that fix.
    - Full sanitizer attempt found retained-byte reads incorrectly using
      NullDisk and a real lifecycle handle leak: ChunkDB/DiskIO/restart used
      `mem::forget(child)`, retaining Tokio pidfd registrations after exit.
      These paths now retain a reaper task; the fixture waits for reaping and
      publishes a checked shutdown receipt. Read verification gets a separate
      memory-backed fixture. The four focused lifecycle/reaping tests pass.
      This shared lifecycle fix changes server sources; image S cannot certify
      the resulting production source tree and must be rebuilt for later OCI
      acceptance. Failed sanitizer logs/data are retained, with no suppression.
    - CLI tests (38 executed plus one distinct ignored case), CLI/example clippy
      and 166 Python fixture tests pass. Refreshed gates and the coherent task
      commit remain pending.
    - Full Console component execution now passes 389 Rust tests (one distinct
      ignored case) plus the registered native ownership smoke. Full sanitizer
      run `sanitize-regression-20261022-084404-1796681` passes every workload,
      service/worker shutdown and normal-binary restoration. The final run at
      `sanitize-regression-20261022-091147-1894480` also passes every case with
      protected placement and explicit EC 8+4 reads suitable for three logical
      nodes. Production placement checks were retained; the old default EC 4+1
      read shape requires more independent nodes and was correctly rejected.
      Actual perf sampling passes with 5,055 samples and retained `perf.data`/
      `perf-script.txt`; optional inferno SVG generation was unavailable.
      Gate logs: `.crowdb-runtime/artifacts/r243-t29b-gates/`.
    - Remaining script checks found release fixtures selecting obsolete
      `test_unsafe_placement`, a Pixi wrapper PID being mistaken for the
      Chunk-KV service PID, and chunk-stream's implicit localhost seed. The
      fixture keeps three logical native nodes with normal protected placement
      for retained EC measurements; it does not qualify physical-host tolerance.
      Chunk-KV now tracks native child PID/start directly and no longer repeats
      failed startup/restart blindly. Chunk-stream receives the live fixture's
      explicit seed; its benchmark parser rejects a missing seed. Native read/
      replay fixtures retain bytes through MemDisk. The final 169 Python tests,
      five directly affected crates' all-target clippy, fmt, 17 shell syntax
      checks and the 381-flow CI inventory pass. Inventory is not runtime proof.
    - Actual ChunkIO read-small/read-large/read-mix pass, with retained-byte
      verification and exact request counts, under `r243-bench-chunkio-read`.
      Both 20-second small-write cases (1T/32T) pass under
      `r243-bench-chunkio-small-2`, including parity and scale-out assertions.
      The one-second rehearsal correctly failed to produce parity at 1T; the
      default duration and every assertion remain unchanged. Two concurrent
      script builds conflicted in a shared CMake archive; subsequent builds
      are serialized. Large writes also exposed misplaced `grep --include`
      options scanning executable strings instead of logs. The shared scanner
      now checks only log files, prints real errors and rejects scan failures;
      three new tests cover those boundaries. The large-write rerun passes
      both stream/direct modes with original 20-second duration, zero errors,
      exact accounting and complete metrics under `r243-bench-chunkio-write-4`.
      Fixture shutdown receipts confirm successful scoped child cleanup.
    - F04/F05 above repair the actual split-transfer restart and stream successor
      failures discovered through the migrated fixture. The Chunk-KV script
      now passes; stream random-read/replay/GC complete with zero data errors,
      but the original performance script remains red. See the contract audit
      below; no metadata, cache, throughput or latency threshold was relaxed.
- [ ] **T30–T33 — Complete registered acceptance ownership and CI**.
  Register A01–A33 in owning Rust/Python/container/UI suites; mocks and manual
  observations do not substitute for actual runtime coverage. Direct/Compose,
  isolated names, missing peers, interruption/lost replies, changed UUID/context/
  ownership, unrelated/shared resources, scoped cleanup, all attached workloads,
  current health/quorum and actual authorization need traceable owners.
  Wire the ignored `cluster_workloads_test.rs` pending-free accounting regression
  explicitly using `CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT`; default ignored
  execution is not coverage. `test-container-e2e-fixture` currently has 159 cases;
  the case inventory checker is not evidence that container flows ran.
- [ ] **T34 — Verify host runtimes honestly** (A08/A09/A31).
  Docker-hosted containerd profiles are separate from native Docker-free A31.
  Native rootful containerd is unavailable locally: socket root:root 0660;
  `sudo -n` requires a password. nerdctl 2.4.1 executes, which proves only tool
  availability. Provide an actual Docker-free native runner and record artifact
  digest/runtime-local image ID, hosts and results; do not count skips as passes.
- [ ] **T35 — Final documentation/gates/cleanup** (A17/all acceptance IDs).
  Update deployment/OCI designs, help, examples, credentials/ports, tool guides
  and CI only for verified behavior. Old standard/single/bare-metal registry
  descriptions still need migration. Run applicable full gates, then remove
  requirement, backlog row and this temporary plan in the final cleanup commit.

## Stream contract audit

- User requested confirmation through tests before changing performance gates.
  The earlier proposed consistency-versus-baseline choice is withdrawn: absence
  of per-append Stream-directory publication does not imply missing durability.
  Keep the original performance assertions and do not rebaseline to hide failures.
- Two metadata boundaries must remain distinct. Every acknowledged append still
  durably advances the fenced ChunkDB cursor. The old raw-byte Stream directory
  was published on allocation/rollover; recovery used that cursor to reconstruct
  the active tail. Historical `first_append_has_no_timer_and_skips_metadata_consensus`
  asserted two publications (create plus allocation), while
  `reopen_recovers_the_durable_active_tail` verified retained bytes after reopen.
- Commit `576d3c36b` introduced public Stream frames and per-frame extent mapping,
  replaced the local manifest snapshot update with `publish_state` on every
  append batch, and changed the first-append test to expect three publications.
  Commit `d166a2bf6` still included the old `publications <= rollovers + 1` gate.
  These are a representation/performance regression and an inconsistent test
  contract, not evidence that the old no-directory-write design lost data.
- Added `durable_cursor_recovery_test` with three independent boundaries:
  valid framed data with a committed cursor recovers without another directory
  publication; bytes beyond the durable cursor remain invisible; corrupt framed
  bytes inside the committed cursor are rejected. Old writers remain fenced.
  All 55 current stream tests pass. All eight NullDisk/MemDisk C++ tests pass,
  including deterministic synthetic reads and successful discard writes.
- The current NullDisk generates disk-ID/offset PRNG bytes, not valid Stream
  frames with matching framing/trailer. This does not disqualify patterned
  NullDisk benchmarking; that fixture needs a pattern matching the framed
  protocol. Retained-byte recovery tests still use memory-backed storage.
- Evidence and a reproducible same-binary NullDisk/MemDisk comparison runner:
  `.crowdb-runtime/artifacts/r243-stream-contract-audit/`. The initial comparison
  overlapped a cold clippy dependency build and is diagnostic only. The isolated
  comparison uses the same release binary and parameters, ten seconds per write
  case, after compilation completed: NullDisk/MemDisk 1T 140.07/131.58 ops/s,
  32T 1019.88/1007.33 ops/s, all zero errors. These samples do not establish a
  new baseline or attribute the main regression to MemDisk. At 1T, 1401/1317
  append batches publish 1407/1323 directories (including allocation/growth).
  Raw-pattern NullDisk Stream reads fail with `LayoutExpired`; MemDisk completes
  all 32 requested reads. The synthetic framed-data fixture remains unfinished.
  Exact binary hash, parameters, operation counts, publications and successful
  scoped shutdown receipts are retained. All-target stream clippy and fmt pass;
  no production behavior or performance threshold changed.
- T29b remains unfinished while its strict stream performance checks fail;
  address the append directory publication cost and framed synthetic-data
  fixture rather than asking the user to trade correctness for performance.
  Native Docker-free A31 remains an external-runner dependency under T34.

## Completed foundation (brief)

- Storage fixes: uncertain DiskDB gates stay closed until a new fenced process
  reconstructs allocation state; diagnostics distinguish Unavailable/NoSpace.
  ChunkKV restarts for terminal uncertainty, including retained split writers.
  Active splits exclude competing dead-owner transfers. Mirror repair exhaustion
  preserves acknowledged prefix locations; ordinary I/O errors never become
  corruption markers. Reader CRC and corrupt-replica protection remain unchanged.
- CLI recovery fixture uses common start/stop, fixed accepted state and containers,
  signed retained S3 I/O, read-only checks and independent scoped cleanup.

- **T01–T03, T08–T14:** durable startup identity/policy, default storage and metadata
  setup, monitor-owned services, access listeners and credentials.
- **T17–T18:** layered current readiness; bounded JSON status/check and exit codes.
- **T21–T24:** six OCI shortcuts; independent Docker stop/clean preserves data by
  default, removes unused owned bridges and handles owned image garbage.
- **T26–T28:** common Docker CLI lifecycle, native signed S3, attached workloads
  and separate RPC tooling. Pending-free accounting correction is verified.
- Preserve data safety: never truncate below acknowledged cursor; distinguish
  missing/superseded execution; retain exact monitor observation failures.

## Verification and retained resources

- Current F03 image: `crowdb-node:r243-s`; archive `target/crowdb-r243-s.oci.tar`.
  Source tree `f7e0b5d52ead45c8ae5047e0d8739bf7ef9c8326` at `14b117df`;
  server source identity verified before the registered run.
  OCI `sha256:e8391da241365b4978ff90adcc7ffec93c185539c63b64886ebd3930a6721fca`;
  Docker `sha256:1fb4a4cdb532cc11ef5dc5c15859c56408bcd5b6c499e08907931e80880a1056`.
  Archive SHA256 `43146f2464e1e100c6e74df9591e5d8dc65a8e3d40288fd379e523f64b814039`.
- F03 evidence: `.crowdb-runtime/artifacts/r243-s-registered/`.
  Registered `console::cli_container::terminal_chunk_writer_recovery` passed
  through `scenarios.run_cases` on local Docker Linux bridge, three one-CPU
  containers, prebuilt `target/debug` common CLI bundle. Exact fixture
  `crowdb-e2e-6bdd60ea399742bcbad80e10ea141d04`, cluster UUID
  `1776530c-327e-45c8-a083-c2251e722eee`. Recovery took 5.13 seconds after
  the sustained outage; one terminal ChunkKV writer replayed, all Access PIDs
  remained fixed, and old/new object reads passed on all three members.
  Explicit operator stop/restart also passed against cached observations.
  Final timing is pass (140.18 seconds including owned cleanup); containers,
  bridge and successful disposable volumes were removed.

- T04b/T29a evidence: `.crowdb-runtime/artifacts/r243-t29-registered/`,
  image S with verified server-source identity and current rebuilt host CLI.
  Registered cases all passed on local Docker Linux bridge with three nodes:
  observation `afcfbd7963cc4e21b4aa26207b4a0263` (57.07 s), hardware
  `d1a44ab68c4546c8bddc68a3b8cdd94a` (56.38 s), logical topology
  `5be96b75349541ac85cc7b322d22000d` (52.65 s), data
  `d396adef22f6416293d6bf4a96e56ac1` (53.49 s), attachment
  `ea37195fc2e34324ac813dcb304c7e67` (53.87 s); each ID has prefix
  `crowdb-e2e-`. Operation/attachment proofs preserve cluster UUID and runtime
  process/container inventory. All owned containers/bridges were removed;
  bind-mounted data and logs remain in the evidence tree.
- T04a evidence: `.crowdb-runtime/artifacts/r243-t04-registered-5/`.
  Registered `console::cli_container::plain_docker_target_attachment` passed on
  image S after server-source verification. Direct Docker startup (no CLI
  deployment) configured three local nodes, discovery HTTP origins, the actual
  Docker physical-host ID and reachable S3/Iceberg/Dataset public URIs.
  Exact fixture `crowdb-e2e-68137577a49041d1a1c6c493a216719a`, cluster UUID
  `2a9b695e-a927-4205-845d-4ff5ae97deca`; final status pass in 55.63 seconds.
  `attachment-proof.json` records three KV and three S3 reads, unchanged container
  IDs/image IDs/service PIDs, and absence of a runtime ownership receipt.
  Containers and the owned bridge were removed; bind-mounted evidence is retained.
  Earlier attempts remain under `r243-t04-registered{,-2,-3,-4}`: pre-created
  logs violated fresh-root admission; malformed discovery URLs were mislabeled
  as a physical-host error; missing public URIs advertised container localhost.
  These fixture input defects were corrected without changing startup checks,
  adding retries or extending deadlines. Failed node data was retained.
- Previous F02 image: `crowdb-node:r243-r`; archive `target/crowdb-r243-r.oci.tar`.
  Source tree `67269b7a179cfa4ddc8aaa12a740ac3155f29f1f` at `00b3fe00`;
  server source identity verified before running the registered cases.
  OCI `sha256:273e479c530e3fc31b4c7ec7ab08ff92360433d6d2a26e582d679d9aa4604ad1`;
  Docker `sha256:f11c3022d57cb530de351addac125e29d52e9a242562e4b0e6de2bf91c1617b4`.
  Archive SHA256 `ab647f26b6081876bd1d781c28a28068c3c45edea2314f5861f8b1fefd47d92f`.
- Previous F02 evidence: `.crowdb-runtime/artifacts/r243-r-registered/`. Exact registered
  `console.cli.container` cases execute through `scenarios.run_cases`, using a
  prebuilt common CLI bundle in `target/debug` (not the full RPC client bundle).
  Docker Linux bridge on this local Linux host; three one-CPU node containers.
  Passed sustained-fault case: `crowdb-e2e-dac91c8b37134f26a8d8701926d039f6`,
  UUID `7a1243a0-74e8-4704-b78a-a8d9875d1d25`. See `recovery-proof.json`,
  `access-health-{paused,checked}.json` and writer replay logs.
  Group-0 replay: `crowdb-e2e-8017473e1bee442d90e4b55c0cbc059b`; confirmed
  storage replay: `crowdb-e2e-da26f7bfa5804a21a8db87be20267cd1`. All three
  registered cases passed with final timings and independent scoped cleanup.

- New baseline: `.crowdb-runtime/artifacts/r243-r-baseline/`, valid case
  `crowdb-e2e-1b8e33246fe64817b1b6c76254c2d54a`. All three storage layers
  and owned ranges recovered, but Access-2 remained Initializing at the deadline.
  The CLI final sample also reported a transient management observation failure.
  Independent `health-correct-address-*` and `health-late-*` confirm stale
  metadata/chunk flags on container 1 (bound node 2). Health binds the node IP,
  not loopback; initial loopback-only sampling was corrected independently.
- Passed: full DiskDB, ChunkKV server, KV server, Chunk client and stream suites;
  DiskDB process-session tests;
  focused uncertainty, actual Unavailable/NoSpace RPC and post-split compatibility
  writer and acknowledged-prefix regressions; five affected packages pass all-target
  clippy and fmt; 159 Python fixture tests.
- Image N: both registered `console.cli.container` interruption/lifecycle cases
  passed. Image Q leader-pause case passed: all three DiskDBs restarted, UUID and
  containers stayed fixed, old/new S3 reads passed on every member, and a strict
  normal DiskDB mix passed. Evidence case: `crowdb-e2e-9df33e1979fb4b7aae178f91ca93cae6`.
- Previous image: `crowdb-node:r243-q`; archive `target/crowdb-r243-q.oci.tar`.
  Source tree `e9cc4a3f8d31cefbbf2da7e447fb4ab2015ce289` at `eddecc03`;
  production files match fix commit `532af33e` (verified by tree diff);
  OCI `sha256:aa00679345d7ab4fc4a735177d5963735ff152f356b388e156217841e0b540ea`;
  Docker `sha256:5937f17bd9b83d8d4efc90d8beaaee8240dafc7fe31b0c278f108f6846b719a9`.
- Previous evidence: `.crowdb-runtime/artifacts/r243-q-uncertain-recovery/` and
  `r243-q-chunk-kv-recovery/`. Each contains `run.py` and per-case logs; the passed
  DiskDB case has `recovery-proof.json`. Valid failed Access case:
  `crowdb-e2e-2bcc4a08714c4c35acdd5b0d132cb108`, cluster UUID
  `385fe4c4-3129-4474-97be-2bb78772cc24`. Its final observations show catalog
  generation 23 and all eight owned ranges Ready; `access-3` remains Initializing.
  It uses executable-checked SIGSTOP/CONT as user `crowdb`, verifies state `T`,
  and holds all three DiskIO processes for 40 seconds, restoring them in `finally`.
  These earlier explicit fault scripts are manual verification. The sustained
  all-DiskIO case is now registered and passed on image R; other sustained fault
  variants remain T31/T33 work.
- Registered CLI selection: `console.cli.container` (three cases). Full `console.cli`
  requires the regular client bundle including `domain-test`; an ad-hoc
  `target/debug` bundle supports only the migrated container CLI cases.
- Earlier diagnostics remain under `r243-m-*`, `r243-n-*`, `r243-l-address-recovery`
  and `r243-l-current-health`. They are evidence only, not the current fix image.
- Retained L volumes/receipts: `r243-l-one`, port 21600, UUID
  `1d0f87cb-164d-4c22-8905-4fbcff9cacce`; `r243-l-three`, ports 21700/21710/21720,
  UUID `fa9b326e-7d0e-4173-83d6-ddaacd0fdb2f`. Containers and bridges cleaned.
  Acknowledged `r243-recovery/payload.bin` is 2 MiB with SHA256
  `91d3beb88a9b2f778a6c44a1c53b63d3c79931845a9aef84b3fb414610bd1938`.
- Failed fixture volumes and receipts remain under each case's
  `nodes/runtime/persistent/oci-clusters/`. Use that runtime root with independent
  `docker-local.py clean`; never fabricate receipts or broadly prune Docker.
  Preserve older J data/receipts too. Successful disposable fixtures delete their
  own data and containers/bridges through the independent helper.

## Open questions

- No unresolved product choice; retry-limit continuation was authorized.
