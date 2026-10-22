<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified OCI Cluster Implementation Plan and Handoff

Requirement: [R243](../backlog/R243-deploy-unified-oci-cluster.md).
Goal: ordinary OCI lifecycle, authenticated rediscovery, current layered
readiness, attached workloads and scoped cleanup.

## Current handoff

- Latest verification: image AB manifest
  `sha256:015ec004d8a565dd74e432bfc812d3e16f0ce745bbe65a0de46dddb6fd692d7b`
  (server revision `ba50e29b`) passes actual four-member Docker-free native
  cold recovery. All former management/SSH/KV endpoints are unreachable;
  hosts change from `.11–.14` to `.21–.24`, start in order 4,3,1,2, and retain
  their original three-voter manifest, identities, pinned keys and credentials.
  One voter plus the confirmed nonvoter cannot recover authority; two original
  voters recover authority while the missing member remains degraded. All
  three serving nodes read the original S3 bytes, Iceberg commit properties and
  Dataset snapshot/manifest. Evidence: `r243-native-kvm/`
  `node{1,2,3,4}-cold-recovery-ab.json`, `cold-protocol-proof-ab.json`,
  `one-voter-nonvoter-negative-ab.json`. All four trust containers are stopped
  with preserved volumes. Registered changed-address and public host-profile
  qualification remain outstanding; the native proof is not their substitute.
- User requests completing R243 first, then implementing the already-authored
  R244 design and plan. Preserve those concurrent document edits until that work.
- Actual Docker CPU inspection reports `NanoCpus` with zero period/quota.
  Host ownership now accepts either representation and rejects contradictory
  limits. Runtime logs retain both output streams so entrypoint rejection is
  visible. All 61 OCI helper tests, CLI all-target clippy, Rust fmt and CLI build
  pass; actual Docker host qualification remains pending.

- **R243 remains incomplete.** Keep the requirement/backlog entry and outstanding
  acceptance work below. The sustained all-DiskIO recovery case now passes;
  this does not certify every runtime, protocol or formation/recovery acceptance.
- Latest continuation: image X full `console.cli.container` scope passes **all
  nine cases**, including both interruption/restart checkpoints, terminal-writer
  recovery, plain Docker attachment, observations, hardware/logical/data and
  explicit pending-free accounting. Evidence:
  `r243-deployment-continuation/cli-container-x/run-c118f264580c44b98592477f10169933/`.
  Image X manifest `sha256:21926e0868bf3282c925bb04890e24ba9e7e42a807bc8216d899c746a2883cdb`,
  Docker config `sha256:64a74415871503d19d34e7647d3b9b310f34abe469963d1211620e8a90b5eb7f`.
  The shared/Web extended gate passes 353 tests. This resolves the observed
  image-W four failures without deadline, assertion or restart-budget changes.
- Native containerd exposes a separate sustained-runtime bug: monitor RPC pings
  add about two established socket descriptors per second, eventually hitting
  its inherited soft nofile=1024. The probe drops/explicitly closes the safe
  connection, but idle socket cleanup awaits an I/O event that never arrives.
  Failure reproduces in a standalone regression: 128 completed pings retain
  527 FDs vs baseline 19 while the same executor remains alive. Worker-owned
  atomic close queues now retain connection identity and wake descriptor cleanup
  without new locks; the regression and five existing probe tests pass. Full
  `test-cpp` (including FFI), full `tree-lint`, monitor all-target clippy and
  fmt pass. Fourteen focused C++ transport/disconnect cases pass, including
  concurrent explicit closes on two engines/four workers while retaining old
  handles. The final targeted lint/format checks also pass. Fresh-image native
  proof remains required. Failed W/X native state remains preserved; all three old
  owned native containers are stopped. Another four independent blank 8-GiB
  raw test devices per VM are attached and their serials/lineage recorded for
  a fresh post-fix cluster; old accepted disks and journals are not reset.
  Socket-use references additionally fence descriptor reuse while reads, writes
  or engine re-arms are in flight. Close rejects new uses atomically; a worker
  retains its close request until existing uses retire. A new physical-FD test
  holds an active I/O scope across close and requires the descriptors to remain
  valid until that scope ends, then close without transport shutdown. Follow-up
  full C++/FFI, 174 monitor tests, affected all-target clippy, targeted tree-lint
  and Rust/C++ format gates pass. Image Y includes this final I/O fence and has been built and loaded.
  No A31 protocol/recreation pass claimed.
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
  - [x] T05b: 8 Web formation and 19 monitor policy/candidate/initialization/
    bootstrap tests pass. Two new KV-server integration cases exercise actual
    `/system/prepare` persistence: asymmetric overlapping coordinator sets cannot
    accept two manifests; restart preserves every accepted byte; late conflicting
    init cannot create storage. A discarded prepare response resumes the exact
    owner after a new server/registry, rejecting changed replica/operation.
    All 7 group-creation tests and KV-server all-target clippy pass. The complete
    monitor suite also passes all 173 tests. Both registered distributed startup
    interruption cases pass on image U, preserving accepted bindings, storage,
    container identity and retained bytes; this does not certify every A22/A24 fault.
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
  - [x] F07: real one/three-node Add Rack flows exposed polling cancelling a
    slow post-mutation rack read. Trace proves POST 201 followed by aborted rack
    reads and a stale sidebar. Polls now let an active read finish; explicit
    refresh starts a fresh read and fences older responses. The regression fails
    before the fix and passes afterward. All 277 UI tests/types pass; both real
    default UIs pass on image U. The existing browser flow retains DOM assertions
    and asserts the mutation's 201 separately, with independent step timings.
- [~] **T15a–T15d — Finish member trust and retained recovery** (A32/A33).
  Initial authenticated member pins, SSH-verified rediscovery, duplicate live
  identity rejection and identity-bound replica routing are implemented. Fresh
  three-node Docker changed-IP recovery with retained S3 bytes passed.
  Foundation implemented: authenticated admission now retains a canonical public
  Ed25519 host key in `NodeRecord`; another key and authenticated address updates
  cannot replace it. A fresh confirmed Group-0 registry can extend initial pins
  to later members and omits pending/cancelled admissions, while rejecting
  unpinned later members, conflicting initial pins, operations and shared keys.
  Initial sealed trust remains immutable. Eleven shared recovery tests and
  affected shared/Web/monitor all-target clippy pass. Thirteen affected Web
  auto-join, target and preflight tests also pass.
  Runtime integration is implemented and under verification: pending StartKv
  first seals the Group-0 manifest's initial pins plus the authenticated local
  admission pin. Confirmation distributes a fresh-authority-verified snapshot to
  all confirmed monitors. Independent workers retain current confirmed contact
  pins/topology for quorum-free SSH proofs, while keeping the original voter
  manifest unchanged. Pending/missing/corrupt snapshots cannot authorize a later
  member's restart or RecoveryIdentity; current authority retirement writes the
  local cancellation fence and stops owned storage/KV services. Existing
  initial trust remains immutable and is never learned from discovery.
  Ten monitor member-trust tests pass, including later-member quorum-free
  identity, unchanged voting/store configuration, exact own key and pending,
  missing, corrupt, foreign and retirement failures. Affected clippy/fmt pass;
  the full protocol/shared/monitor/Web gate passes 782 tests with zero failures
  or ignored cases across 165 test/doc-test binaries. Fourth owned native KVM
  host is prepared for actual non-voter admission and address recovery.
  Actual image-Z later admission now passes on four Docker-free KVM hosts:
  UUID `543959e6-713f-4900-82a1-de92a17f0577`; all four snapshots have initial
  pins 1/2/3 and confirmed contacts 1/2/3/4, while every original manifest still
  contains exactly three nodes. Node 4 is explicitly diskless with disabled data
  services. All three serving members retain S3/Iceberg/Dataset witnesses after
  admission. Evidence: `r243-native-kvm/node{1,2,3,4}-confirmed-contacts.json`,
  guest VM1 `read-four-members.log` and `admit-four.log`. The fourth CLI launch
  exposed nerdctl's null (rather than empty) Devices list on diskless nodes;
  creation and server admission succeeded, but post-create verification returned
  an error. Normalize this absent list while retaining exact mount/resource
  checks. The regression and all 59 OCI helper tests, 20 image identity tests and
  CLI clippy pass; fixed CLI resumes that exact fourth container and seals UUID.
  Explicit source-guard exclusions cover only the audited host Python modules
  embedded by CLI, never the node services, entrypoint or build inputs.
  Cold image-Z acceptance exposed a startup cycle: retained voter startup waits
  ten seconds for KV health before exposing discovery, while the KV quorum needs
  authenticated discovery to replace old IPs. Original voters repeatedly exit
  with `KV server did not become ready`; the later nonvoter stays available.
  All old management/SSH/RPC endpoints were proven unreachable; an independent
  fifth host forwards only the explicitly stable public protocol URIs. Failed
  state, old image, trust/identity hashes and all data remain preserved.
  Split retained process startup from explicit admission's health wait. Startup
  and health restarts now spawn promptly; current KV health is independently
  probed, and a live unready child consumes no additional restart attempts.
  The production resume/health regression passes with an unavailable health
  endpoint, six observations, recovery on the same PID, failed health after one
  actual restart and changed-key rejection. Full monitor tests, all-target clippy
  and Rust format gates pass. The first gate caught an over-eager test helper
  requiring unrelated fixtures' runtime directories; health setup is now lazy.
  Image AA fixes the startup cycle on the preserved changed-IP cluster. Actual
  one-voter plus confirmed-nonvoter observations remain unavailable; two original
  voters restore authority while the missing required member keeps the cluster
  degraded. All four authenticated endpoint records then converge to .21–.24.
  Full protocol acceptance still fails after the unchanged 180-second bound:
  Web's independent protocol sampler validates new committed listeners against
  its retained UI topology, so it rejects them as `protocol-configuration-invalid`.
  Storage and actual service readiness have recovered. Preserve this second
  distinct cold-recovery failure and original image/identity receipts.
  The sampler now reads confirmed local identity/address alongside current
  service intents within its existing one-second authority budget; it never
  refreshes or rewrites UI configuration on a status request. A real KV/HTTP
  regression verifies S3, Iceberg, Dataset and Access health at the new address,
  while the old UI host and original manifest remain unchanged. Pending,
  cancelled, wrong-node/discovery and foreign-cluster records authorize no probes.
  Full Web gate passes 150 tests with zero failures or ignored cases; affected
  all-target clippy and Rust format checks pass. The final focused regression
  is repeated after extracting its helpers. Public host profile migration passes
  190 Python fixture tests; actual native/Docker host executions remain pending.
  **Gap:** fresh-image cold changed-IP recovery acceptance and final registered host-runtime
  fixture/CI migration. Verify real four-member
  contacts with unchanged three-voter authority, preserved protocol witnesses
  and host-network address changes; retain all failed evidence.
  Owners: shared `deployment/recovery*`, `registry.rs`, Web `node/admission.rs`,
  monitor `node/runtime/{admission,recovery}*`, KV identity-route tests.
- [ ] **T16 — Complete current dependency acceptance** (A15/A23/A26/A27).
  Group0/disk+binding → owner → space layers, current operations, stateless idle
  ChunkKV and owned-range writer readiness exist. Finish service stop, voter loss,
  stale evidence, mixed failures and bounded endpoint checks with non-repair proofs.
  - Image-U full `console.cli.container` scope: seven pass; observation/data
    round trips fail with current (not expired) health evidence reporting
    `chunks read exceeded its 750 ms deadline`. Metadata and other admission
    flags are Ready. Exact isolated observation/data cases pass. Logs:
    `r243-deployment-continuation/{cli-container-full,observation-isolated,data-isolated}`.
    No timeout increase, caller retry or assertion relaxation. DiskIO's five-second
    inline topology refresh serially reads service and hardware metadata before
    its existing read timer begins; this is a hypothesis, not a proven root cause.
    Add cancellation-safe phase histograms and precise shared-budget timeout
    diagnostics before attributing the failure or changing readiness semantics.
    Phase instrumentation is implemented: `chunkio.readiness.{total,hardware,
    chunkdb,diskio}` histogram/error/in-flight metrics, exposed through Access
    `/_crowdb/metrics` with fixed phase labels. Each subphase uses the same
    absolute 750 ms deadline; failures name the dependency. Cancellation retains
    latency/error accounting. CLI fixtures collect bounded metrics before teardown.
    Two focused deadline/cancellation tests, five cached-health tests, three S3
    metrics tests, 182 Python fixture tests and all-target affected clippy pass.
    Fresh-image full-scope execution remains required; this is observability,
    not a claimed fix for the intermittent deadline failure.
  - Image V full CLI scope finishes with seven passes and two failures. Both
    startup replay cases, sustained terminal-writer recovery, attachment,
    hardware/logical/data round trips pass. Observation fails on Access-1:
    ChunkDB phase max 754.97 ms, hardware max 199.23 ms, DiskIO max 5.64 ms;
    generation 6 is current. This rules out DiskIO refresh as that failure's
    direct cause. `ListChunks(max_keys=1)` can enumerate pages of foreign-domain
    or foreign-owner records before finding one result, and merges system/user
    scans serially. Replace health listing with two routed point reads, accepting
    only typed authoritative NotFound or the exact requested chunk identity.
    Integration regression passes on real ChunkDB/KV RPC: empty and 320-record
    catalogs each perform exactly two Gets, zero Scans and zero writes; a corrupt
    chunk record fails. Fresh image and complete CLI rerun remain required.
  - The other image-V failure is the pending-free fixture's missing exclusive
    allocation precondition: ChunkDB reserves strips while the benchmark compares
    global busy bytes (observed delta 289406976 vs own 19922944). The fixture now
    explicitly stops Access, Chunk-KV and ChunkDB through normal service intent,
    verifies stopped PIDs, and preserves KV/DiskDB/DiskIO processes. The original
    strict assertion passes in `pending-free-quiesced` on image V (one test,
    zero ignored). Three precondition tests and all 185 fixture tests pass.
  - Startup recovery now retains the process in Initializing while exact durable
    commit/free intents await DiskDB routing or transport recovery. Typed segment
    corruption and permanent RPC failures remain terminal; stop cancels pending
    work before RPC admission. Free results retain uncertain outcomes as pending.
    Four new real-KV/DiskDB regression tests cover retired cleanup, deleted-chunk
    free, cancellation and malformed segments. All 38 full-stack lifecycle tests,
    three runtime/routed-probe tests, five Access cached-health tests, six DiskIO
    client tests and three binding-fence/partition tests pass. Rust fmt and
    affected all-target clippy pass. Fresh-image startup replay remains required.
  One earlier image-T Compose-three run returned HTTP 503 on namespace create,
  logging `malformed Iceberg record`. Exact-case reproduction and the final
  six-case image-U run pass, but no fix for that intermittent error is claimed.
  Retained failed volumes recover the namespace on every member. A separate
  instrumented diagnostic completed 100 creates; concurrent helper tests and
  41 namespace/retry tests pass. Temporary diagnostic code was removed and
  modified diagnostic containers are not acceptance evidence. Keep the original
  failed logs/volumes and investigate a future recurrence without blind retries.
  F01 addresses an uncertainty/readiness gap; do not conflate free bytes with
  allocatability. Preserve detailed successful facts as well as errors.
- [ ] **T19–T20 — Add common CLI host/containerd lifecycle** (A08–A10/A29/A31).
  Docker bridge lifecycle/ownership receipts work. Host adapters are implemented;
  finish actual host recreation and Docker host acceptance. Reuse production resource/device/port/interface/
  credential validation from `container/oci-image/{run-node.py,runtime.py,storage.py}`.
  Seal daemon/context/endpoint, exact resources/mounts/image and owner before
  effects; namespace is a backend detail, never application cluster identity.
  Verify applied cgroups/host network and one node per physical host.
  - Host launcher now forwards `--node-policy-file` read-only to the normal
    entrypoint, validating bounded regular UTF-8 TOML and UID-10001 readability
    before runtime effects. This permits explicit production supplied-media and
    public-URI inputs without bypassing normal node policy. Failed resource
    verification retains the owned container and data for explicit cleanup.
    All 47 OCI helper tests pass, including both runtimes, rejected file inputs
    before runtime access, and no implicit removal of failed/foreign containers.
    Common CLI now embeds those same host validators and accepts Docker or
    containerd host start/stop with explicit production inputs. It seals the
    private runtime/owner/input receipt before creation, verifies current daemon,
    exact resources/mounts/startup policy and media lineage, and resumes lost
    create replies without adopting foreign containers. Independent
    `oci-host-clean` preserves all external data/media and receipts. Three
    Docker-free native hosts have launched image Y through the actual CLI and
    completed authenticated production admission/formation. Native protocol and
    recreation proof remain pending. Initial read-only FD samples remain stable
    at monitor 56 and DiskIO 187; no nofile increase was used.
    Host code lives in CLI `local/host*` and OCI `host_{inputs,runtime,lifecycle,
    cleanup}.py`; tests cover interrupted creation, daemon/input/owner drift,
    exact raw mapping, stopped resource changes and independent cleanup.
    The first protocol run repeatedly hashed a 389-MiB debug CLI over read-only
    9p. It created a durable protocol witness, then was stopped during later
    verification; the witness and data remain. A write retry was correctly
    rejected by the already-existing namespace. Use retained witness reads
    without resetting any state. The local stripped client reaches Ready, but
    an immediate other-host status exposed
    strict cross-host future timestamp rejection. Preserve that failed evidence
    and diagnose the clock boundary before claiming the three-host pass.
    Current collection/CLI reduction now waits for a small peer timestamp only
    inside the original monotonic request budget and each fact's freshness
    horizon (at most three seconds), then applies unchanged pure freshness and
    operation fences against the real local clock. Large/far-future offsets,
    insufficient remaining time and expired evidence remain unknown. Twelve
    shared readiness regressions and seven actual CLI HTTP tests pass, including
    a single HTTP observation with an 80-ms offset and rejected 10-second offset;
    affected all-target clippy and Rust fmt pass. Native pre-recreation retained-witness read now passes on all three hosts
    with cluster UUID `c91e3082-dbc0-49fd-bf3f-acc90c0bc0ac`: S3 bytes,
    committed Iceberg properties and Dataset snapshot/manifest. Proof lives on
    VM1 at `/srv/crowdb-r243-final/protocol-before-recreation.json` and private
    `protocol-witness.json`; final recreation proof remains in progress; no timeouts or readiness assertions were relaxed.
- [x] **T25 — Register direct Docker/Compose reference acceptance** (A01/A03/A25).
  - Six registered `deployment.reference` cases cover direct Docker and the
    unchanged shipped one/three-node Compose files, plus real editable UIs in
    both node counts. Normal entrypoints, dedicated bridges/named volumes and
    published UI/S3/Iceberg/Dataset ports supply neither fixed IPs nor seeds.
    Endpoint-only clients create no CLI runtime ownership receipt.
  - Current image `crowdb-node:r243-u`, Docker config
    `sha256:6858601c47671889ff6bb0c90971d5f737e786c327d873a3254e3cacf97ca93a`;
    OCI manifest `sha256:79b755857632825444a10c0d894e7d8e37f3a849a6e8f0297034d62cb45d2e91`.
    Verified archive/receipt: `target/r243-u.oci.tar{,.json}`. Includes F07 below.
  - All six actual fixtures pass in `final-reference/run-237c44f371c54fb7b030895283c0b2c0`:
    management writes, signed S3 retained bytes, Iceberg property commit/read,
    Dataset inline snapshot publish/manifest read, unauthenticated rejection,
    matching UUIDs and unchanged credentials/data after actual container
    recreation. Direct Docker recreates in reverse order without a fixed IP.
    All six checked cleanup receipts have zero retained successful volumes.
  - Direct three-node cases first hold back the third voter: check exits 1 in
    the bounded wait with phase waiting, no admitted nodes/racks and no accepted
    bootstrap. The runner's phase rollup incorrectly treated this expected exit
    as failure despite passing fixture lifecycles. CLI phase receipts now record
    the expected exit; report regressions preserve unexpected-exit/cleanup failures.
    Both affected cases pass with correct final reports under `report-docker-three`
    and `report-ui-three`; successful scoped cleanup is verified separately.
  - All 180 Python fixture tests pass, including image/name/project, foreign
    storage, unrelated attachments, preexisting scope and negative-exit guards.
    CI inventory owns 388 named flows; inventory is not runtime execution.
    Evidence root: `.crowdb-runtime/artifacts/r243-deployment-continuation/`.
- [ ] **T29 — Finish removing legacy public paths and migrate their callers**
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
  - [x] T30a: `console::cli_container::pending_free_accounting` owns a fresh
    normal one-node Docker fixture and explicitly runs the otherwise ignored
    `cluster_workloads_test` with `CROWDB_TEST_CLUSTER_MANAGEMENT_ENDPOINT`.
    The host test executable and actual CLI are pinned in the client bundle;
    it executes exactly one test, with no ignored cases. Real pending frees
    precede the mix baseline; positive expected delta equals actual busy delta
    and errors remain zero. The registered run and checked cleanup pass under
    `r243-deployment-continuation/pending-free`. CLI all-target clippy passes.
  - Python fixture suite: 180 tests; CI inventory: 388 owned flows. Inventory
    does not substitute for runtime execution of the remaining acceptance.
- [ ] **T34 — Verify host runtimes honestly** (A08/A09/A31).
  - [x] Native Docker-free containerd: three independent owned KVM Linux hosts,
    rootful containerd 2.2.1 + nerdctl 2.4.1; image Y manifest
    `sha256:afcbd73d5f39b7d0a61841dbce203e338419f598750c5502de137589ff408b54`.
    Actual common CLI host creation, authenticated production admission and
    formation, signed S3 bytes, committed Iceberg properties and Dataset
    snapshot/manifest all pass. CLI start seals the observed application UUID.
    Independent `oci-host-clean` removes all three exact owned containers,
    retaining raw devices, data and both receipts; CLI recreates in order 3,1,2.
    All three containers acquire new IDs while original UUID and credentials
    remain; all three protocol witnesses read successfully afterward.
    Applied limits stay `cpu.max=200000 100000`, memory 2147483648 and host
    network namespace. Daemon, input hashes and physical identities match
    before/after. Docker client/socket/daemon are absent on each proof host.
    Evidence: `r243-native-kvm/node{1,2,3}-recreation-proof.json`,
    `protocol-after-recreation.json`, guest `/srv/crowdb-r243-final/` logs and
    private witness. This certifies image Y plus the recorded current host CLI,
    not the newer in-progress member-trust server changes.
  - Repeated FD samples remain monitor 56 and DiskIO 187 on VM1;
    unchanged soft nofile=1024, no larger restart budget. Old failed W/X data
    remains preserved separately; no accepted media was reset.
  - [ ] Actual Docker host profile and changed-address host-network proof remain.
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
- At the audit boundary the strict Stream performance checks remained red.
  The subsequent user decision below moves the directory redesign and framed
  synthetic-data fixture to R244; do not replace those failures with a pass.
  Native Docker-free A31 now has three owned KVM hosts; remaining proof is tracked under T34.

## Stream latency attribution and first optimizations

- [x] **F06 — Expose phase distributions and remove serial metadata waits**.
  User requested perf metrics sufficient to attribute latency and asked to fix
  the obvious bottlenecks first. Added process-wide atomic histograms for
  admission-to-completion, queue dispatch, capacity preparation, batch encoding,
  mirror writes, fenced cursor resolution, directory build/publication,
  page CAS, head GET/CAS, and reads. Chunk-KV-server metrics registration includes
  these histograms in periodic perf output; the benchmark prints cumulative
  count/avg/p50/p99/max. Window flushes preserve full-run distributions.
  Queue includes capacity preparation; nested stage percentiles are not additive.
- Identical 4 KiB workloads, ten seconds per sample, same retained release
  services and native fixture parameters, compilation outside measurement:
  NullDisk 1T 134.29 -> 151.79 ops/s, average 7445 -> 6587 us, p99
  9732 -> 7748 us; 32T 1017.09 -> 1838.39 ops/s, average 31456 ->
  17384 us, p99 71552 -> 42448 us. MemDisk 1T 131.78 -> 145.80 ops/s;
  32T 1008.04 -> 1847.55 ops/s, p99 70964 -> 36014 us. All eight samples
  have zero errors and watchdogs; all four scoped shutdown receipts pass.
- The pre-change NullDisk 1T batch averages identify the wait: mirror 1022 us,
  cursor 2514 us, directory publication 3815 us; directory CPU build only 12 us.
  The KV adapter now reuses the last observed/successful CAS revision only for
  the exact stream/epoch/generation, still fencing every head mutation with KV
  CAS. Immutable directory page writes run with a maximum concurrency of eight,
  and all must complete before head publication. No append consistency or read
  snapshot boundary changed. Single-thread head GET count falls from 1352 to 3;
  publication average falls to 2998 us. The request still requires durable
  mirror data and cursor advancement before completion.
- Existing tests plus four new cases pass (59 stream integration cases): no
  redundant head read after CAS/load, cold visibility of all 17 parallel pages,
  stale-writer and immutable-purpose fences, conflicting immutable page rejection,
  and periodic/cumulative perf distribution visibility. All 43 common-metrics
  tests and the service perf-log shutdown/disable test pass. Rust fmt and
  all-target common/stream/Chunk-KV-server clippy pass. Evidence, binary hashes, phase
  distributions and receipt checks:
  `.crowdb-runtime/artifacts/r243-stream-contract-audit/phase-comparison.json`,
  `phase-{before,after}/`, `optimized-tests-final.log`,
  `common-metrics-tests.log`, `service-perf-tests-final.log`,
  `optimized-clippy-final.log`. The service exposes `--metrics-interval`
  (one second by default; zero disables output), and shutdown flushes the
  last metrics window.
- Remaining: every append still rebuilds/rewrites historical directory pages;
  the 32T NullDisk sample writes 10933 pages for 1154 publications. Bounded
  concurrency reduces waits but does not remove this growing write amplification.
  Original strict throughput/publication assertions remain unmodified and unmet;
  T29b and R243 remain incomplete. Framed synthetic reads and T34's external
  native runner proof remain outstanding.
- User subsequently deferred the directory redesign to
  [R244](../backlog/R244-chunk-stream-incremental-directory-publication.md).
  It owns incremental page references, active-tail commitment/checkpoint
  separation, shared-page reclamation and real Chunk-KV WAL performance
  acceptance. Do not continue that optimization in this deployment task or
  mark the existing failing performance gates as passed because work was moved.

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

## Current continuation files

- UI: `app/crowdb-web/ui/src/data/useClusterTree{,.test}.ts` and existing
  `e2e-test/clients/console-ui/e2e/flows/10-cluster-rack-node.spec.ts`.
- Runtime acceptance: `e2e-test/oci_reference_{fixture,runtime,protocols,browser,workloads}.py`,
  `run.py`, `scenarios.py`, `client.py`, `console_cli_client.py`, `report.py` and
  fixture/report/selection tests under `e2e-test/tests/`.
- Bootstrap integration: KV-server `tests/group_creation_test.rs` and
  `tests/common/system_bootstrap.rs`; namespace concurrency: Iceberg
  `tests/namespace_create_test.rs` and `tests/common/store.rs`.
- Explicit attached workload: CLI `tests/cluster_workloads_test.rs`.

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

- Registered Docker changed-address fixture attempt 4 confirms the later member,
  authenticates new endpoints and actually elects Group-0 leader replica 2 at
  16:59:42.901 (and metadata leader replica 3). Its new member binding already
  contains `.19` seeds. The assertion still failed because the fixture used a
  five-second health client for the aggregate's six-second bounded gather of
  an absent third voter. Replace that caller with the existing public CLI status
  command's unchanged ten-second observation budget. This does not change the
  product deadline, 180-second recovery bound, negative quorum assertions or
  required protocols. Failed state/logs are retained under `registered-cold-ab4`.

## Host-profile diagnosis

- Public native host qualification has five failed invocations, including four
  harness setup/configuration failures. These do not satisfy the established
  source-driven five-attempt threshold (see previous retry-limit handoff). The
  mistaken continuation question was withdrawn; existing authorization applies.
  R243 stays incomplete; R244 implementation follows only after R243. Command (inside owned VM5):
  `pixi run --as-is --manifest-path container/oci-image/pixi.toml -e runtime python
  e2e-test/run.py --profile containerd-host --concurrency 1 --cpus 3 --memory-mib
  3072 --archive target/r243-ab.oci.tar --artifacts /srv/crowdb-host-profile/native-results`.
  Logs `/srv/crowdb-host-profile/native-profile{,2,3,4,5}.log` retain full output.
  1. Read-only source validation attempted Git object writes; replaced with an
     isolated temporary index preserving object DB and user's index. Regression
     tests and actual read-only preflight pass.
  2. Full single-voter Production storage correctly rejected protected placement.
     Fixture now separates Production metadata lifecycle from explicit Local
     full-protocol storage with zero node failure tolerance.
  3. Production metadata fixture omitted required KV HTTP/RPC ports. Policy now
     supplies 10000/10100; ordinary monitor validation remains unchanged.
  4. Metadata fixture incorrectly required whole-cluster storage Ready. Current
     authority is verified independently; diskless metadata is never claimed Ready.
  5. UUID/authority and first CLI resume succeed, but immediate retained recreation
     fails before container creation with `host node TCP port 9090 is already
     occupied`, exit 2. Run `ff8341320cb14b729c1d9d43394559d3` retained. Owned
     containers/private daemon were cleaned; data, identities and journals remain.
     Port validation used a bare bind and misclassified TCP TIME_WAIT as a live
     listener. An independent socket experiment and regression reproduce the
     failure; listener-compatible SO_REUSEADDR permits closed connections while
     retaining active-listener rejection. Run the focused gate before replay; do not sleep,
     enlarge budgets, skip validation or remove retained data. VM5 still has no
     Docker installation. Three-host protected native proof and four-host AB cold
     recovery remain valid separate evidence; public profile has no pass yet.

## Open questions

- No unresolved product choice; retry-limit continuation was authorized.
