<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Physical Node Test Plan

This reusable catalog follows category, test case, and scenario. It does not
track the current run's progress. Keep selected scenarios, actual commands,
source/image identities, and results in a separate run directory. Historical
passes do not establish coverage for a new image.

Upstream: [deployment and recovery](../design/deploy/design-crowdb-deploy.md),
[OCI builds and runtime](../../container/oci-image/README.md),
[container acceptance](../../e2e-test/README.md), and
[KV recovery](../design/kv/design-crowdb-kv-slot.md).

## 1. Execution contract

### Environment and scope

- Use two physical failure domains by default: A `192.168.31.129` and
  B `192.168.31.184`. Recheck host identities, interfaces, and addresses each
  run; an IP address does not prove resource ownership.
- Use five fixed voters, A1/A2/A3 and B1/B2, with separate persistent volumes
  and discovery identities. Port offsets are 0/1/2 on A and 0/1 on B; verify
  the values actually accepted by the current launch interface.
- Fix each container at host networking, 1 CPU, 2048 MiB, and 512 PIDs, with
  one Tokio worker per service. Record actual cgroup limits, storage capacity,
  reserves, and deviations.
- Simulate a host fault only by stopping this run's designated containers on
  that host. Do not change shared networking, routes, firewalls, the kernel,
  or the Docker daemon. A three-node variant requires a separate profile and
  cannot substitute for five-node coverage.
- Use the existing SSH key and noninteractive BatchMode. Supply credentials
  privately; exclude passwords, private keys, tokens, login identities, and
  commands containing authentication arguments from committed docs/reports.
- Run all builds, tests, and executables, including remote commands, through
  `pixi run`.

### Case selection and results

- Retain P00-P19 as parent case IDs; `.1`, `.2`, etc. identify independently
  verifiable scenarios. The default main sequence covers every parent and its
  ordinary scenarios. Scenarios explicitly marked as extensions are required
  only by the selected profile; an unselected extension has not passed.
- Freeze scope, concurrency, resources, data size, timeouts, and acceptance
  conditions before starting. Record added regressions separately. Do not
  remove scenarios, weaken assertions, or change profiles after failure to
  obtain an all-pass result.
- For each scenario, record prerequisites, fault target/time, actual actions,
  assertions, duration, result, evidence paths, and input identities. Use
  `PASS / FAIL / NOT RUN`; an injection that never occurred is a coverage gap.
- Follow dependency order and stop dependent work at the first failure.
  Preserve failures and valid passes. After repair, repeat only scenarios
  whose inputs or behavior changed, then resume unfinished work. Do not
  blindly replay partially executed stages.
- Persist the operation, key/ID, and expected content before each mutation;
  persist its acknowledgment after success. Record timeouts/disconnections as
  `OutcomeUnknown` and reconcile them read-only. Controllers must not replay
  uncertain writes.
- Give fault injection and later read-only audits distinct command-log names.
  Preflight those names before mutation. A corrected fresh attempt needs its
  own artifact namespace and operation receipt; retain the failed attempt and
  establish its mutation outcomes before submitting another operation.
- Do not increase timeouts merely to hide failures, weaken checks, or add caller
  retries. An agreed production probe-budget change needs a separate attempt
  and preserves earlier failures. P14 observes
  the production client's existing explicit safe-retry policy unchanged.
- Complete builds before their consumers and serialize builders sharing an
  output directory. Freeze sources before building OCI; retain the source
  tree, commit/dirty diff, client digests, and image digest. Reassess affected
  coverage after source changes. Separate compilation/linking from acceptance
  workloads on shared physical hosts. A contention scenario must declare its
  competing workload in advance and collect host pressure, CPU, and disk data.
- Record CI and physical results separately. When the user stops CI inspection,
  do not query or infer new runs or make CI completion a physical prerequisite.
  Required local gates still apply.

### Assertions shared by recovery cases

- Before injection, capture container IDs, volume names/CreatedAt, discovery
  UUIDs, node IDs, bootstrap bindings, all 35 services' container/PID/start-time
  identities, catalog state, and replica state.
- Observe continuously from injection through recovery and the full data audit.
  Complete samples must be no more than five seconds apart. A fault target
  missing from `/proc` remains an absent observation; retain sibling samples.
- Permit only the process changes specified by the case. Surviving monitors
  must keep publishing fresh, truthful observations during recovery. An offline
  target may be unobservable; old Ready evidence is not current evidence.
  Freshness and health are separate assertions: updating an old healthy
  timestamp cannot conceal Unavailable.
- Recovery gates require current, healthy monitor observations from every
  node, newer than its container start and no more than three seconds old at
  receipt. Fence all 35 process identities until the full audit ends; do not
  reset the baseline to tolerate another restart. Establish the first strict
  health check and identity fence within the original service-recovery budget.
  An early Ready response cannot defer actual recovery verification beyond
  that budget. Independently check strict health again when the full audit
  starts. Later health failures or identity changes are audit failures, not
  another recovery-wait interval.
- Measure catalog takeover, service recovery, and each of Groups 0/1/3's fixed
  checkpoint catch-up separately, each with its original 180-second budget.
  Record each gate's own start and duration. Preserve P10's 60-second limit,
  P13's 30-second limit, and shorter production-client limits separately.
- After recovery, verify P03 baselines, P04/P05 data, P07 acknowledged live and
  deleted records, every historical witness, and this case's new writes.
  Read and validate complete content through all five recovered endpoints.
- Actual EC fragment placement determines payload availability; KV quorum
  cannot substitute for it. Report temporary availability beyond the fragment
  loss budget separately from complete historical correctness after recovery.

## 2. Category A: hosts, images, and cluster formation

### P00 Environment, authentication, and images

- **P00.1 Hosts and resources**: verify both physical hosts, SSH keys, clocks,
  interfaces, cross-host listener reachability, Docker/cgroups, free disk, and
  memory. Inventory existing resource ownership and record five-node limits
  and storage budgets. Fail inadequate preparation without shrinking data.
  Record the actual link medium, selected route/interface, and each Wi-Fi
  adapter's power-management state. Declare the network profile before loaded
  acceptance; neither enabled nor disabled power saving is a universal pass
  requirement. Any setting change requires user authorization, before/after
  verification on both hosts, and a restoration or continuing-profile decision.
  Where available, record kernel ACK policy, CPU idle/governor state, scheduler
  pressure, and Wi-Fi power saving as possible timing variables. Their presence
  does not prove causation; do not tune host settings automatically.
  Do not disconnect an interface or alter shared routing as a test fault.
- **P00.2 Identical image content**: import the same verified image transfer
  file on both hosts. Check its digest, manifest-referenced config content hash,
  platform, source/runtime labels, and ordered rootfs layers. Docker may expose
  a manifest ID on one host and a config ID on the other; comparing ID strings
  alone is insufficient.
- **P00.3 Invalid inputs**: offline regressions reject damaged transfers,
  source/runtime label mismatches, reordered layers, and architecture mismatch
  before any node upgrade starts.
- **Evidence**: redacted host inventory, image/build receipts, actual inspect
  results from both hosts, and provenance mapping.

### P01 Five-node discovery, initialization, and identity

- **P01.1 Formation**: form five voters through production policy, discovery,
  key admission, and storage preparation. Verify five distinct discovery
  UUIDs/node IDs, accepted initial_voters, fixed membership, and real listeners.
  Use the supported five-voter path without inventing CLI arguments.
- **P01.2 Addresses and ports**: verify each offset is applied once, same-host
  ports do not collide, cross-host addresses avoid unreachable loopback, and
  both rack/host assignments are correct.
- **P01.3 Identity retention**: restart existing nodes with their original
  bindings, volumes, and identities without bootstrapping again. Reject stale
  bindings, foreign volumes, and replacement containers reusing an old name.
- **Evidence**: accepted policy, membership/listeners, volume mappings,
  bootstrap bindings, and discovery identities.

### P02 Cluster readiness and continuous observation

- **P02.1 Baseline**: all five Web endpoints are Ready, with seven services
  per node and 35 actual live processes. Verify health, dependencies, and
  continued renewal of DiskDB's current ownership lease.
- **P02.2 Freshness**: record receipt times and reject monitor snapshots older
  than container start or three seconds. Distinguish process liveness, listener
  liveness, and data-service readiness. Cross-host sampling may reuse an
  authenticated SSH connection, but each response requires a complete fresh
  host sample. Record transport/process-start cost separately from node sampling
  time. Reuse cannot relax the five-second complete-observation bound, replay
  samples, renew old timestamps, or hide sampling failures.
  For loaded cross-host latency, retain idle/load/idle observations with the
  same native or protocol audit, source/image, data size, logical deadlines,
  concurrency, resource limits, and pinned process identities. Record host I/O,
  competing builds, cached-health responses and failed generations separately.
  If authorized, compare declared Wi-Fi power-management profiles as distinct
  attempts; retain both states and results without rewriting the first failure
  or treating a faster comparison as complete recovery acceptance. Use matched
  request/response packets and conservative cross-host clock bounds to locate
  delay. Record capture scope, startup boundary, kernel drops, and gaps: dropped
  or missing capture packets cannot prove network loss. Reconcile collector
  failures offline when retained bytes suffice, preserving the original receipt
  and qualifying derived evidence separately.
  Positive matched packet timestamps include host stack and queue delays;
  they are not pure Wi-Fi propagation or server-handler duration. State clock
  assumptions and retransmission limits. With capture gaps, an observed request
  or reply need not be the earliest emission or arrival.
- **P02.3 Authority degradation**: during Group0 failover/unavailability or a
  slow remote observer, the local monitor still reports current processes and
  truthful health. Service-intent reads must not block health publication or
  diagnostic control. Readiness HTTP requests share independent background
  evidence rather than rereading full authority on every poll. Component
  regressions cover reads lasting over one second, concurrent readers, expired
  or failed evidence, and recovery without page traffic. A warm ChunkDB point
  probe must still read the real owner/storage when its client's ordinary
  registry refresh becomes due; it must not expand into a full authority scan.
  Cover cold discovery, stale-owner rejection, failed-route refresh, and actual
  storage errors with the original deadline. When a bulk response
  blocks a normal TCP connection, the same storage client's physical probe
  reads the original disk's 4096-byte header through the bounded priority lane.
  Rejection, short reads, and probe timeout still fail. Compare the same client,
  target, and deadline; distinguish response counts from failed probe generations.
  Start physical headers as soon as hardware discovery succeeds while the
  independent ChunkDB read continues. Exercise the production orchestration
  with hardware taking 50ms, ChunkDB 700ms, and a header 100ms: all checks must
  complete within the original shared 750ms budget. Held or failed dependencies,
  empty/offline hardware, short headers, and cancellation must still fail without
  leaked work. With 17 disks, preserve the eight-read batch bound.
  This 750ms case remains a component scheduling control. Physical Access
  qualification uses the declared two-second production dependency budget and
  2100ms outer watchdog. Record every failed generation and continue sampling;
  a successful new generation recovers automatically. Raise every probe failure
  below three seconds with its configured timeout, actual duration, error and
  recovery; decide jointly before increasing the production budget again.
  During a sustained dependency outage, verify continuous negative generations
  within the original fault window. Require at least three distinct advancing
  generations per fixed endpoint, reject missing/error/Ready samples, and retain
  the complete history. Negative evidence may truthfully expire between slow
  rounds; do not require every response to be current or renew an old timestamp.
  Validate its declared three-second age and require current Ready evidence only
  after actual recovery. Preserve process identity and durable-data checks.
  Separate deadline correctness from availability qualification: a real read
  completing after its declared shared budget requires truthful Unavailable/503,
  rather than renewed Ready evidence. That response still fails any applicable
  recovered-health availability check; it does not by itself prove a software
  defect. Distinguish measured OS/link timing from a source defect established
  by a causal regression. Keep formal recovery, identity, data, and availability
  assertions unchanged while the cause remains unresolved.
  Hold a KV scan frame after its first control byte while the same ChunkDB KV
  client issues a real point read. Verify point progress with one additional
  connection per endpoint and the existing worker/completion budgets. Cover
  rejection, deadline expiry, failed-generation reconnect, explicit pool clear,
  and the unchanged ordinary transport as a control.
  Under mixed system/user-data metadata, record background KV pages and bytes.
  Grow only the other maintenance domain and verify that scoped chunk and
  reservation scans do not transfer its pages. Cover all supported purposes,
  zero-valued ID suffixes, continuation, foreign slots, and multiple destinations;
  unscoped inventory must still return both domains.
  Hold one instance's process-lease CAS while another instance remains ready.
  A grant pass must attempt each independently ready instance once, atomically
  publish the healthy instance's lease/grant pair, and report the failed instance
  after the pass. Cover a valid exhausted sequence as another independent
  publication failure. Preserve the failed instance's bytes and all process,
  assignment, revision, expiry, and replacement fences without a conditional retry.
  An error's phase label alone does not identify where a shared budget was
  consumed. Never renew an old healthy value or restart a live service solely
  for transient unready status. Cover a new process that remains unready and
  several listeners exhausting their probe deadlines: background recovery must
  yield between passes, and real health probes run concurrently so their waits
  do not accumulate and stall the entire snapshot.
- **P02.4 Observer negatives**: reject old snapshots, wrong container/PID/start
  identities, OOM, missing siblings, complete-sample gaps over five seconds,
  and a second identity change after recovery.
- **P02.5 WAL I/O and executor progress**: use the production multi-thread
  runtime with one worker and a real file-backed BlockDevice WAL. Hold an
  actual write or durable-sync boundary and verify that independently spawned
  apply/RPC-like work still progresses while the append remains unacknowledged.
  Release the boundary, then verify write order, durable reopen, and original
  error behavior. Keep a current-thread compatibility control; do not claim
  that synchronous fallback provides task isolation. In physical slow windows,
  correlate Group0 read barrier, apply-fence wait, actual engine apply, WAL
  append/sync, and cgroup counters. Record that a shared readiness deadline can
  expire before DiskIO starts; a phase label is not proof of slow physical I/O.
- **P02.6 Process observation validity**: follow the
  [Console service lifecycle contract](../design/console/design-crowdb-console-ui.md#13-service-lifecycle).
  Pair actual owned-process samples with focused backend/UI controls for failed
  reads, malformed or incomplete inventories, expired evidence, and wrong Node,
  bootstrap/operation epoch or process identity. Missing PID evidence is unknown,
  not confirmed absence; retain registered services and successful siblings.
  Only a complete, fresh, identity-matched observation can confirm that no process
  exists, and an accepted startup must remain pending. Endpoint health cannot
  supply a missing PID or prove process absence.
- **P02.7 Read-only plan recovery**: retain a failed-only saved plan and verify
  observation continues every two seconds even without Capacity data or another
  queued deployment. Unknown evidence keeps waiting or retains the recorded
  failure; a current owned running process reconciles progress to deployed.
  Include the same PID before and after an observation gap, interrupted-plan
  reload, and stale/foreign/expired evidence that must not clear a failure.
  Keep the original three-second UI reconciliation assertion. The plan must
  issue no automatic Start, Restart or Deploy; explicit Retry alone initiates
  another failed lifecycle mutation. Progress recovery does not establish health.
- **Evidence**: complete raw samples, gate receipts, recovery identity fences,
  observer results, persisted plan revisions, and lifecycle request counts.

## 3. Category B: native data, protocols, and scale

### P03 Baseline data and allocation through five endpoints

- **P03.1 KV**: verify five-replica mappings and current leaders/leases for
  Groups 0/1/3. Issue single and batch writes once, record acknowledgments, and
  read complete keys/values through all five endpoints.
- **P03.2 ChunkDB**: allocate and query the same Chunk ID with the production
  native client. Check location, size, and identity; a successful new ID cannot
  substitute for the original allocation result.
- **P03.3 S3**: independently generate S3 SF1 data with `crowdb-tpc-loader`.
  Retain each object's URI, length, and digest, and fully read all eight tables'
  files through five endpoints. Retain a run-unique witness of at least 96 KiB
  and verify its complete bytes and hash.
- **P03.4 Iceberg**: independently generate eight-table SF1. Check namespace/
  table UUIDs, snapshots, metadata locations, manifest file sets, file lengths/
  digests, decoding of every row, and independent scans. Verify schema, property
  commits, and data files separately; footer reads or ten-row samples cannot
  replace full reads. For credential expiry, retain the same FileIO and open
  stream across the service grant's actual lifetime, then seek/read the same
  immutable object. Record original TTL, identity, and refresh scope; do not
  substitute table reloads, clock changes, or longer TTLs. Limited scans must
  release the native reader before the input file on success and failure. The
  verifier must exit normally; a crash or hang after row validation is FAIL.
- **P03.5 Dataset**: generate inline fields, multipart payloads, and immutable
  sample snapshots. Fully read each report and field through five endpoints;
  verify the actual report count.
- **P03.6 Protocol isolation**: native stress keys must occupy proven safe
  ranges outside S3/Iceberg/Dataset metadata prefixes. Adding a test suffix to
  a protocol key can still corrupt protocol scans.
- **Evidence**: data/ack ledgers, loader provenance and commands, all generated
  reports, and independent verification from five endpoints.

### P04 A 512-record reproducer and five-owner coverage

- **P04.1 Historical keys**: run the original 512-record reproducer separately
  and record its actual owner coverage. Do not automatically count five owners.
- **P04.2 Formal workload**: read the full catalog and choose run-unique safe
  keys within five actual Serving owners' ranges. Use 32 batches of 16 records,
  each 64 KiB: 512 records/32 MiB total, retaining the default five-second batch
  deadline. Uniform first-byte distribution does not prove routed ownership.
- **P04.3 Integrity**: record each batch's acknowledgments and unknown outcomes,
  then fully read it back. Retain split/cutover state, phase latency, and errors.
  Reconcile unknown writes read-only without replaying them.

### P05 Larger working sets and persistent-storage boundaries

- **P05.1 Scale**: after P04 passes, use a new namespace with at least 4096
  deterministic 64-KiB records, totaling 256 MiB across five actual Serving
  owners. Budget mirrors, metadata, WAL, reclamation, and artifacts; keep test
  media at least 30% free.
- **P05.2 Extent boundaries**: construct persistent data across file ends using
  actual block sizes. After cold open, trigger asynchronous reads crossing one
  and multiple extents. Verify full content and exactly one completion. Record
  actual offsets, lengths, and boundaries; a large working set alone does not
  prove this path ran.
- **P05.3 Error propagation**: component regressions cover missing subsequent
  extents, truncated tails, and range overflow. Preserve real I/O errors; reject
  short-read success, duplicate completion, or retries masking errors.
- **Evidence**: owners/keys/bytes, full digests, actual extent paths, and
  separately stated component and physical coverage.

## 4. Category C: 30-minute mixed workload and final consistency

### P06 Sustained mixed workload

- **P06.1 Parameters**: declare concurrency, operation mix, pacing, key/version
  rules, and bounded data volume before starting. Cover both hosts and five
  owners; reused or reclaimed data must still produce successful operations.
- **P06.2 Concurrent native and protocol work**: mix native insert, overwrite,
  delete, and readback with S3 writes/reads/deletes, Iceberg commit/load/cleanup,
  and Dataset publication/readback.
- **P06.3 Effective duration**: sustain at least 30 continuous minutes of
  successful overlapping mixed work, excluding builds, deployment, and idle
  waits. Waiting after a fixed seed is not load. A repair or controller restart
  that interrupts the workload requires a fresh continuous 30-minute run.

### P07 Acknowledgment semantics and data audit

- **P07.1 Ledger**: retain operation, key/version, expected length/hash, ack
  status, and time before/after mutations. Serialize same-key overwrites or use
  an explicit version contract to avoid verifier races.
- **P07.2 Online checks and final full audit**: continuously read acknowledged
  data. At completion verify every live key, overwritten value, full protocol
  payload, and witness, and continued absence of all acknowledged deletions.
- **P07.3 Unknown outcomes**: retain OutcomeUnknown for timeouts/disconnections.
  If present after recovery, verify expected content. Do not assume absence or
  replay the mutation to eliminate uncertainty.
- Request failure during normal load, content mismatch, OOM, or an unplanned
  process replacement fails that workload stage.

### P08 Performance and resource observations

- **P08.1 Complete sampling**: at least every five seconds record throughput,
  success/failure/unknown counts, p50/p95/p99/max, CPU, RSS/cgroup memory, free
  space, process identities, catalog state, and reclamation.
- **P08.2 Phase statistics**: separate build, startup, seed, sustained load,
  fault, and audit durations. Retain successful operations/bytes, effective
  overlap, and phase latencies; container liveness alone is insufficient.
  Comparisons of sampling overhead or repairs must hold workload, leader host,
  routed owner, client/image, and actual cross-host path fixed, recording the
  observer's enablement and cost. Leader movement or early workload completion
  makes samples incomparable; retain them without claiming improvement.
- **P08.3 Host anomalies**: retain diskstats, I/O/memory pressure, cgroup
  throttling/OOM, and readable kernel errors. Low CPU or ample memory cannot
  rule out fsync or device I/O stalls.

## 5. Category D: process, container, and host faults

### P09 Cold start and image upgrade on original volumes

- **P09.1 Cold recreation**: stop and recreate all five containers with original
  volumes, policy, UUIDs/node IDs, and bootstrap. Verify 35 services, every fixed
  checkpoint, and complete historical data through five endpoints. If this also
  accepts a new image's startup, explicitly share that actual cold recovery
  and its complete observation. Do not inject the same passed scenario again;
  retain interrupted startup attempts as failures.
- **P09.2 Same-volume upgrade**: after a production repair, verify the new
  image's content identity before replacing containers. Preserve all volume
  names/CreatedAt and data identities. Image transfer tar files distribute
  software; they are not data-volume backups.
- **P09.3 Operation races**: before recreation recheck old container IDs,
  stopped states/times, mounts, and ownership. Stop if another operation started
  or replaced a container during preparation; do not blindly remove/create it.

### P10 One unresponsive ChunkKV

Combine full catalog and current placement observations at the recovery gate.
All entries may be Serving while a handoff source is preparing or quiescing;
that alone cannot establish recovery. Reject active handoffs and selected moves.
Catalog generation must match before and after health sampling. Distinguish
historical, committed, serviceable overlays from active owner cutover. Retain
the original 180-second recovery budget and the subsequent strict health,
PID/start-time, and complete-audit assertions.

- **P10.1 Injection**: SIGSTOP only one ChunkKV whose container/PID/start-time
  was verified. Do not pause the entire container or a same-named service.
- **P10.2 Recovery**: the old process exits and is replaced by a new PID within
  60 seconds; the other 34 identities remain fixed. Verify old data and this
  case's new acknowledged writes. Reject another silent replacement during
  the full audit. Track takeover-triggered source checkpoints and return
  migration so a recovered process cannot conceal permanently stuck preparation.
  Component injection covers synchronous/asynchronous root-publication failure,
  sustained unavailability, and intervening writes: complete the pending root's
  original barrier before the next checkpoint writes data. Failure remains
  Unavailable, the old root stays recoverable, and the recovered new root
  contains acknowledged writes; repeated Internal errors are unacceptable.
  A terminal-journal component case must drive an actual ChunkKV metadata
  mutation during the DiskIO outage. Include a metadata-only operation, such
  as bucket creation: object payload writes can fail before metadata publication
  and therefore never exercise the intended journal failure or replay. Keep
  the original restart, retained-data, and healthy Access identity assertions.
- **P10.3 Failure cleanup**: after retaining errors, resume any surviving
  stopped process so injection does not contaminate later work.

### P11 Cross-host KV lifecycle

- **P11.1 Follower**: choose a follower on a different physical host from the
  current leader. Stop its local KV through its Web endpoint; verify the
  completed Stop receipt and actual exit. Remote authority still manages the
  node. Restart once, require a new PID and 34 unchanged siblings, and complete
  the historical audit.
- **P11.2 Leader**: identify the current leader again and Stop it. Its original
  completed receipt remains readable after exit. Wait for surviving nodes' new
  authority, then restart the old node once and complete the audit.
- **P11.3 Failover window**: throughout the case, check surviving monitors'
  freshness and actual process identities. Final Ready alone can miss blocking
  intent reads, old health snapshots, or sibling restarts. Component regressions
  cover a reachable first management seed still returning the old leader:
  connection failure or rejection without a hint must continue discovery of
  another member's new route within existing retry and lease-safety budgets.
- **P11.4 Exit evidence**: before diagnosing native abort, self-join, WAL drain
  failure, or another abnormal exit, retain application, RPC, tree, stderr,
  and crash/core evidence for that exact PID and container generation.
  Save stderr before injection and compare its exact retained prefix afterward;
  older panic bytes must not be attributed to the new exit. Successful native
  RPC joins do not establish that Tokio tasks or blocking native jobs finished.
  A focused component case holds a real service's background sampling read with
  a registered timer, drains the service, then drops its runtime. Drain must
  retain ownership until the read and its native payload finish; closed sampling
  admission must not launch another job. Capture the isolated process's complete
  output under an external watchdog. A matching component panic proves that
  path is reachable, not the originating future of a physical exit without a
  backtrace. Keep existing supervisor grace and I/O deadlines unchanged.
- **P11.5 Leader tenure notifications**: a component regression accepts a
  fenced Admin step-down before the old leader loop consumes its notification,
  then drives a real election into a new term. The retained notification must
  not step down the new leader; a fresh matching request must still fence it
  immediately. During physical failover, retain term, role, and step-down
  reasons so an unexpected extra election is not dismissed as a slow network.

### P12 Both B-host nodes offline

- **P12.1 Actual takeover**: prove B1/B2 are current Serving owners before
  stopping them. A's three voters take over the catalog within 180 seconds;
  issue one KV write/read and record its actual new acknowledgment. If earlier
  faults changed placement, let normal balance restore both owner prerequisites.
  Completed independently recoverable transfers must not lose balance eligibility
  forever because of historical transition markers. Fail missing prerequisites;
  stopping empty owners or changing balance policy cannot manufacture coverage.
  Regress the ordering where a lease-excluded target's completed marker was
  released before its first checkpoint: the exact current Serving assignment
  may still claim root publication for its new epoch. Different owner, epoch,
  tree, stream, or active transition must not bypass checks through marker
  absence; the old root writer remains fenced.
- **P12.2 New payload while offline**: keep B offline and write a new S3 witness
  and Iceberg property commit through A. The new payload's actual placement must
  allow A to read it independently; fully read it through all three A endpoints.
- **P12.3 EC boundary**: record old objects' fragments per host and loss budget.
  For EC 8+4 with six fragments on B, reconstruction while B is offline cannot
  be required or claimed as existing rack protection. Report new-write
  availability separately from temporary unavailability of old EC objects.
- **P12.4 Recovery and persistence**: restore B, then recreate B1 and B2
  separately on original volumes. Verify original identities, fixed checkpoints
  on five replicas, and all historical data through five endpoints, including
  old EC objects unavailable during the outage.
- **Extension**: to test whole-host tolerance for old EC data, separately select
  a real placement within its fragment loss budget and fully read old data
  during the outage. Three surviving voters cannot establish that guarantee.

### P13 All three A-host nodes offline

- **P13.1 Quorum loss**: stop A1/A2/A3. B reports current unavailability within
  30 seconds, and new mutations must not acknowledge success. Retain failed and
  unknown outcomes; connection loss does not establish that no write occurred.
- **P13.2 Lease-safety exit**: a surviving ChunkKV exiting after its original
  lease expires requires same-process evidence of lease invalidation, deliberate
  stop, RPC worker join, and exit. Native abort or unexplained replacement
  cannot be classified as normal lease fencing.
  New runtime-shutdown panics remain failures even when recovery, data reads,
  and native RPC joins pass. Retain actual exit status and backtrace when
  available; a later replacement PID cannot supply either retrospectively.
- **P13.3 Recovery and protocol reads**: restore A, verify every service and
  fixed checkpoint on five replicas, then audit all native/protocol history.
  Independently retain each Iceberg reader's endpoint, PID, table/object/stage,
  times, stdout, and exit code before aggregating results.
- **P13.4 Error attribution**: separate the first curl/GetObject error from
  interrupted readers and undispatched remainders. Passing raw S3 files cannot
  substitute for full Iceberg reads; concurrent output order cannot identify
  an endpoint.

## 6. Category E: allocation, backlog recovery, and space correctness

### P14 Explicit safe retry at allocation level

- **P14.1 Bounded fault**: preserve all KV processes and Group0 leader/term/
  lease. Inject Group1 leadership step-down; a current leader must accept at
  least one injection within three seconds.
- **P14.2 Actual trigger**: start the production allocation client once and
  observe explicit allocation-level Unavailable safe retry. Final success,
  transport timeout, or an injection that never occurred is insufficient.
- **P14.3 Existing policy**: release injection and complete allocate/query for
  the same ID within the original five-retry/ten-second limit. Retain actual
  retry/ack evidence; instrumentation must not change branches or policy.

### P15 Backlog beyond 1024 slots and full cold recovery

- **P15.1 Actual gap**: stop a real KV follower and acknowledge at least 1100
  single writes through surviving replicas. Record its offline checkpoint upper
  bound and targets before/after writing to prove an actual gap over 1024.
- **P15.2 Catch-up and cold start**: restore the follower and verify bounded
  FetchGap concurrency and actual progress. Then stop all five nodes and cold
  start on original volumes. Freeze Groups 0/1/3's acknowledgment targets before
  the fault; all five healthy replica engines per group must reach them before
  full data checks. Do not move targets with subsequent writes.
- **Extension P15.3**: separately create physical reclaimed-WAL, empty-sealed-WAL,
  and high-promise/old-chosen boundaries and retain trigger evidence. An absent
  trigger remains a coverage gap; component regressions or a normal large-gap
  pass do not establish these physical boundaries.

### P16 Final data and DiskDB occupancy

- **P16.1 Final gate**: all catalog entries are Serving, five endpoints are
  Ready, and 35 services are healthy. Require the same identities in the next
  sample and at the end of the final audit.
- **P16.2 New and old data**: verify a new KV batch, new ChunkDB allocate/query,
  and all historical data separately. Preserve distinctions among
  acknowledgments, acknowledged deletions, and OutcomeUnknown.
- **P16.3 Full occupancy**: scan all this run's DiskDB occupancy,
  covering every disk, zone, and continuation without truncation. Verify live
  mirror segments do not overlap across chunks; retain actual live, allocated,
  free, and reserved counts.
- **P16.4 Consistent inventory**: a bounded current-version scan is not an MVCC
  snapshot. Record one initial cutoff for complete busy-allocation and
  `/chunk/` scans. Either prove both prefixes unchanged through a valid
  completion fence, or reconstruct both inventories at that fence by replaying
  every intervening journal put and delete. Do not ignore metadata fields or
  discard changed records. Apply all scope, completeness, allocation, bounds
  and non-overlap checks to the resulting inventory.
- **P16.5 Completion fence and journal**: a coalesced ReadIndex may return a
  conservative earlier read point. For reconstruction, submit one isolated
  test-control marker to the same Group1 after both scans, outside the audited
  prefixes. Record intent before submission, use a unique key, and require a
  positive acknowledgment with a matching transport correlation ID and a
  chosen revision beyond the initial cutoff. Record the logical request ID
  separately. An unknown outcome fails without automatic resubmission. All
  scans and replay are read-only; the marker is a separate control write.
  Fetch the complete bounded journal interval for both prefixes without
  splitting operations sharing a slot. Reject GC gaps, missing history,
  truncation, a read slot below the fence, and foreign or unordered operations.
  Retain raw scans, operations, reconstructed maps, and marker outcome.
- **P16.6 Failed stability evidence**: matching busy-allocation or `/chunk/`
  mutations must fail an unchanged-inventory guard. Capture the exact failed
  prefix and slot interval immediately, before journal GC or expensive failure
  collection, with one bounded 20-second read-only diagnostic. Retain raw and
  decoded keys, values, operations and source identities. Diagnostic failure
  preserves the original audit exception. A GC gap or truncation leaves the
  changed fields unknown. A later consistent audit is a separate qualification,
  not proof about the failed scan or permission to weaken its assertions.

## 7. Category F: first-failure diagnosis, repair, and cleanup

### P17 Failure evidence and classification

- **P17.1 First error**: print the failing case, operation, and complete error;
  enable `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` during diagnosis. Retain original
  client output rather than only a wrapper panic.
- **P17.2 Shared timeline**: retain inspect/exit/OOM, current PID/start-time,
  health timestamps, KV role/term/lease/checkpoint, catalog/transfer, DiskIO,
  and disk resources. For a cross-host timeout, distinguish connection setup,
  request transmission, and response headers/body. Match the connection to the
  current service's socket ownership. A generic client timeout alone does not
  identify its phase. A scoped two-host packet capture must retain kernel
  timestamps, clock bounds, snapshot/size limits, and capture drop counts;
  capture drops alone do not establish network loss. Keep diagnostic helpers
  outside the application's constrained cgroups, record their interference,
  and retire only their exact owned identities. Reusing an observer connection
  must still fetch a fresh response for every scheduled sample. Report a failed
  exchange before a later scheduled reconnect; do not replay a cached body,
  retry inside the sample, or extend the request timeout.
- **P17.3 Native and host evidence**: analyze crash/core before changing code
  for an abnormal exit. For I/O stalls, add diskstats/pressure and accessible
  blocked-thread stacks. Separate observations, inferences, and unproven causes.
  A later pass does not prove an old abort without a stack or a timeout without
  an endpoint label had the same cause.
- **P17.4 Complete logs**: select application, RPC, metrics, and tree logs by
  actual PID, container start, and log type. Historical stderr is not a new
  process error, but a nonempty old log must not hide new errors. Retain before/
  after evidence or a delta bound to this execution.

### P18 Repair and continuation

- **P18.1 Minimal reproduction**: find the first divergence and classify code,
  ordering/injection, observer, or environment causes. Add a focused regression
  that fails on old code, then run the affected group and relevant gates.
- **P18.2 Actual inputs**: rebuild, verify, and distribute images/clients after
  production inputs change. Repair dependencies that make later cases slow,
  hang, or fail before dispatching more work against them.
- **P18.3 Continuation ledger**: give each attempt a separate identity and
  non-overwritable receipt. Retain original failures, acknowledged writes, and
  unknown outcomes. Resetting baselines or moving stop markers cannot erase
  observer failures. Record repairs/environment checks and new audit scope
  before resuming unfinished work. Before startup or other mutations, verify
  each stop/investigation marker's origin, time, and failure. Retain an old
  marker and continue only after proving its failure repaired and current
  identities unchanged. Unknown, new, or unrepaired markers keep blocking work.
- **P18.4 Coverage boundaries**: distinguish component, container, physical,
  and soak coverage. A focused pass on a new image does not inherit all old
  coverage. An interrupted workload needs a new 30-minute run. Explain whether
  later recovery changes affect old soak inputs without repeating unaffected
  work merely to produce a same-image pass. Sharing a completed scenario
  requires matching source/epoch/image, actual fault receipts, complete
  observations, and recovered identity fences. Diagnostic sampling gaps are
  not continuous observation; a complete fault/audit interval requires a fresh
  complete scenario. Keep an unchanged-image revalidation separate from a
  causal repair: preserve the first failed attempt and carry an unexplained
  failure into the final report. A passing revalidation does not explain that
  failure or establish a long-term stability rate.

### P19 Stop, retain evidence, and retire exact resources

- **P19.1 Both-host preflight**: freeze the complete deletion inventory for
  both hosts. Verify run labels, names, container IDs, original volume names/
  CreatedAt, mounts, and final stopped states/times. Stop on any change; a
  matching name alone does not authorize deletion.
- **P19.2 Evidence after stop**: after stopping this run's nodes, retain complete
  service logs, crashes, inspect results, and data ledgers. Finish evidence
  capture on both hosts before the first deletion.
- **P19.3 Volume backup policy**: default to no additional full-volume archives
  and follow this run's user retention/cleanup instructions. Retain existing
  private backups. Create and verify extra volume backups only when explicitly
  requested. Service logs, image transfers, and volume backups serve different
  purposes and cannot substitute for each other.
- **P19.4 Exact deletion**: delete only authorized owned resources that passed
  preflight, with counts exactly matching the inventory. Do not use daemon-wide
  prune or clean shared runtime/other tasks' resources.
- **P19.5 Post-cleanup verification**: confirm this run's target resources are
  absent on both hosts, their listeners are closed, and SSH key access still
  works. Retain run artifacts, original errors, and agreed files. Disclose any
  earlier out-of-scope deletion or unrecoverable data loss in the report.

## 8. Run deliverables and acceptance conclusion

Use `.crowdb-runtime/artifacts/physical-nodes/<run-id>/`. Retain actual harness
sources and commands. No single repository Pixi task currently implements this
entire catalog; do not depend on old temporary files or describe an existing
one-command runner that does not exist.

- `run.json`: selected parent/child scenarios and extension profiles, hosts,
  node/port/resource mappings, ownership inventory, source/image/client
  identities, dirty diff, harness provenance, and build/transfer receipts.
- `results.json`: each scenario's PASS/FAIL/NOT RUN, independent gate start/
  limit/duration, actual injection trigger, unrun reason, attempt, and evidence
  paths. Never overwrite old results.
- `data-ledger.jsonl`, `metrics.jsonl`, `checkpoints.json`: data and acknowledgment
  semantics, phase statistics, fixed targets, and actual recovery progress.
  Segment large files.
- `loader/` and `audits/`: protocol source versions, generation/injection/
  verification commands, complete reports, and independent endpoint results.
  Mark unimplemented paths NOT RUN; other protocols or components cannot
  substitute for them.
- `observations/` and `first-failures/`: continuous injection-through-audit
  observations, identity fences, first errors, complete logs, crashes, and
  explicitly unproven causes.
- `cleanup.json` and `summary.md`: exact retirement, actual source-generation
  coverage, unfinished/untriggered scenarios, retention/loss facts, and next
  steps. Distinguish ending a run from passing every selected required scenario.

Conclusions cover only explicitly selected scenarios actually passed in this
run. Track human joint review separately; a test pass cannot complete it.

## 9. Implementation and regression entry points

- Startup/images: `container/oci-image/{run-node,runtime,artifact,transfer}.py`,
  `container/single-node-container/entrypoint.sh`, and
  `container/crowdb-monitor/src/node/startup/policy.rs`.
- Formation/observation: `lib/crowdb-protocol/src/mgmt/node/`,
  `app/crowdb-web/src/node/`,
  `lib/crowdb-console-shared/src/ops/cluster/readiness/`, and
  `container/crowdb-monitor/src/node/runtime/`.
- Data/recovery: `lib/crowdb-{kv,kv-client,chunk-kv-client,chunkdb-client}/`,
  `app/crowdb-chunkdb/src/topology/`,
  `lib/crowdb-chunk-stream/src/production_chunk/`, and
  `lib/crowdb-tree/src/backend/local/`.
- Components: KV backlog/FetchGap durability, ChunkStream finalized-empty,
  Monitor discovery/health/service receipts, ChunkDB allocation retry/topology
  availability, and BlockAsyncPageStore reads across extents. Select groups
  affected by actual changes and run them through `pixi run`.
- Containers/protocols: `e2e-test/tests/deployment/`, `e2e-test/tests/console/`,
  `e2e-test/tests/access_iceberg/`, and corresponding S3/native SDK clients.
  Count them separately from real cross-host faults and the continuous
  30-minute workload.
