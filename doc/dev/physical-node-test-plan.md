<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Physical Node Test Plan

Validate five-node deployment, data correctness, sustained load, and recovery
across two physical hosts. This is a reusable plan. Store each run's results
separately; previous passes do not establish that a new run passed.

Upstream: [deployment and recovery](../design/deploy/design-crowdb-deploy.md),
[OCI build and runtime](../../container/oci-image/README.md),
[container acceptance](../../e2e-test/README.md), and
[KV recovery](../design/kv/design-crowdb-kv-slot.md).

## 1. Default environment and boundaries

- Physical host A: `192.168.31.129`.
- Physical host B: `192.168.31.184`.
- Verify host identity, current interface, and address before each run.
  A LAN IP locates a host but does not establish resource ownership.
- Use existing SSH configuration and Ed25519 authentication; verify
  noninteractive `BatchMode`. Never record login usernames, passwords, private
  keys, access tokens, or application secrets in this plan or committed reports.
  Supply credentials through local SSH configuration/private inputs; redact logs.
- Use five fixed voters: A1/A2/A3 on A, B1/B2 on B. Port offsets are `0/1/2`
  on A and `0/1` on B. Each node has a separate persistent volume and identity.
  These are two physical host/rack failure domains, not five hosts.
- Use host networking, hard limits of `1 CPU / 2048 MiB` per container, and
  one Tokio worker per service. Record cgroup limits and any deviations.
- Simulate host failure by stopping this run's containers on that host; record
  the scope. Do not change shared firewalls, interfaces, routes, kernel settings,
  or the Docker daemon.
- A reduced three-node run must explicitly declare A1/A2/B1 and three voters.
  It cannot substitute for five-node coverage.

## 2. Execution contract

- Run every build, test, and executable through `pixi run`, including remotely.
- Create a unique run ID. Store scripts, source snapshots, receipts, clients,
  logs, and results in `.crowdb-runtime/artifacts/physical-nodes/<run-id>/`.
  Inventory volume ownership separately. Do not clean shared runtime directories
  during an active run. Check both hosts' free disk space and memory first.
- No repository task currently implements this entire plan. Save a harness
  using current public interfaces, its source, and actual commands. Do not depend
  on earlier `/tmp` files, hardcoded node UUIDs, or deleted artifacts. Do not
  present this plan as an existing one-command Pixi task.
- Finish shared builds before consumers; never build concurrently into the same
  output directory. Freeze source during OCI builds. Record commit, dirty diff,
  and relevant source identities.
- Reuse images only when service, dependency, and source inputs match. Import
  the same verified OCI on both hosts; compare execution configuration and
  ordered rootfs layers. Distinguish OCI manifest digests from Docker image IDs.
- Label resources with run ownership; verify it before faults, recreation, and
  cleanup.
- Execute phases in order. Capture the first failure and stop dependent work.
  After repair, rerun affected cases and resume unfinished work; retain valid
  passes whose inputs have not changed.
- Do not increase timeouts, weaken assertions, or add caller retries to pass.
  Poll only defined readiness/recovery conditions. Never replay timed-out
  mutations; audit uncertain writes through read-only calls.
- Use `PASS / FAIL / NOT RUN`. An untriggered fault is a coverage gap even if a
  later request succeeds. Any failed or unrun required case prevents all-pass.

## 3. Deployment and baseline data

- [ ] **P00 Environment, authentication, and image**: verify SSH key access,
  clocks, Docker/cgroups, free space, management interfaces, cross-host listener
  reachability, and existing resource owners. Save redacted inventory, source
  identity, build receipts, and same-image import verification on both hosts.
- [ ] **P01 Five-node discovery and initialization**: form five voters through
  normal startup policy, discovery, key admission, and production storage
  preparation. Verify five distinct discovery UUIDs/node IDs, accepted
  `initial_voters = 5`, fixed membership, and real listener addresses. Apply
  offsets exactly once; check same-host conflicts and cross-host loopback.
  Currently `run-node.py --initial-voters` accepts only `1/3`; use the supported
  explicit node-policy/controlled container path for five voters and verify
  Monitor's accepted result. Do not invent a working `--initial-voters 5` command.
- [ ] **P02 Whole-cluster readiness**: verify seven managed services per node,
  35 total, including counts, live processes, health, and dependencies. All five
  Web endpoints must report Ready. Monitor observations must postdate this
  container start and be no older than three seconds; old snapshots are invalid.
  Measure age when receiving the snapshot and record that receipt time. Verify
  current DiskDB ownership leases; live owners must renew expiring leases
  without changing their process epoch.
- [ ] **P03 Baseline data and allocation**: check five-replica mappings for
  Group 0, metadata Group 1, and explicit data Group 3. Wait for current leaders/
  leases before single-attempt operations. Verify exact KV single/batch writes;
  deterministic S3 objects of at least 96 KiB with byte/hash checks; Iceberg
  namespace/table/property commits and metadata locations; and native ChunkDB
  allocation/query of the same ID. Cross-read through all five endpoints. Use
  `crowdb-tpc-loader` for generated Iceberg and standalone S3 data, plus generated
  Dataset samples with inline fields, multipart payloads, and complete readback.
  Record nodes, groups, objects, and acknowledgments without application secrets.

## 4. Write scale and 30 minutes of sustained load

- [ ] **P04 Short 512-record reproduction**: use native ChunkKV defaults.
  Write 32 batches of 16 records, each 64 KiB: 512 records/32 MiB across five
  owners. Read the complete catalog first; generate run-unique keys within
  actual ranges and verify routed owners. Exclude reserved S3, Iceberg, and
  Dataset metadata prefixes; appending a test suffix to a protocol key still
  corrupts that protocol's scan namespace. Select safe ranges covering all five
  owners, or fail preparation. Uniform first bytes do not establish
  coverage. Test the old key shape separately and report its coverage.
  Preserve the default five-second deadline per batch. Verify every mutation
  acknowledgment and full readback; save catalog split/cutover state, latency,
  and results. Audit failed/uncertain writes read-only without replay.
- [ ] **P05 Larger working set**: after P04 passes, seed a fresh namespace with
  at least 4096 deterministic 64 KiB records, 256 MiB total, across five Serving
  owners. Budget both mirrors, metadata, WAL, reclamation, and artifacts; retain
  at least 30% free test-media capacity. Expand only owned test media or fail
  preparation if insufficient; do not silently shrink the working set. Record
  key counts, acknowledged bytes, and verification digests, not just API success.
- [ ] **P06 Sustained workload**: run **30 continuous minutes** on P05's data,
  excluding builds, deployment, and idle waiting. Mix native ChunkKV inserts,
  overwrites, deletes, and readback across both hosts and all five owners.
  Concurrently exercise S3 reads/writes/deletes, Iceberg commit/load/cleanup, and
  Dataset sample reads/publication. Declare concurrency, ratios, pacing, and
  working-set limits before starting. Continuously produce successful operations
  while reusing/reclaiming bounded data; a fixed load followed by waiting does
  not qualify.
- [ ] **P07 Soak data verification**: journal each mutation's operation ID,
  key/version, expected length/hash, acknowledgment state, and timestamp.
  Serialize overwrites per key or use an explicit version contract to prevent
  verifier races. Continuously sample acknowledged data; finish with a complete
  audit of live keys and expected deletions. Journal timeout/connection-loss as
  `OutcomeUnknown` and audit it, rather than declaring it uncommitted. Any
  mismatch, request failure, OOM, or unexpected process replacement fails the
  normal load phase.
- [ ] **P08 Load observations**: sample throughput, success/failure/unknown
  counts, p50/p95/p99/max latency, CPU, RSS/cgroup memory, free space, service
  PIDs, catalog, and reclamation at least every five seconds. Report acknowledged
  operations/bytes, actual active duration, and resource changes. Live containers
  alone do not prove a pass. Keep different load-stage latencies separate.

## 5. Faults and recovery using the soak data

Before each case, save identities, PIDs, catalog, data inventory, and replica
status. After recovery, verify P03's baseline, all P07 data, and fresh
acknowledged writes from the current case.

- [ ] **P09 Retained-volume recreation**: recreate all five containers with
  original volumes, startup policies, and accepted identities. Verify unchanged
  UUIDs/node IDs, 35 recovered services, readback through every endpoint, and
  all replicas caught up. Repeat affected recovery after production image fixes.
- [ ] **P10 Unresponsive child process**: send `SIGSTOP` only to one verified
  ChunkKV child. Monitor must recover it with a new PID within 60 seconds; the
  other 34 service PIDs must remain unchanged. Verify old data and fresh
  single-attempt operations. Clear injected faults on failure paths too.
- [ ] **P11 Cross-host KV lifecycle**: from a Web endpoint with an available
  remote leader, stop/restart its local follower. Separately stop the current
  leader. Verify Stop receipts, actual termination, continued remote management,
  a new PID, and no collateral restarts. A completed Stop receipt must remain
  readable after the operation stops the former Group-0 leader.
- [ ] **P12 Both B nodes offline**: stop B1/B2; A retains three-voter quorum.
  Verify single-attempt KV writes/reads and catalog takeover onto surviving
  Serving owners within 180 seconds. While B stays offline, submit fresh S3
  writes and Iceberg property commits through A, then cross-read. Use new
  witnesses per fault to prove new writes occurred. Restore B, then recreate
  B1/B2 individually with retained volumes. Check original identities, all data
  through five endpoints, and all five replicas caught up.
- [ ] **P13 All three A nodes offline**: stop A1/A2/A3; B loses quorum.
  Observe current unavailability within 30 seconds; writes must not acknowledge
  success. Audit unknown outcomes after recovery without asserting absence.
  Restore A; verify every service, five-replica checkpoints, and complete data,
  not just a new leader.
- [ ] **P14 Allocation-only safe retry**: inject bounded Group-1 leadership
  step-down while preserving KV processes and the Group-0 lease. Within three
  seconds, verify the leader accepted at least one step-down. Observe explicit
  allocation-level `Unavailable` safe retry. After release, complete same-ID
  allocation/query within the original five-retry/ten-second limit. Record
  actual retry evidence and original limits; success alone is insufficient.
  Use the production client; instrumentation must not change branches or policy.
- [ ] **P15 Large backlog and full cold restart**: use normal acknowledged
  writes to leave a controlled offline KV replica more than 1024 slots behind.
  Record group, start/end checkpoints, and actual gap. Restore it and verify
  bounded-concurrency FetchGap/leader recovery progress, then cold-start all
  five retained-volume nodes. Fix acknowledged targets for Groups 0/1/3;
  require all five replicas per group to reach them with healthy engines and
  exact data. Do not move targets with ongoing writes. Report actual coverage
  of reclaimed WAL, empty sealed WAL, high promise/old chosen, and other recovery
  boundaries; component regressions do not prove physical coverage.
- [ ] **P16 Final recovery gate**: require all catalog entries Serving, all five
  endpoints Ready, and 35 healthy services. The next sample must show the same
  normal PIDs. Audit all data; complete a fresh KV batch and fresh ChunkDB
  allocation/query. Fully scan owned DiskDB occupancy read-only; verify no
  cross-chunk overlap among live mirrored segments and no scan truncation.

Measure recovery budgets independently: catalog takeover 180 seconds, service
recovery 180 seconds, and fixed-checkpoint catch-up 180 seconds per group.
Record each condition's own start and elapsed time. Whole-phase duration cannot
prove individual deadlines. Preserve shorter production-client limits.

## 6. Failure evidence, repairs, and cleanup

- [ ] **P17 Failure capture**: print failing cases, operations, and complete
  errors. Use `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` while debugging. Save client
  output, service logs, current inspect data, exit/OOM state, health freshness,
  KV roles/leases/checkpoints, catalog/transfers, DiskIO availability, and disk
  usage. Export allowlisted/redacted fields without credentials.
- [ ] **P18 Repair verification**: locate the first divergence and root cause.
  Add focused regressions and run affected component tests plus fmt/clippy.
  Rebuild/verify images when production inputs change. Rerun affected cases
  under the same resources and limits. After repairing P04, complete P05–P08;
  512 records alone do not finish testing. Repairs or harness restarts during
  the soak require a complete new 30-minute run after the fix.
- [ ] **P19 Freeze and cleanup**: stop owned containers before freezing full
  service logs. Delete only inventoried containers, volumes, and transfer files
  with verified ownership. Check owned ports are closed on both hosts, no owned
  resources remain, and SSH key access still works. Retain run artifacts.
  Do not prune the daemon or clean other tasks' resources. Capture failures
  before cleanup.

## 7. Deliverables and the next run

Save these artifacts and link them from the final summary:

- `run.json`: run ID, source/image identities, both IPs, node/port/resource
  mapping, ownership inventory, actual commands, and harness source locations.
  No login credentials.
- `results.json`: P00–P19 states, times, limits, errors, evidence paths, and
  unrun reasons. Preserve first failures separately from repaired results.
- `data-ledger.jsonl`, `metrics.jsonl`, and `checkpoints.json`: acknowledged/
  uncertain outcomes, load metrics, fixed targets, and actual catch-up.
  Segment large files if needed.
- `cleanup.json` and `summary.md`: cleanup verification, actual passing
  coverage, unresolved defects, and next starting point. Distinguish "run ended"
  from "all required tests passed." CI does not replace physical results.
- `loader/`: loader source identity, generation/injection/verification commands
  and reports, Iceberg tables/snapshots, S3 inventory, immutable Dataset snapshots,
  and sample verification summaries. Confirm implementation and acceptance of
  all three targets before use; mark unimplemented paths `NOT RUN`.

Open issue requiring retest (2026-10-31): five containers writing
512 records × 64 KiB reached the default five-second deadline during
split/cutover. The previous run acknowledged and recovered only 294 records,
not a full load. Reproduce, repair, and fully pass P04; then seed at least
4096 records and run 30 continuous minutes. Remove this open item only after
successful repair and retest, preserving results in its run directory.

Suggested request for a future run:

> Execute the full five-node test in `doc/dev/physical-node-test-plan.md` with
> the two LAN hosts and existing SSH keys. Run continuously for 30 minutes.
> Resolve open issues first, capture and repair failures, then resume.
> Do not reduce coverage or relax limits.

## 8. Implementation and regression entry points

- Startup/images: `container/oci-image/{run-node,runtime,artifact,transfer}.py`,
  `container/single-node-container/entrypoint.sh`, and
  `container/crowdb-monitor/src/node/startup/policy.rs`.
- Formation/observations: `lib/crowdb-protocol/src/mgmt/node/`,
  `app/crowdb-web/src/node/`,
  `lib/crowdb-console-shared/src/ops/cluster/readiness/`, and
  `container/crowdb-monitor/src/node/runtime/`.
- Data/recovery: `lib/crowdb-{kv,kv-client,chunk-kv-client,chunkdb-client}/`,
  `app/crowdb-chunkdb/src/topology/`, and
  `lib/crowdb-chunk-stream/src/production_chunk/`.
- Component regressions:
  `lib/crowdb-kv/tests/group_test/{recovery_backlog,fetch_gap_durability}_test.rs`,
  `lib/crowdb-chunk-stream/tests/production_chunk_test/finalized_empty.rs`,
  `container/crowdb-monitor/tests/{member_discovery,node_service_receipt}_test.rs`,
  and `app/crowdb-chunkdb/tests/{allocation_retry,topology_availability}_test.rs`.
- Container/protocol regressions: `e2e-test/tests/deployment/`,
  `e2e-test/tests/console/`, `e2e-test/tests/access_iceberg/`, and corresponding
  S3/native SDK acceptance entries. Select tests for actual changes and use
  `pixi run`. Distinguish component, container, and real cross-host coverage.
