<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Two Physical Node Validation Plan

Upstream: [deployment design](../design/deploy/design-crowdb-deploy.md),
[OCI image guide](../design/deploy/design-crowdb-oci-image.md),
[KV quorum design](../design/kv/design-crowdb-kv-group0.md),
[Chunk-KV recovery design](../design/chunkds/design-crowdb-chunk-kv-server.md).

Goal: validate discovery, key-only admission, host-network composition,
service readiness, bounded data operations and recovery on two real hosts.
Results and receipts: [track-two-physical-nodes.md](track-two-physical-nodes.md).
The physical three-container matrix and requested CI selections pass. The plan
remains open for authorized publication and the five-container physical follow-up.

## Fixture and constraints

- Host A: `u24-amd`, `192.168.31.129`; host B: `cj7960`, `192.168.31.184`.
- Final placement: two logical voters on A, one on B; host-network offsets
  `0/1` on A and `0` on B. Each node owns a separate labeled volume.
- All remote work now uses `cj` and `id_ed25519`, key-only SSH. Existing
  authorized keys are preserved; passwords are absent from run artifacts.
- Build and execute through `pixi run`. Record immutable OCI receipts and
  compare execution configuration plus ordered rootfs layers on both hosts.
- Never alter shared firewall, Docker, NIC, routes or kernel settings.
  Stop/remove only resources verified to carry the exact run label.
- Preserve the first failing operation and complete logs. Do not lengthen
  deadlines or add caller retries to turn a failure into a pass.

## Completed prerequisites and baseline

- [x] **Inventory and reachability**: capture hosts, interfaces, Docker versions,
  resources, endpoint ownership and cross-host SSH/TCP reachability.
- [x] **Repair host SSH**: install the existing public key on B, preserve other
  keys, verify permissions and key-only `BatchMode` login.
- [x] **Compose offset nodes**: verify all canonical listener blocks and
  offset-resolved setup choices. Local offsets `0/1/2` reached active Group 0.
  Files: `container/oci-image/run-node.py`,
  `container/single-node-container/entrypoint.sh`,
  `lib/crowdb-protocol/src/mgmt/node/deployment.rs`,
  `container/crowdb-monitor/src/node/startup/policy.rs`.
- [x] **Persist accepted admission choices**: use the target-resolved choices
  for endpoint publication when the request omits setup. Files:
  `app/crowdb-web/src/node/{admission,setup}.rs`.
- [x] **Separate DiskDB listener blocks**: support explicit main/HTTP/RPC ports,
  shift each once, retain custom legacy base behavior, and prove adjacent
  instances disjoint. Files: protocol node setup/validation/deployment,
  Web DiskDB lifecycle/configuration and planned deployment, monitor policy.
- [x] **Exact mixed-placement image and bootstrap**: import the verified image
  on both hosts, admit all three nodes without passwords, initialize
  three-voter Group 0, provision production disk media, and reach seven
  healthy services per node. Files: attempt-12 receipts and snapshots.
- [x] **Bounded baseline corpus**: KV put/get/batch, 96 KiB S3 object,
  Iceberg metadata, and a small mirrored ChunkDB allocation; cross-host exact
  reads. Files: attempt-12 corpus and native-client evidence.
- [x] **Retained-volume recreation**: recreate all three nodes with the
  corrected image and original identities, without a password-file mount.

## Recovery verification and remaining work

- [x] **Managed listener recovery**: fault one owned Chunk-KV child, require a
  changed PID within the existing bound, all services ready and cross-host
  corpus reads. Strengthen Access startup to probe every catalog owner before
  another launch. Files: monitor service dependencies and regression tests.
- [x] **Allocation retry contract regression**: use real RPC responses to prove
  known `Unavailable` retries the same chunk ID and a lost reply submits once.
  File: `app/crowdb-chunkdb/tests/allocation_retry_test.rs`.
- [x] **Observe remote loss**: stop B, verify KV quorum write/read, retain the
  S3 `ServiceUnavailable` result and failed 180-second catalog failover wait,
  then restore B. This completed observation is a failed availability case.
- [x] **Diagnose recovery defects**: reproduce the unfinished owner-2-to-owner-1
  `TargetPreparing` transfer and explain why it cannot finish; inspect the
  planner's busy-owner exclusion and the target preparation dependency.
  DiskDB stale-owner discovery and released-source WAL/grant fixes pass focused
  regressions. Attempt 15 also reproduced empty sealed-WAL recovery rejection;
  the production adapter regression passes. The upgrade then exposed reclaimed-journal recovery losing live DiskDB occupancy; KV and DiskDB restart regressions now pass. Preserve the corrupted attempt-15 evidence and run a fresh dynamic-ownership matrix after rebuilding. Files: Chunk-KV transition worker/monitor, DiskDB ownership lifecycle,
  and focused regressions selected from first divergence evidence.
- [x] **Repeat physical allocation retry**: attempt 15 retained three voters,
  injected Group-1-only leadership unavailability, observed the explicit
  allocation-level `Unavailable` retry, and completed same-ID allocation/query
  within the original five-retry/10-second limit. The KV processes and Group 0
  stayed live. Earlier process-pause attempts are separate failed observations.
- [x] **Repeat remote storage recovery**: after repairing the defects, stop B,
  require bounded range recovery and one-shot S3/Iceberg reads, recreate B with
  its volume/key-only admission, and recheck identities and exact data.
- [x] **Repeat expected two-voter host loss**: after preceding recovery passes,
  stop both A nodes, record expected Group-0 unavailability, restore them and
  verify all services and durable data. Earlier-image evidence is historical.
- [x] **Pass final health/data/allocation gate**: require stable healthy PIDs,
  existing KV/S3/Iceberg reads, a fresh batch and fresh allocation/query.
  Attempt 12 passed restored health, existing reads and batch, but failed fresh
  allocation with `disk-group not owned`.

## Evidence and cleanup

- [x] **Freeze diagnostics**: retain status, inspect, policies, receipts,
  transitions, workload errors and complete logs. Capture final log archives
  after container stop to avoid concurrent-write tar failures.
- [x] **Clean exact owned resources**: remove attempt-12 through attempt-16 containers and volumes,
  delete its remote transfer archives, verify all owned endpoints closed,
  retain artifacts outside deleted volumes and retain host-B SSH access.
- [x] **Report actual coverage**: distinguish passes, failed recovery cases,
  earlier-image observations and the physical retry coverage gap.

## Verification groups

- Protocol: node deployment/setup contract tests and full crate tests.
- Web: DiskDB deployment configuration tests and full crate tests.
- Monitor: node policy, service dependencies and service readiness tests.
- ChunkDB: allocation retry tests and dynamic retry tests.
- Quality: Rust fmt, affected crates' clippy with warnings denied, launcher
  Python tests and shell syntax; immutable OCI build for production changes.
- Physical: repeat only the affected matrix after the first failures are
  repaired, preserving successful baseline evidence and default limits.

## After physical validation: requested CI jobs

- [x] **Fetch requested CI evidence**: retain all eight job logs and map the
  exact failed cases in run 41003611925 after completing physical attempt 16.
- [x] **Repair retained Monitor fixtures**: accept valid startup policy before
  identity, resume and cleanup operations. The exact two failures pass; the
  earlier passing Monitor targets and the remaining targets are retained.
  Files: `container/crowdb-monitor/tests/{kv_resume,node_cleanup}_test.rs`.
- [x] **Repair common Console/bootstrap policy**: the exact inspection case
  reproduces container exit due to disabled Paxos KV, followed by no route to
  host. Keep mandatory KV listeners enabled while skipping storage setup;
  verify the original case and affected Console/deployment selections.
  Files: `e2e-test/profiles/setup_choices.py`, native policy, bridge-node and
  reference-recovery fixtures.
- [x] **Preserve live services during readiness recovery**: native inspection
  passed serially after bootstrap repair but a concurrent-load run exposed a
  monitor restart during normal transfer readiness withdrawal. Recorded HTTP
  evidence distinguishes `/ready` 503 from `/health` 200. Keep readiness for
  admission/status and liveness for restart decisions; preserve dead-listener
  restart assertions and verify the new image with the affected case.
  Files: monitor runtime health/probes and service readiness regression.
- [x] **Verify Go SDK failure cleanup**: focused and complete S3 selections
  pass. Retain safe operation/error diagnostics; the original opaque CI
  cleanup cause cannot be established from its artifact.
- [x] **Skip ordinary metadata for diskless KV-only setup**: the mandatory
  Group-0 prerequisite must not reserve a default Group 1. Reproduce the
  metadata race, retain both disabled/KV-only unit coverage, repair the
  preparation predicate, rebuild and resume the stopped native-data group.
- [x] **Resolve remote wildcard topology addresses**: HTTP Console topology
  reports must inherit the reporting server management host before entering
  the shared cache. Reproduce false loopback Group-0 authority loss after
  stopping a local follower, reuse the existing endpoint resolver, add an
  HTTP/cache regression and rerun the affected lifecycle case.
- [x] **Publish canonical protocol listeners with retained-input compatibility**:
  repair Pixi/CLI/Compose mappings and image metadata, retain version-1 launch
  ports while sealing fresh version-2 inputs, verify mapping/receipt regressions
  and CLI quality gates; the image rebuild is complete before reference verification.
- [x] **Complete requested Console partitions**: browser, native-data,
  native-operations, transition and inspection, with original coverage and
  fail-fast progression. Keep browser fixture networks isolated from other jobs.
- [x] **Complete deployment reference**: rerun its original case set after the
  shared bootstrap repair, then diagnose any remaining failure.
- [x] **Close AccessTests and quality gates**: run the requested Access component
  and SDK commands once, fmt, affected clippy and required C++ lint before the
  first coherent commit/push.
- Job IDs: `123893403740`, `123899615935`, `123899616023`, `123899615992`,
  `123899616153`, `123899616076`, `123899616075`, `123899615882`.
- Run: https://github.com/buzzcrow/crowdb/actions/runs/41003611925

## Authorized publication and five-container follow-up

- [~] **First publication**: user authorized a single coherent commit and push
  after the physical matrix, requested CI repairs and relevant gates pass.
- [ ] **Five-container physical matrix**: after that push, run a fresh cluster on the same two physical
  hosts, retaining immutable image receipts, cross-host data witnesses, fault
  and recovery results, and exact owned-resource cleanup.
- [ ] **Final publication**: repair failures and verify affected groups, then commit and push the
  resulting five-node validation changes and evidence summary a second time.
