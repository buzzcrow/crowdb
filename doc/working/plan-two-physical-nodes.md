<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Two Physical Node Validation Plan

Upstream: [deployment design](../design/deploy/design-crowdb-deploy.md),
[OCI image guide](../design/deploy/design-crowdb-oci-image.md),
[KV quorum design](../design/kv/design-crowdb-kv-group0.md).

Goal: validate the current CI reliability fixes on two real Linux hosts using
reproducible CROWDB clusters, with cross-host discovery, key-only admission,
host-network container composition, Group 0 bootstrap, service readiness
recovery, transient KV quorum loss during allocation, and owned cleanup
evidence.

## Topology and limits

The basic two-physical-node run uses one logical node per host and one initial
voter. It proves cross-host discovery, key-only admission and durable restart;
it does not claim two-voter quorum. The composition run uses three logical
voters on one host with listener offsets `0`, `200` and `400`; this validates
that a physical host can run multiple host-network containers. A later mixed
placement can put any of those logical nodes on the second host.

- Physical host A: `u24-amd`, `192.168.31.129`, Linux 6.8, Docker 29.8.1.
- Physical host B: `cj7960`, `192.168.31.184`, Linux 6.11, Docker 28.3.3.
- Inventory evidence: `.crowdb-runtime/artifacts/two-physical-nodes-20261030T010205Z/run-manifest.json`.
- Bidirectional ICMP and SSH/22 reachability passed; CROWDB service ports are
  currently closed on both hosts.
- Remote account: `cj`; credentials are used only through SSH/`sshpass` and
  are never written to the plan, logs, environment artifacts, or reports.
- Required source/image identity: the current checkout plus a newly built
  immutable OCI image; the existing images differ in source revisions and are
  not acceptable for the final run.
- Network: direct host addresses on `192.168.31.0/24`; all advertised
  management, KV, DiskDB, DiskIO, ChunkDB, Chunk-KV, Access, Web and SSH
  endpoints must be reachable from both hosts. Host-network listener offsets
  use a stride of at least `200`: offset `100` collides with the adjacent
  default KV RPC range (`10000/10100`).

## Safety gates

- Only run containers, data directories, bridges and logs labeled with this
  run's UUID. Never reuse an existing CROWDB cluster or data root.
- Capture `docker inspect`, image receipt, host inventory, endpoint map and
  service logs before cleanup. Cleanup stops and removes only this run's
  containers, volumes, networks and remote directories.
- Do not change system firewall, routes, Docker daemon settings, kernel
  parameters, physical NIC settings or shared services. A missing port or
  privilege is a blocked prerequisite, not a reason to weaken the test.
- Keep the first workload small and bounded. Do not add retries or lengthen
  readiness deadlines to hide a failure; record the first failing operation,
  logs and persisted status, then stop the affected phase.

## Phase 1: inventory and owned fixture

- [x] **Record host inventory**: capture hostnames, IP/interface selection,
  kernel, Docker, CPU/memory, disk space, image presence, and bidirectional
  SSH/TCP reachability. Files: this plan, run manifest and artifact directory.
- [x] **Add host-network port offsets**: extend the existing
  `CROWDB_PORT_OFFSET` path to shift monitor, Web, Access, KV, DiskDB,
  DiskIO, ChunkDB, Chunk-KV and SSH listeners; persist shifted setup choices
  and use them for Group 0 endpoint publication. Verify the launcher offset
  validator and shell syntax. Files: `container/single-node-container/entrypoint.sh`,
  `container/crowdb-monitor/src/node_args.rs`,
  `container/crowdb-monitor/src/node/runtime/kv.rs`,
  `container/oci-image/run-node.py`.
- [x] **Build and distribute exact image**: build with `pixi run oci-build`,
  record source tree and image digests, transfer/import the same archive on
  both hosts, and verify the digest plus required runtime labels. The final
  flexible-port image was also built locally for the three-container run;
  transfer it to host B before a mixed-placement run.
- [x] **Prepare isolated roots**: create one run UUID, per-host data/log roots,
  a unique Docker network or host-network port map, and a machine-readable
  endpoint map. Verify no endpoint is already owned.
- [x] **Start two logical nodes**: launch one node per physical host with
  `initial_voters = 1`, distinct data roots and explicit physical-host IDs.
  Verify both candidates are visible before admission.
- [x] **Start three composed nodes**: launch three offset nodes on one
  physical host with offsets `0/200/400`, verify all listener families are
  unique, and verify all three candidates are visible before admission.

## Phase 2: bootstrap and normal cross-host behavior

- [x] **Admit and bootstrap the two-node run**: create one rack record, admit
  both discovered nodes with `ssh_password = null`, initialize one-voter Group
  0, and record node IDs, discovery IDs and cluster ID.
- [x] **Admit and bootstrap the composed three-node run**: create one rack,
  admit all three offset nodes with key-only SSH, initialize three-voter Group
  0, and verify management/RPC ports `10000/10100`, `10200/10300` and
  `10400/10500`.
- [ ] **Verify dependency readiness**: wait for KV, DiskDB, DiskIO, ChunkDB,
  Chunk-KV and Access readiness on all logical nodes. Confirm the monitor
  status is `ready` and identities/PIDs are stable after one health interval.
- [ ] **Run bounded data corpus**: perform KV put/get/batch, S3 bucket/object
  put/get, one Iceberg metadata round trip and one ChunkDB small allocation;
  verify exact bytes and read through both physical-host endpoints.

## Phase 3: failure and recovery cases

- [x] **Remote logical-node restart**: stop and recreate host B's node using
  the same data root without mounting a password file. Verify the original
  discovery identity, cluster ID, active Group 0 status and key-only SSH.
- [ ] **Remote logical-node stop**: stop only host B's CROWDB node. Verify the
  two host-A voters retain Group 0 quorum, linearizable KV writes continue,
  existing S3 reads continue, and status identifies the stopped node.
- [x] **Key-only composed restart**: recreate each of the three composed
  containers without `CROWDB_SSH_PASSWORD_FILE`; verify
  `PasswordAuthentication no`, all three dynamic KV health endpoints and
  active Group 0 status.
- [ ] **Managed listener failure**: stop or fault only the selected Chunk-KV
  listener while leaving its process ownership observable. Verify consecutive
  readiness failures trigger the monitor's bounded restart, the PID changes,
  Access becomes ready again, and no caller timeout is modified.
- [ ] **Transient allocation quorum loss**: during a bounded small allocation,
  pause one KV voter long enough to produce the known `leadership quorum
  unavailable` response, restore it, and verify ChunkDB allocation recovers
  through the allocation-only transient path. Verify outcome-unknown cases
  remain terminal and no duplicate chunk metadata is published.
- [ ] **Expected two-voter host loss**: stop host A's two logical nodes only
  after the preceding recovery cases pass. Record the expected Group 0 quorum
  loss and the exact readiness/error surfaces, then restore both nodes and
  verify durable state recovery. This is an observation case, not a pass claim
  for availability while two of three voters are down.

## Phase 4: evidence and cleanup

- [ ] **Collect evidence**: save commands, source/image identity, endpoint map,
  host inventory, cluster status snapshots, monitor events, service logs,
  process identities, workload results and first-failure diagnostics.
- [ ] **Run final health/data gate**: after all recovery cases, require all
  three logical nodes and their enabled services ready, repeat KV and object
  reads from both physical hosts, and check no unexpected process/container
  remains.
- [ ] **Cleanup owned resources**: stop/remove only this run's containers,
  networks, volumes and remote directories; verify the endpoints are closed
  and retain artifacts outside deleted data roots.
- [ ] **Report result**: mark each case pass, expected observation, failed or
  blocked with evidence. Do not mark the run complete if the image, placement,
  or cross-host path was not proven.

## Consolidated files and commands

- Plan/tracking: `doc/working/plan-two-physical-nodes.md`,
  `doc/working/track-two-physical-nodes.md`.
- Image: `container/oci-image/build.py`, `container/oci-image/run-node.py`,
  `container/oci-image/control.py`.
- Existing lifecycle references: `e2e-test/profiles/node-bootstrap.py`,
  `e2e-test/profiles/containerd-cluster.py`, `e2e-test/console_native_fixture.py`.
- Local quality gates: `pixi run cargo fmt --all -- --check`, affected Rust
  tests/clippy, and `pixi run oci-build`.
- Remote checks: `ssh`, `docker`, `docker inspect`, `docker logs`, and the
  repository's CLI/HTTP cluster status commands, all recorded in the run
  manifest.

## Verification groups

- Unit: current monitor and ChunkDB regression tests already added for the CI
  failures.
- Integration: image/launcher validation, endpoint ownership, discovery and
  SSH admission.
- Physical E2E: three logical voters over two physical hosts, normal writes,
  one-voter loss/restart, managed listener recovery, transient allocation
  recovery, expected two-voter quorum loss and durable recovery.
