<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA physical multi-node validation Plan

Upstream: [RDMA transport](../design/rpc/design-crowdb-rpc-rdma.md),
[native SDK](../design/access-server/design-crowdb-native-sdk.md).

Goal: validate the existing RPC, storage and native SDK across distinct physical
hosts, with reproducible fabric, resource and failure evidence.

Status: independently scheduled follow-up. This request refines the plan only;
no remote host, fabric or cross-host benchmark is changed or executed. Completion
of the local RDMA implementation and Dataset client work does not depend on this
plan. Hardware inventory must be supplied when execution is scheduled.

## Scope and prerequisites

- At least two distinct physical hosts with reachable peer HCA addresses.
  Record host identity, NIC model/firmware, provider/kernel versions, link rate,
  MTU, port/GID/IP mapping, NUMA placement and switch/fabric settings.
- Use the same source revision, native library hashes and immutable OCI image
  digest on every host. Keep data directories, service IDs, endpoints and
  cleanup ownership separate for this run.
- Host configuration supplies devices, memlock and non-root access. Application
  and test code do not move links or enter other network namespaces. Changes
  to shared fabric settings require separately authorized operator work.
- Each container receives an explicit device/address and nonoverlapping service
  and CM ports. Benchmark port offsets and real-service explicit endpoints must
  produce a recorded complete endpoint map; HCA physical port remains unchanged.
- Missing hosts, HCA access or peer reachability leaves this plan pending. It
  cannot be replaced by localhost, two containers on one host or a TCP-only run.

## Phase 1: inventory and remote fixture ownership

- [ ] **Capture inventory**: write one machine-readable manifest per host and
  a run manifest tying physical hosts, NICs and peer endpoints together. Probe
  device access under the application UID, available registration capacity and
  bidirectional reachability. Files: `tools/rdma/`, `e2e-test/`.
- [ ] **Extend the explicit launcher**: reuse immutable image/client bundles,
  source checks, container labels, resource limits, reports and cleanup. Record
  the ownership of every remote process and directory; cleanup only this run.
  Preserve logs on startup failure and interruption. Files:
  `tools/benchmark/rpc_bench_process.py`, `tools/rdma/`, `e2e-test/`.
- [ ] **Verify actual peer placement**: correlate TCP preflight/CM addresses,
  QP identity and NIC port counters with the distinct hosts. Reject a result
  whose endpoints accidentally connect within one host. Files: launcher and
  structured transport observations.

## Phase 2: transport correctness

- [ ] **Run exact-byte corpus**: cover empty/small payloads, 64 KiB/256 KiB/1 MiB
  classes, framing boundaries and logical controls beyond one receive window.
  Run TCP, strict RDMA, single/multiple QPs and producers, packing off/on, idle
  singleton and several batches in flight. Preserve client request counts and
  per-request validation; SEND/CQE counts are separate observations. Files:
  `tools/benchmark/rpc_regression.py`, `lib/crowdb-rpc/tests/`.
- [ ] **Exercise bounded pressure**: hold receive leases, saturate WR and byte
  windows, then release capacity. Report posted receives, SQ occupancy,
  registered bytes, held bytes and queue high water. All admitted owners must
  remain valid until completion/release, including cancellation and shutdown.
- [ ] **Inject owned failures**: stop only this run's peer or selected service;
  cover setup failure, stale generation and accepted partial delivery. Eligible
  fallback preserves identity; partial reads fail terminally and ambiguous
  mutations are not replayed. Do not flap a shared physical link to simulate
  failure. Retain exact failing case and transport counters.

## Phase 3: real services and embedded consumers

- [ ] **Run distributed service corpus**: place real metadata/Chunk-KV/DiskIO
  services and clients on distinct hosts, using service-discovered routing.
  Verify HTTP, native Rust and unprivileged OCI results before and after owned
  service restarts. Cover conditional errors, generation selection, copy,
  multipart, mixed sizes and physical read integrity. Files:
  `e2e-test/tests/access_s3/native/`, `tools/rdma/run-native-s3.py` and fixtures.
- [ ] **Verify caller lifetime**: retain registered read leases across stream
  close and runtime shutdown while simulating asynchronous host-copy completion;
  verify bounded producer/held capacity and final release. Record actual RDMA
  readiness, fallback, QPs, allocation classes and copy counters per process.
- [ ] **Attach accepted Dataset corpus when available**: reuse native Dataset
  acceptance without reimplementing its authority, Snapshot/Split selection or
  acknowledgement. Keep this optional subcase explicit if Dataset is not yet
  available; it cannot silently count as executed.

## Phase 4: comparable performance and report

- [ ] **Run pinned baselines**: use fixed physical cores/NUMA, the same message
  distribution/seed, loader concurrency, QP count, warmup/duration/repeats and
  payload-copy behavior for TCP and RDMA. Start with the checked-in mixed-size
  profile, then include recorded application-size distributions when supplied.
  Files: `tools/benchmark/profiles/rdma-mixed-size.json`, regression runners.
- [ ] **Report throughput and tails**: count completed client requests, bytes/s,
  p50/p99/p999 by size, client+server CPU/request, memory and errors. Separate
  closed-loop concurrency tests from intended-arrival open-load latency. Never
  compare unrestricted cores against pinned runs as one performance gain.
- [ ] **Publish reproducible evidence**: retain source/image/binary identity,
  full commands, host/fabric manifest, actual path/counters, failures and raw
  trials. Explain remaining copies and hardware limitations. Do not impose an
  invented TPS gate; record repeatability and latency/resource trade-offs.

## Verification and exit

- Unit: remote fixture selection, ownership and cleanup; incompatible/missing
  prerequisites; truthful report classification and endpoint maps.
- Integration: bounded transport corpus, pre-payload fallback, partial failures,
  shared-owner retention and no-device TCP regression.
- E2E: at least two physical hosts, real services, native/non-root OCI consumers,
  exact results, restart/failure cases and retained-lease pressure.
- All strict-RDMA success cases require zero workload fallback, validation and
  transport errors. Expected injected failures are identified separately, with
  no successful partial result or premature owner reuse.
- Final report must prove cross-host execution and explain each skipped scope.
  Missing environmental prerequisites remain pending with their exact evidence.

Use the current gates through `pixi run`; launcher changes require fixture tests
and affected Rust/C++ formatting/lint/tests. Concrete remote commands are added
after inventory and endpoint selection, rather than embedding assumed hosts.
