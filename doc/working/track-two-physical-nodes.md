<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Two Physical Node Test Track

Plan: [plan-two-physical-nodes.md](plan-two-physical-nodes.md)

Run ID: `two-physical-nodes-20261030T010205Z`

## Preconditions

- [x] Local host inventory captured.
- [x] Remote host inventory captured through SSH.
- [x] Direct peer reachability and SSH reachability verified.
- [x] Host-network port-offset support verified by focused test, clippy and
  shell syntax checks.
- [x] Current source/image identity recorded and identical on both hosts.
- [x] Final flexible-port image built locally: digest
  `sha256:7f4c32b59e3ea9315a7726f6667ae6d5f4ea795f70c29c920371d045a80cf42d`.
- [ ] Final flexible-port image transferred to host B for mixed placement.
- [x] Isolated run UUID, roots, network and endpoint map created.

## Bootstrap

- [x] Two logical nodes launched: host A ×1, host B ×1.
- [x] Two candidates discovered and admitted with key-only SSH.
- [x] One-voter Group 0 initialized and remote node restarted without a password file.
- [x] Three logical nodes launched on one host with offsets 0/200/400.
- [x] Three candidates discovered and admitted with key-only SSH.
- [x] Group 0 initialized with three voters and all dynamic KV listeners healthy.
- [ ] All required services ready on all logical nodes.
- [ ] Baseline KV/S3/Iceberg/ChunkDB corpus passed.

## Recovery matrix

- [ ] Stop host B logical node: two-voter quorum remains available.
- [ ] Restart host B logical node: identity and data retained.
- [ ] Managed Chunk-KV listener failure: monitor restarts unhealthy service.
- [ ] Transient allocation quorum loss: allocation-only recovery succeeds.
- [ ] Stop host A's two logical nodes: expected quorum loss recorded.
- [x] Restore the two-node run and all three composed nodes with key-only SSH.

## Evidence and exit

- [x] Status snapshots, process identities and monitor events saved.
- [x] Service/container logs saved for every phase.
- [ ] Final cross-host reads and health gate passed.
- [ ] Owned resources cleaned; endpoints closed.
- [ ] Result report completed with pass/expected/failed/blocked classification.
