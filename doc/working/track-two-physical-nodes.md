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
- [ ] Isolated run UUID, roots, network and endpoint map created.

## Bootstrap

- [ ] Three logical nodes launched: host A ×2, host B ×1.
- [ ] Three candidates discovered and admitted.
- [ ] Group 0 initialized with three voters.
- [ ] All required services ready on all logical nodes.
- [ ] Baseline KV/S3/Iceberg/ChunkDB corpus passed.

## Recovery matrix

- [ ] Stop host B logical node: two-voter quorum remains available.
- [ ] Restart host B logical node: identity and data retained.
- [ ] Managed Chunk-KV listener failure: monitor restarts unhealthy service.
- [ ] Transient allocation quorum loss: allocation-only recovery succeeds.
- [ ] Stop host A's two logical nodes: expected quorum loss recorded.
- [ ] Restore host A and host B: full cluster and data recovery passed.

## Evidence and exit

- [ ] Status snapshots, process identities and monitor events saved.
- [ ] Service/container logs saved for every phase.
- [ ] Final cross-host reads and health gate passed.
- [ ] Owned resources cleaned; endpoints closed.
- [ ] Result report completed with pass/expected/failed/blocked classification.
