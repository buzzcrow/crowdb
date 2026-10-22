<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RDMA physical multi-node validation Plan

Upstream: [R5](../backlog/R5-rdma-alloc.md), with transport behavior supplied
by [RDMA RPC transport](../design/rpc/design-crowdb-rpc-rdma.md) and the current
[RDMA implementation plan](plan-rdma.md).

Goal: validate the already implemented RDMA RPC and data paths across physical
nodes after the single-node, multi-container acceptance is complete. This plan
does not add transport behavior or change the R239 backlog scope.

## Scope

- one or more containers per physical node;
- one explicitly selected RDMA NIC/device and address per container;
- shared RPC framing, request correlation, fallback, and bounded buffers;
- host-provided fabric, HCA, device access, and memlock setup;
- no network-namespace manipulation in the application or test path.

The host-network port rule remains in force when this plan is executed: each
container receives a distinct port offset applied to all service ports. The
HCA physical port is not part of the offset.

## Tasks

- [ ] **Record node inventory**: capture each node's HCA, port, GID/IP
  mapping, provider libraries, and reachable peer address.
- [ ] **Prepare multi-node OCI runtime**: provide the required devices,
  userspace libraries, memlock allowance, and application-user access on each
  node.
- [ ] **Bind endpoint devices**: configure one local device/address mapping per
  container and verify that the negotiated peer identity matches the intended
  node.
- [ ] **Run transport corpus**: exercise CM/QP setup, bounded wire-slot frames,
  receive repost, request correlation, disconnect/error handling, and
  pre-payload TCP fallback across the fabric.
- [ ] **Run service corpus**: compare TCP and RDMA results for Access Server,
  DiskIO, Chunk-KV, and SDK flows byte-for-byte and by error/sequence behavior.
- [ ] **Publish topology evidence**: report node count, device mapping, QP
  count, fallback count, buffer pressure, and final result.

## Exit condition

The same RPC and service corpus passes across the available physical nodes,
with explicit device ownership and no namespace operations. Failures caused by
missing fabric or unavailable HCA remain environment blockers and do not alter
the R239 transport contract.

## Verification

Run the current R239 and service gates through `pixi run`, then execute the OCI
profile with the node inventory and device mappings recorded above.
