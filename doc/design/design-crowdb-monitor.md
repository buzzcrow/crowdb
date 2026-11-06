<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Cluster Management

`crowdb-monitor` discovers nodes, supervises local service processes, and executes deployment work. Paxos-KV Group 0 stores the durable system metadata that governs membership, registrations, and ownership. The Web UI and CLI submit management operations through shared console logic.

A process can be alive while it is recovering, fenced, or unable to serve its assigned data. The management design keeps those conditions visible instead of treating process existence as complete readiness.

## Local execution and durable decisions

Group 0 is the Paxos-KV system group. It stores system metadata, including hardware topology, service registrations, membership, and ownership. The monitor performs local work under that control model: preparing resources, starting services, observing them, and releasing resources when their lifecycle permits it.

Discovery supplies candidates and observations. It does not by itself admit a machine, transfer ownership, or replace durable identity. This matters when a machine restarts with old local state or a previously reachable process appears again after a partition.

Bootstrap establishes the initial control services and then the storage services that depend on them. A prepared deployment can advance as its dependencies become ready. Recovery can inspect existing state and resume the relevant work without treating every restart as a new cluster.

## Management interfaces share operations

`crowdb-console-shared` implements common management operations and validation. `crowdb-web` presents them through HTTP and the browser UI; `crowdb-cli` presents commands, output, and exit status.

The interfaces can differ in interaction without inventing different meanings for a disk removal or service deployment. Business authority remains with the service that owns the underlying state. An operator's request enters that service's lifecycle rather than rewriting observed state in the UI.

When an operation stalls, operators can inspect its requested state, durable ownership, observed process, and readiness separately. A running process alone does not prove that recovery or an ownership transfer has completed.

## Service lifecycle inside a node

The monitor owns the service processes inside a node container: it starts them, observes readiness, coordinates shutdown, and restores the appropriate processes after restart. Each storage service recovers its own data and validates its serving authority. The container runtime establishes the node's CPU and memory budget and attaches host resources.

Retaining data and accepted identity across restart is essential. A recreated container reopens the intended node state; a fresh image does not authorize initialization over existing storage. The [container deployment design](design-crowdb-deployment.md) explains OCI packaging, runtime independence, host networking, block devices, and resource limits.

## Why integrate management with storage?

Deployment changes who can serve data and which physical resources exist. Keeping these operations connected to durable metadata makes it possible to recover their intent after a worker or management process fails. It also prevents an old process observation from becoming a new ownership decision.

The cost is coordination between persistent state and external effects such as launching a process or attaching storage. Those effects still need observation, retry handling, and cleanup. CROWDB gives that work explicit owners instead of leaving each command to improvise recovery.

Implementation entry points are the [monitor](../../container/crowdb-monitor/src), [shared console operations](../../lib/crowdb-console-shared/src), [Web application](../../app/crowdb-web), and [CLI](../../app/crowdb-cli). The [E2E guide](../../e2e-test/README.md) describes deployment and restart validation scenarios.
