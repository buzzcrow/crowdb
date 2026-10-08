<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Multi-node Deployment

This document defines the target architecture for node discovery, Group-0
bootstrap, shared console control and light-container deployment. It is a design
contract under review, not a claim that LAN discovery or containerd lifecycle
integration is implemented. Unresolved mechanisms are identified explicitly.

The [console architecture](../console/design-crowdb-console.md) defines the
shared operation layer; the [Group-0 architecture](../kv/design-crowdb-kv-group0.md)
defines system metadata; the
[reconfiguration design](../kv/design-crowdb-kv-reconfiguration.md) governs
Paxos voting membership. This design connects those responsibilities without
making node discovery a membership protocol.

## Table of Contents

- [1. Goals and boundaries](#1-goals-and-boundaries)
- [2. Components and identities](#2-components-and-identities)
- [3. Invariants](#3-invariants)
- [4. Node discovery](#4-node-discovery)
- [5. Node and rack configuration](#5-node-and-rack-configuration)
- [6. Bootstrap and recovery](#6-bootstrap-and-recovery)
- [7. Shared UI and management operations](#7-shared-ui-and-management-operations)
- [8. Authorization and partitions](#8-authorization-and-partitions)
- [9. Image and runtime deployment](#9-image-and-runtime-deployment)
- [10. Resource and lifecycle management](#10-resource-and-lifecycle-management)
- [11. Configuration and observability](#11-configuration-and-observability)
- [12. Correctness and validation](#12-correctness-and-validation)
- [13. Decisions still required](#13-decisions-still-required)

## 1. Goals and boundaries

- Each node has a monitor and a Web UI. Operators can use any member's UI;
  there is no permanently designated UI host.
- Monitors discover nodes on a management LAN without a hand-written peer list.
  Discovery identifies nodes, not the individual servers hosted on them.
- Before Group 0 is initialized, UI operations are restricted to node/rack
  preparation and Group-0 bootstrap. Other server deployment requires a ready
  Group 0 and confirmed initial topology.
- Group 0 is the shared information platform and authority for cluster
  membership, racks and deployment operations.
- Production deployment uses Linux light containers: OCI images package
  binaries/dependencies, while network, disks and performance resources are
  explicitly assigned from the host.
- Single-host tests simulate multiple nodes using isolated containers on a
  dedicated Docker network and independent simulated disks.

The deployment layer is not a general scheduler. Kubernetes and Nomad are not
the default deployment substrate. OCI image compatibility allows other runtimes
to consume an image; it does not establish complete Kubernetes deployment
support. Cross-subnet discovery has a seed fallback and does not require a
multicast relay. Independent initialized clusters are never automatically merged.

## 2. Components and identities

```text
operator browser
    -> any node's Web UI / shared console operations
    -> Group 0: committed topology and operation authority
    -> target node monitor
    -> runtime and managed server processes

node monitor <-> management-network discovery <-> peer node monitors
```

- **Node:** a deployment and hardware-management identity. A production node
  normally corresponds to a physical Linux host; a test node may be a container.
  A node exists before any KV server is started.
- **Monitor:** the node management endpoint, discovery participant and local
  process/container supervisor. Reuse `crowdb-monitor`; do not introduce a
  second agent with overlapping responsibilities.
- **UI:** a presentation/API entry point into shared operations. It owns neither
  confirmed topology nor a private cluster. Losing the initiating UI does not
  delete a submitted operation.
- **Server:** a managed service/process deployment on a node. Server deployment
  is distinct from node admission and Group-0 voting membership.
- **Cluster identity:** the durable identity associated with one initialized
  Group 0. A node cannot infer or replace it from nearby multicast announcements.
- **Rack:** an editable logical grouping of nodes. A node-supplied rack value
  is an initial hint; confirmed placement belongs to Group 0.
- **Physical failure domain:** actual shared host/rack infrastructure used by
  placement policy. Virtual rack labels do not manufacture hardware independence.

Persist node identity, cluster binding and accepted bootstrap information in
the node's host-mounted state root. Persist Group-0 replica state through its
existing WAL/storage contract. These identities are not image contents and are
not derived from IP addresses. Existing protocol identifier types remain the
canonical types; the exact allocation of pre-admission node identities must be
resolved before changing the public schema.

## 3. Invariants

- **I1 — Durable identity:** recreation, endpoint changes and rack reassignment
  preserve node identity. Conflicting live endpoints claiming one identity
  cause a visible conflict, not silent overwrite.
- **I2 — Candidate discovery:** discovery records are observations. They grant
  no admission, voting membership, rack authority or execution permission.
- **I3 — Group-0 authority:** confirmed topology and management mutations use
  Group 0. A local monitor/cache never substitutes for unavailable authority.
- **I4 — Bootstrap binding:** every selected monitor durably accepts one
  compatible cluster/bootstrap manifest before starting its initial replica.
  Conflicting manifests are rejected. No timeout permits replacement bootstrap.
- **I5 — Authorized execution:** a connected Group-0 replica is not proof of
  authority. Management mutations require quorum-backed authorization, and
  stale operations cannot execute after revocation or authority transition.
- **I6 — Explicit failure domains:** virtual rack assignment and physical
  failure-domain identity are distinct placement inputs.
- **I7 — Runtime portability:** a supported-architecture OCI image carries
  code; runtime inputs carry identity, network, devices and persistent state.
- **I8 — Bootstrap gate:** only Group-0 prerequisite processes and Group-0
  initialization run before the shared information platform is ready. Other
  deployment requires committed initial topology.

## 4. Node discovery

### 4.1 mDNS and DNS-SD

Monitors both announce and browse a CROWDB node discovery type. The proposed
type is `_crowdb-node._tcp.local.`; finalize the name before publication.

- mDNS transports local-link DNS queries/responses using UDP port 5353 and
  multicast groups `224.0.0.251` / `ff02::fb`.
- DNS-SD uses PTR records to enumerate instances, SRV to identify monitor host
  and port, A/AAAA to resolve addresses, and TXT for bounded metadata.
- TXT carries only discovery essentials: identity, protocol compatibility and
  optional cluster binding. Full node/hardware/rack information is obtained
  through the monitor management handshake.

These mechanisms come from [RFC 6762](https://www.rfc-editor.org/rfc/rfc6762.html)
and [RFC 6763](https://www.rfc-editor.org/rfc/rfc6763.html). The standards define
discovery and caching; CROWDB defines admission and authorization separately.
Implement standard announcements, refresh, cache expiry and clean departure,
rather than inventing a high-frequency broadcast heartbeat.

Each monitor's discovered-peer cache feeds its local UI backend. Browser code
does not send mDNS traffic. Peers query/announce directly within the discovery
domain; there is no requirement for a separate gossip membership layer.

### 4.2 Network scope and reachability

Discovery runs only on configured management interfaces. Host-network nodes
use the management NIC/VLAN; Docker test nodes use their dedicated bridge
interface. The advertised monitor endpoint must be directly reachable by peers
in that scope. A wildcard bind address is not an advertised address.

UI addresses exposed to browsers and monitor addresses used by peers are
separate endpoint purposes. In bridge tests, host UI ports are published while
peer monitor connections use container-network addresses. Production host
networking requires an explicit local port plan.

mDNS does not normally cross routed subnet/VLAN boundaries. When unavailable,
configured peer/seed monitor endpoints provide the same handshake path. The
seed list is a discovery input, not an authoritative membership list. Monitor
connectivity verifies endpoint usability; discovery expiry alone does not
establish node failure or remove a confirmed member.

### 4.3 Cluster separation and trust

Classify a discovered peer as unbound, bound to this cluster, bound to another
cluster, incompatible or identity-conflicting. Only compatible candidates can
be selected for bootstrap/admission. A foreign cluster is displayed separately;
its topology is not imported automatically.

Announcements are untrusted hints. The direct handshake and admission policy
must validate the management peer before privileged actions. Never advertise
credentials in TXT records. Trust provisioning remains a decision in section 13.

## 5. Node and rack configuration

Before bootstrap, an operator can inspect nodes, create/name virtual racks and
assign nodes. Node-provided rack hints can be overridden. The UI labels these
values as an uncommitted bootstrap draft, not shared authoritative metadata.
Drafts in different UIs can differ; consensus-backed UI consistency begins only
after Group 0 and topology publication are ready.

The accepted bootstrap manifest captures the chosen initial node/rack mapping.
After bootstrap, all rack edits and node admissions are committed through
Group 0. Rediscovery cannot overwrite operator-confirmed rack placement.

Physical host/failure-domain information remains independently inspectable.
Several test containers may occupy different virtual racks while sharing one
physical failure domain. Placement policy must not count those labels as
independent physical redundancy.

## 6. Bootstrap and recovery

### 6.1 State transitions

The node's management lifecycle is:

- **Unbound:** identity exists, discovery is active, no cluster is accepted.
- **Prepared:** a cluster identity and initial manifest are durably reserved;
  the monitor accepts retries of that manifest and rejects conflicting ones.
- **Group-0 starting:** selected monitors provision/start their initial replicas
  using the same fixed voting-member manifest.
- **Topology publishing:** Group 0 has usable quorum/leadership; initial hardware
  and cluster metadata are being published idempotently.
- **Active:** shared topology is confirmed and general deployment is enabled.
- **Authority unavailable:** binding persists, diagnostics/discovery continue,
  and new cluster-management mutations are disabled until authority recovers.

Unavailability is not a transition back to Unbound. Clearing cluster binding
requires an explicit recovery/decommission procedure, not an automatic timeout.

### 6.2 Initialization flow

1. Any UI collects compatible candidates and the operator's rack assignments.
2. The operator explicitly selects initial Group-0 voting members. Candidate
   count is not voting-member count; application nodes need not all vote.
3. Submit a bootstrap identity and immutable initial manifest to selected
   monitors. Persist compatible preparation before starting replicas.
4. Start the minimum Group-0 services with the fixed manifest. Use existing
   Paxos election and WAL durability, not discovery-cache membership.
5. Wait for quorum-backed readiness and publish initial metadata idempotently.
6. Persist/refresh cluster contact information for member UIs. Enable general
   deployment only after initial topology is confirmed.

Replica startup coordination and interrupted preparation recovery must be
specified before implementation. Durable per-node conflict rejection is
necessary but alone does not establish a complete distributed bootstrap
protocol. Two overlapping requests must never create incompatible groups;
arbitrary disjoint selections can form distinct clusters and must not be
advertised as one globally unique LAN cluster.

### 6.3 Recovery and subsequent admission

Restart resumes accepted identity and replica WAL state. Lost multicast does
not erase stored Group-0 contacts. An interrupted bootstrap reuses its operation
identity/manifest; partial metadata publication resumes without admitting
unselected nodes or repeating destructive provisioning.

A newly discovered node enters the candidate view. A committed admission
operation binds it to the cluster and assigns its rack. Adding a deployment
node does not automatically alter Group-0 quorum. Voting-member changes follow
the existing reconfiguration contract.

## 7. Shared UI and management operations

All UIs invoke the same shared console operations and read confirmed state
from Group 0. Consistent reads support action confirmation; watch/refresh keeps
presentation current. A temporarily stale screen does not create a second
authority. The UI displays stale/unavailable state explicitly.

Management operations have durable identity, target, requested action and
execution outcome. Group 0 commits the management intent/authorization; the
target monitor performs the side effect and records its result. This is not
an atomic transaction between a KV write and a runtime action.

Retries therefore use operation identity and local execution recovery. Crash
after runtime success but before result publication must inspect runtime state
and complete/reconcile the same operation, rather than blindly repeat it.
Image digest and deployment-instance identity distinguish the intended instance
from a reused process ID or container name.

Changing UI hosts does not require moving cluster state. Local secrets and
runtime observations are not replicated as public topology. Credentials needed
to authorize another UI must be provisioned through the chosen trust policy.

## 8. Authorization and partitions

For a three-voter Group 0 partitioned two-to-one, only the two-voter side can
commit management mutations. The minority cannot use its local replica or
monitor cache as replacement authority and cannot bootstrap a new Group 0.

The deployment policy is that a node may execute controlled actions only while
holding valid authorization from the current Group 0. Define bounded grants
and renewal for operations that outlive a single consensus commit. Fencing
generations distinguish old ownership/authorization from current grants;
execution boundaries must reject expired or superseded commands.

A bare increasing token is insufficient unless the receiving execution path
knows which generations are current and validates them. Likewise, a lease is
insufficient unless timing, renewal, expiry and restart behavior are defined.
No design claim of stale-node exclusion is valid until those enforcement
details are resolved and tested.

Monitors and UIs remain available for diagnostics during authority loss.
The exact meaning of stopping an unauthorized node's existing data services
is unresolved: management authorization does not automatically revoke every
data group's independent Paxos authority. Section 13 makes that choice explicit.
Group-0 replica processes must be allowed to recover quorum even while general
deployment is gated; stopping all replicas on authority loss would prevent
recovery.

## 9. Image and runtime deployment

### 9.1 Common OCI image

Publish OCI images to Docker Hub or another OCI-compatible registry. The same
image for a supported CPU architecture contains monitor/UI/server binaries and
their dependencies. Node-local state and cluster identity are external mounts.
Tags identify releases for humans; production plans pin immutable image digests.
Architecture-specific builds may share a multi-platform image index only when
their dependencies have been validated.

Docker and containerd consume these images. `nerdctl` is an operator CLI for
containerd, not an additional daemon. Production monitor runtime control uses
containerd APIs without depending on dockerd. See the
[nerdctl reference](https://github.com/containerd/nerdctl) for CLI/runtime
terminology; its CLI compatibility does not define CROWDB lifecycle semantics.

### 9.2 Production profile

- Linux host, containerd runtime and host networking.
- Explicit management/data-plane network selection; discovery uses management.
- Stable disk/device identity and explicitly permitted block-device access.
- Host-persistent config, node state, server state, logs and crash artifacts.
- Explicit CPU/memory and device assignments; resource claims are validated
  before runtime side effects.

Host networking removes a container bridge/NAT layer but does not itself
guarantee high performance. Disk passthrough does not itself bypass filesystem
cache: the storage engine's raw-block/direct-I/O path defines that behavior.

### 9.3 Single-host test profile

- One container per simulated node on a dedicated user-defined Docker bridge.
- Unique persistent roots and identities; independent simulated disk files or
  loop devices, never overlapping writes to one backing disk.
- Identical internal listener ports are allowed because network namespaces
  differ; browser UI access uses distinct published host ports.
- Actual multicast discovery is exercised within the bridge. Separate networks
  isolate independent discovery tests.

Docker bridge behavior is described in the
[Docker reference](https://docs.docker.com/engine/network/drivers/bridge/).
The test profile measures functional correctness, not raw-disk production
performance. Multi-host host-network tests separately validate LAN discovery
and real resource access. macOS-hosted Linux containers require a VM/network
setup that is tested explicitly; image portability does not make the VM's LAN
multicast behavior equivalent to Linux host networking.

## 10. Resource and lifecycle management

Containers are packaging, resource/isolation and upgrade units. Deployment
plans explicitly assign host resources; monitors validate and execute, rather
than autonomously choose global placement.

- **CPU/memory:** cpuset, memory limits and optional NUMA assignments define
  container boundaries. Application thread placement stays within those limits.
  IRQ/kernel affinity tuning is host policy, not image content.
- **Disk:** stable device references, exclusive intended ownership and supported
  engine access modes. Important data never lives only in the writable layer.
- **Network:** host ports and management/data interfaces are explicit.
  RDMA device access and TCP fallback remain the responsibility of their
  transport contracts, not the discovery mechanism.
- **Lifecycle:** explicit start, stop, drain, upgrade and rollback operations.
  Image upgrades pin digests and preserve state. Stateful service rollback is
  allowed only within its storage/protocol compatibility contract.
- **Monitoring:** container and process health are separate observations.
  Existing monitor supervision can recover eligible process exits; it must not
  silently override a deliberate stop, expired authorization or upgrade drain.
- **Runtime access:** monitor receives the minimum runtime/device privileges
  needed. Application/UI containers do not all receive the containerd socket.

No automatic global placement/config reconciliation is required. Monitor
detects drift against confirmed deployment inputs and reports it; local
liveness recovery follows explicit policy. Repair/upgrade progress and failure
remain visible to every authorized UI through shared operation records.

## 11. Configuration and observability

Local startup inputs are limited to bootstrap prerequisites and process/runtime
settings: persistent root, management interfaces, listen/advertise addresses,
optional discovery seeds, trust inputs and runtime endpoint. Group 0 owns
confirmed cluster topology and management intent. This does not move every
process tuning parameter into Group 0; respect the existing
[configuration architecture](../config/design-crowdb-config.md).

Expose:

- Discovery peer state, endpoint reachability, last observation/expiry,
  incompatible identities and multicast-interface status.
- Bootstrap manifest identity, preparation/start/publication progress and the
  reason general deployment is gated.
- Group-0 authority/renewal status, rejected stale operations and recovery needs.
- Desired deployment inputs, actual image digest/resources, process/container
  health, drift and operation outcomes.

Keep local logs/crash artifacts persistent and collect asynchronously. Do not
replicate every log line into Group 0 or expose credentials in discovery/UI
diagnostics.

## 12. Correctness and validation

- Restart and endpoint changes preserve identity; duplicated roots cause a
  visible conflict (I1).
- Three monitors discover each other on a real test bridge and LAN; expiry
  affects candidates, not confirmed topology (I2).
- Multiple UIs can edit local drafts, but only the accepted manifest becomes
  shared topology. General deployment stays gated through incomplete publication
  (I3, I8).
- Concurrent overlapping bootstraps, partial preparation, initiator crashes
  and interrupted publication recover without conflicting replicas or identity
  replacement (I4).
- Partition tests distinguish reachable replicas from quorum authority and
  deliver delayed/retried commands to verify stale-action rejection (I5).
- Multiple virtual racks on one host preserve one physical failure-domain
  identity (I6).
- Docker bridge and Linux containerd host-network profiles use the same image
  digest and exercise restart, persistent disk/state and resource boundaries
  (I7).
- Existing Group-0 membership changes remain governed by reconfiguration;
  deployment-node discovery never changes a voting set.

Before adoption, measure discovery latency/traffic and stale-view bounds at the
intended LAN size. Broad ecosystem use of mDNS is not proof of unlimited
single-domain scalability.

## 13. Decisions still required

The architectural flow is settled; the following mechanisms need explicit
agreement before their implementation:

- **Bootstrap coordination:** one selected coordinator with durable monitor
  preparation and resumable delivery, or a quorum/prepare protocol over fixed
  selected membership. Define conflicts, cancellation and recovery; avoid
  assuming local reservations alone establish global agreement.
- **Strict node activity policy:** decide whether Group-0 authorization expiry
  stops existing data-serving processes, including reads, or only gates
  management/new deployment while data groups retain their own authority.
  Define expiry bounds and fencing enforcement. Group-0 recovery and diagnostic
  processes remain exempt from general deployment gating.
- **Admission trust:** explicit UI approval with authenticated management peers,
  or preprovisioned deployment credentials for automatic admission. Specify how
  every authorized UI obtains access without public secret replication.
- **Pre-admission identity allocation:** retain canonical numeric node IDs with
  a defined collision/allocation scheme, or use a durable discovery identity
  mapped to a confirmed node ID at admission. IP-derived identities are excluded.
- **Monitor placement:** a host service controlling containers, or a dedicated
  management container with narrowly scoped runtime access. Choose the production
  recovery/security boundary independently of application image compatibility.
