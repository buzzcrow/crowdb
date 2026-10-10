<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Multi-node Deployment

Node containers run discovery, SSH, monitor and Web before cluster creation.
Group 0 owns confirmed topology and management operations. Linux amd64 images
are built by Pixi-managed standalone BuildKit and run on system Docker or
containerd/nerdctl. The [OCI image design](design-crowdb-oci-image.md) defines
construction, runtime resources and publication.

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
- [13. Supported scope](#13-supported-scope)

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
    -> Docker node container and managed server processes

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
canonical types. Each node generates and persists a discovery UUID before
admission. Candidates are identified by UUID; bootstrap fixes the UUID-to-numeric
node-ID mapping in its accepted configuration. Subsequent admission allocates a
numeric node ID through Group 0 and durably records the mapping. Allocation and
mapping publication are conditional and idempotent under concurrent admission;
IDs cannot collide or be silently reassigned. Endpoint and rack changes do not
change either identity. Duplicate live UUID claims are identity conflicts.

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

Monitors announce and browse `_crowdb-node._tcp.local.` on the configured
management interfaces. Candidate expiry removes observations after five seconds;
it never removes a confirmed member.

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
Announcements, refresh, cache expiry and clean departure follow these mechanisms.

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
credentials in TXT records. Admission requires explicit operator approval in the UI and authenticated
management peers; discovery never automatically admits a node. The Cluster
tab shows a separate Candidate Nodes list below its left-side cluster view.
Moving a candidate into a cluster opens SSH access and rack selection. Before
bootstrap this updates only the visibly uncommitted draft; afterward admission
requires a committed Group-0 operation.

All admitted nodes support mutual SSH access using Ed25519 keys. Each node
creates and persists its own `id_ed25519` private key; only `id_ed25519.pub`
is exchanged into the other nodes' `authorized_keys`. Private keys are never
copied between nodes or published in Group 0. Joining uses username/password
once when working key access has not already been provisioned. Verify SSH
host identity and its association with the candidate monitor before exchanging
keys. Install existing members' public keys on the candidate and its public
key on every existing member, then verify bidirectional key access before
confirming admission. Failed setup remains visibly pending and resumes the
same admission operation without duplicating keys. Pending nodes cannot receive
general deployments. Cancellation removes authorization entries installed by
that operation without removing unrelated keys.

The multi-node Docker test profile provides an SSH login account with default
username/password `crowdb` / `crowdb`, prefilled by its UI. Production credentials
are supplied explicitly. Initialization passwords are discarded after key
setup and are not persisted in Group 0. Node keys, `authorized_keys` and SSH
host keys survive container recreation in persistent storage. Runtime service
accounts need not acquire an interactive shell merely to provide this login.
Every management entry point uses the locally provisioned SSH identity; Group 0
stores only nonsecret connection metadata and credential references. Mutual
SSH access grants each node the configured login user's access to its peers.

## 5. Node and rack configuration

Before bootstrap, an operator can inspect nodes, create/name virtual racks and
assign nodes. Node-provided rack hints can be overridden. The UI labels these
values as an uncommitted bootstrap draft, not shared authoritative metadata.
Drafts in different UIs can differ; consensus-backed UI consistency begins only
after Group 0 and topology publication are ready.

The accepted bootstrap manifest captures the chosen initial node/rack mapping.
After bootstrap, all rack edits and node admissions are committed through
Group 0. Rediscovery cannot overwrite operator-confirmed rack placement.

Connection and rack edits authenticate the currently discovered UUID over SSH.
`NodeRecord` keeps its discovery UUID, numeric ID, admission operation and physical
host identity. `node_update` persists fixed source/target inputs in Group 0 before
changing hardware, refreshes peer RPC endpoints without changing voting flags,
then publishes the updated mapping. A retry from another UI resumes those same
inputs. Moving a node with disk groups is rejected until those groups are removed;
this prevents losing its hardware children during relocation.

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

Bootstrap exclusion is enforced by each target KV server before creating
store 0 and covers the complete store-0/group-0 initialization sequence. The
server atomically and durably accepts the first bootstrap operation identity
and its fixed initial configuration. A conflicting operation is rejected before
creating or reusing store 0. Retries of the accepted operation resume unfinished
steps and verify existing content; an existing store 0 without group 0 is not
permission for another operation to take over. Restart retains the accepted
operation. Failure and timeout do not automatically release its ownership.
Monitor preparation and KV-server acceptance refer to the same operation and
configuration; neither UI-local state nor a generic store/group creation path
may bypass the exclusion.

Every selected member must durably accept the same operation and configuration
before any initial replica becomes eligible for election or data publication.
Overlapping requests cannot both obtain all required acceptances. They may each
occupy part of their selected membership, in which case both remain incomplete
and the UI exposes the conflict for explicit cleanup and retry. This mechanism
guarantees per-node exclusion, not a globally unique cluster on the LAN.
Disjoint selected memberships may create independent clusters; UIs display
them separately and do not automatically merge their logs or topology.

### 6.3 Recovery and subsequent admission

Restart resumes accepted identity and replica WAL state. Lost multicast does
not erase stored Group-0 contacts. An interrupted bootstrap reuses its operation
identity/manifest; partial metadata publication resumes without admitting
unselected nodes or repeating destructive provisioning.

A newly discovered node enters the candidate view. A committed admission
operation binds it to the cluster and assigns its rack. Adding a deployment
node does not automatically alter Group-0 quorum. Voting-member changes follow
the existing reconfiguration contract.

The UI supports Group-0 member management and explicit group/store deletion
for bootstrap recovery. To consolidate independently initialized clusters, the
operator selects the retained cluster, cleans the other cluster's group 0,
store 0 and binding, then admits its cleaned nodes to the retained cluster.
Voting additions use reconfiguration and catch-up, not merging independent
Group-0 histories. Deleting a group or store alone does not silently release
bootstrap ownership or cluster binding. Cleanup must fence delayed commands
and prevent old replicas from restarting before allowing a new bootstrap;
accepted operations retain durable terminal state so stale retries cannot
recreate deleted resources. Unreachable nodes remain pending cleanup and are
not eligible for reuse. Existing committed clusters require explicit destructive
cleanup rather than cancellation of an uncommitted draft.

The public fixed `PreparedBootstrap` manifest is a recovery record distributed to
selected monitors over authenticated SSH. It is not a global cluster registry or
an authority before consensus. KV-server durable acceptance arbitrates resource
ownership at each participant. A private Unix control socket handles local
monitor commands; the unauthenticated HTTP handshake only exposes observations.
SSH connection/authentication has a ten-second bound; bidirectional proof has a
twenty-second bound. Passwords are discarded, sessions are reused during setup,
and only public keys cross nodes.

### 6.4 Voting replica admission

Node admission grants hardware/service management access; it does not add a voter.
Adding a replica uses `JoinGroupRequest` to import a snapshot from a confirmed
leader. Existing peers first register the new replica without voting rights.
The target wires existing peers and catches up through WAL until its contiguous
applied frontier reaches the leader's observed frontier. Only then is the new
replica promoted and published with store membership in Group 0. Once promotion
begins, an ambiguous response retains the caught-up replica for retry rather than
deleting a replica another peer may already count in its quorum.

System replica join uses identified system ownership before creating store 0.
Generic join cannot bypass an accepted system bootstrap. Independent clusters
remain separate: operators explicitly clean one cluster, wait for every node's
cleanup, admit the released nodes to the retained cluster and add voting replicas.

## 7. Shared UI and management operations

The UI presents explicit lifecycle states:

- **Unbound draft:** local temporary topology and a separate candidate list;
  SSH preparation and rack edits do not constitute committed membership.
- **Bootstrap in progress:** per-node store-0/group-0 progress and the fixed
  submitted configuration. Later draft edits cannot alter that operation.
  Failure offers resume of the same operation or explicit cleanup.
- **Topology publishing:** quorum exists but initial publication is incomplete;
  show initialization progress and keep general service deployment disabled.
- **Active:** load confirmed Group-0 topology and replace the temporary view.
  Subsequent changes use Group 0; unused drafts are never silently imported.
- **Authority unavailable:** retain the last confirmed topology with an explicit
  stale/unavailable indication and diagnostics. Disable authority-dependent
  operations and never return a bound node to replacement-cluster creation.
- **Multiple clusters discovered:** show distinct clusters and let the operator
  select one to manage; neither independent topology nor drafts auto-merge.

Admission with partial SSH setup is shown as joining/recovery required, with
operation progress distinct from confirmed membership. Retry installs only
missing authorization and resumes verification. Cancellation removes only
public-key authorization installed by that operation, preserving pre-existing
or concurrently required trust; failure to finish cleanup remains visible.

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

The monitor checks cluster binding and the complete current service intent
through linearizable Group-0 reads before executing a management command. Each
read has a three-second bound. It checks the intent again after stopping an old
process and before starting its replacement. A superseded operation or unavailable
authority rejects execution. Recovery reads current desired intents through the
same authority path and does not execute a retained command solely from disk.

Admission commands similarly match the current cluster publication and UUID,
numeric ID, operation ID and cancellation state. Local durable cancellation and
retirement markers reject delayed commands after cleanup or restart. Long-running
data ownership uses its existing expiry and fencing contract rather than deriving
an extended lease from these management checks.

Monitors and UIs remain available for diagnostics during authority loss.
Group-0 unavailability does not by itself stop existing data services. Each
data group continues only under its own Paxos authority and read/write rules.
Operations requiring current Group-0 authority fail, including management
mutations, keep-alive updates, ownership acquisition/renewal and ownership
balancing. Failure to obtain ownership content is an authority-unavailable
result, never an empty ownership set; it cannot trigger release or reassignment.
Existing ownership remains subject to its own expiry and fencing contract;
actions requiring renewed ownership stop when that authority expires. Cached
ownership cannot extend its validity indefinitely.
Group-0 replica processes must be allowed to recover quorum even while general
deployment is gated; stopping all replicas on authority loss would prevent
recovery.

## 9. Image and runtime deployment

### 9.1 Common OCI image

Publish OCI images to Docker Hub or another OCI-compatible registry. The same
image for a supported CPU architecture contains monitor/UI/server binaries and
their dependencies. Node-local state and cluster identity are external mounts.
Single-node and multi-node operation use the same image, monitor and UI.
Default manual startup awaits candidate selection and bootstrap for one or more
nodes. Explicit automatic single-node startup creates virtual disks and initializes
the cluster, then disables UI editing. Startup policy and the UI editing capability differ,
while identity, discovery, authority and managed-service behavior remain shared.
Read-only single-node mode rejects UI management mutations in the backend as
well as disabling editing controls.
Tags identify releases for humans; production plans pin immutable image digests.
Architecture-specific builds may share a multi-platform image index only when
their dependencies have been validated.

System Docker and Pixi-supplied containerd/nerdctl manage the node container.
The monitor supervises local managed processes; external runtime lifecycle
recovers the whole node. Standalone BuildKit explicitly exports and validates
OCI media types and blob digests without Docker or containerd dependencies.
The [OCI image design](design-crowdb-oci-image.md) defines shared construction,
resource verification and publication without changing cluster semantics.

### 9.2 Production profile

- Linux host, system Docker or rootful containerd runtime and host networking.
- Each node runs its monitor and Web UI inside a CROWDB node container.
  The node container is the management boundary and controls managed services
  using explicit mounted resources and local process supervision. Application service containers
  do not inherit that runtime access. An external host/runtime startup mechanism
  starts and recovers the node container itself; it cannot recover itself after
  its own termination. Node identity, SSH identity and management state use
  host-persistent mounts.
- Explicit management/data-plane network selection; discovery uses management.
- Stable disk/device identity and explicitly permitted block-device access.
- Host-persistent config, node state, server state, logs and crash artifacts.
- Explicit CPU quotas, memory limits and permitted block devices; the launcher
  validates inputs before creating the container and verifies applied resources.

Host networking removes a container bridge/NAT layer but does not itself
guarantee high performance. Disk passthrough does not itself bypass filesystem
cache: the storage engine's raw-block/direct-I/O path defines that behavior.

### 9.3 Single-host test profile

- One CROWDB container per simulated node on a dedicated user-defined Docker
  bridge, each running its own monitor, Web UI and SSH endpoint. Starting three
  node containers enables mutual discovery without pre-entering peer addresses;
  opening any published UI exposes the candidate list and cluster preparation.
  Docker availability does not require an implicit runtime socket mount: the
  monitor manages local processes for this profile, while future external runtime control
  requires separately configured runtime access.
- Unique persistent roots and identities; independent simulated disk files or
  loop devices, never overlapping writes to one backing disk.
- Identical internal listener ports are allowed because network namespaces
  differ; browser UI access uses distinct published host ports.
- Actual multicast discovery is exercised within the bridge. Separate networks
  isolate independent discovery tests.

Docker bridge behavior is described in the
[Docker reference](https://docs.docker.com/engine/network/drivers/bridge/).
The test profile measures functional correctness, not raw-disk production
performance. Linux host-network acceptance separately validates explicit resource
inputs, credentials and persistent replacement using the same image. macOS-hosted Linux containers require a VM/network
setup that is tested explicitly; image portability does not make the VM's LAN
multicast behavior equivalent to Linux host networking.

## 10. Resource and lifecycle management

Containers are packaging, resource/isolation and upgrade units. Deployment
plans explicitly assign host resources; monitors validate and execute, rather
than autonomously choose global placement.

- **CPU/memory:** the production launcher validates CPU quotas and memory limits
  against host capacity, then verifies Docker applied them. Memory is at least
  512 MiB. Hardware observations respect container limits. CPU affinity, NUMA
  and IRQ tuning remain host policy.
- **Disk:** stable device references, exclusive intended ownership and supported
  engine access modes. Important data never lives only in the writable layer.
- **Network:** host ports and management/data interfaces are explicit.
  RDMA device access and TCP fallback remain the responsibility of their
  transport contracts, not the discovery mechanism.
- **Lifecycle:** monitor-managed services support explicit deployment, start,
  stop and restart using shared operation records. Container replacement or
  image upgrade is performed externally with a pinned digest and the same
  persistent root; storage/protocol compatibility remains required.
- **Monitoring:** container and process health are separate observations.
  Existing monitor supervision can recover eligible process exits; it must not
  silently override a deliberate stop, expired authorization or upgrade drain.
- **Runtime access:** the launcher uses the system Docker daemon. Node containers
  supervise local processes and receive no Docker or containerd socket. Explicit
  permitted block devices are passed through by the host launcher.

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
- Docker bridge and Linux Docker host-network profiles use the same image
  digest and exercise restart, persistent disk/state and resource boundaries
  (I7).
- Existing Group-0 membership changes remain governed by reconfiguration;
  deployment-node discovery never changes a voting set.

Before adoption, measure discovery latency/traffic and stale-view bounds at the
intended LAN size. Broad ecosystem use of mDNS is not proof of unlimited
single-domain scalability.

## 13. Supported scope

The discovery identity and monitor placement decisions are settled: durable
UUIDs identify candidates, Group 0 owns admitted numeric IDs, and each node's
monitor/UI run in its node container. No human decisions remain open in this design. Additional runtimes,
architectures and Kubernetes orchestration remain outside this supported scope.
