<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R227: console — Multi-node discovery and light-container deployment

## Problem

CROWDB needs a multi-node deployment flow in which each node runs
`crowdb-monitor` and a Web UI. Operators should not have to enter every peer
address before discovering available nodes. The current monitor and console
surfaces do not establish the LAN discovery and shared bootstrap contract
described here; existing service registration is not node discovery.

The permanent [console design](../design/console/design-crowdb-console.md)
defines Group 0 as topology authority, and the
[Group-0 design](../design/kv/design-crowdb-kv-group0.md) defines its service
registry. Neither a local discovery cache nor a console-local launch registry
can replace that authority. Existing console node/server coupling must be
reviewed when adding nodes before any server exists.

Concrete scenarios:

- Three machines on a home LAN discover each other through their monitors.
  An operator opens any node's UI, assigns racks, and initializes Group 0.
- Several containers on one machine simulate independent nodes on one Docker
  bridge network, with independent identities, state directories and disks.
- Production Linux nodes use light containers with host networking, host
  persistent state, and explicit disk/device access. Containers package binaries
  and dependencies while host resources remain explicitly managed.
- A network partition must not allow an isolated UI or monitor to create a
  replacement cluster or execute stale deployment commands.

## Solution

The target architecture is defined in the
[multi-node deployment design](../design/deploy/design-crowdb-deploy.md).
This requirement tracks its implementation and acceptance; unresolved decisions
in that design must be settled before their dependent implementation begins.

### Architecture and authority

```text
browser on any node
    -> local crowdb-web / shared console operations
    -> Group 0: confirmed topology and management authorization
    -> target crowdb-monitor
    -> containerd / managed server processes

before Group 0 exists:
local UI -> local monitor discovery -> peer monitors -> bootstrap preparation
```

Every node has a monitor management endpoint and a Web UI. No fixed UI node
is required. Discovery advertises nodes, not each managed server. Group 0 is
the first managed service allowed to start; more complex server deployment is
enabled only after Group 0 is ready and the initial topology is published.

### Invariants

- **I1 — Stable node identity:** node identity is persisted outside the image
  and container writable layer. IP, container name, restart and rack reassignment
  do not change it. Cloned identity directories are detected as conflicts rather
  than silently merging two nodes.
- **I2 — Discovery is a candidate view:** discovery never grants cluster
  membership, voting rights, rack authority or deployment permission. Records
  can expire without deleting confirmed members from Group 0.
- **I3 — Shared authority:** after bootstrap, confirmed node/rack topology and
  cluster deployment intents are authoritative in Group 0. UI caches can lag,
  but mutations succeed only through committed control-plane state.
- **I4 — One bootstrap identity per node:** a node persists its accepted cluster
  identity and initial Group-0 manifest before starting a replica. Conflicting
  manifests are rejected; retries of the same operation are idempotent. No
  timeout or empty discovery result permits automatic replacement bootstrap.
- **I5 — Quorum-backed authorization:** connectivity to one Group-0 replica is
  insufficient. New management mutations require current quorum-backed
  authority. Stale commands cannot execute after authority moves or expires.
- **I6 — Explicit placement:** virtual racks are editable logical groups.
  Physical host/failure-domain information is recorded separately; multiple
  containers on one host do not become independent physical failure domains
  because their virtual rack labels differ.
- **I7 — Portable image, explicit resources:** the same OCI image for a supported
  OS/CPU architecture is used for single-host Docker tests and production
  containerd deployment. Network, disks, persistent mounts and resource limits
  are runtime inputs, not baked cluster identity.

### Numbered work items

1. Extend `crowdb-monitor` identity/profile/status surfaces and shared types in
   `crowdb-protocol` to expose stable node identity, monitor endpoint, protocol
   compatibility, optional rack hint, physical-host identity and cluster binding.
   Obtain full hardware/rack information through a direct monitor handshake.
   A node can exist before any KV or application server is deployed.

2. Add mDNS + DNS-SD announcement and continuous browsing to `crowdb-monitor`.
   Use a CROWDB node service type (proposed `_crowdb-node._tcp.local.`), SRV
   address/port resolution and bounded TXT metadata. Select management interfaces
   explicitly; do not announce on storage data-plane interfaces by default.
   Support record updates, expiry, clean departure, duplicate identity detection
   and incompatible-peer reporting. The browser UI reads its backend's candidate
   view; it does not perform multicast itself.

   mDNS is link-local UDP discovery, not consensus or a health guarantee.
   DNS-SD structures service enumeration and endpoint metadata. These are borrowed
   standard discovery mechanisms; CROWDB defines its own node and authorization
   semantics. See [RFC 6762](https://www.rfc-editor.org/rfc/rfc6762.html) and
   [RFC 6763](https://www.rfc-editor.org/rfc/rfc6763.html).
   Provide explicit peer/seed addresses as a fallback when multicast is blocked
   or nodes span subnets. Neither fallback nor discovered endpoints bypass the
   same handshake and cluster-binding checks.

3. Extend `crowdb-web` and `crowdb-console-shared` with a pre-bootstrap view:
   candidate nodes, editable virtual racks, node-to-rack assignments and Group-0
   initialization. Node-supplied rack information is a hint, not immutable
   placement. A pre-bootstrap draft is local and visibly uncommitted; differing
   drafts on different UIs do not constitute confirmed topology. Limit this
   stage to node/rack/bootstrap operations, excluding other server deployment.

4. Extend the existing shared `bootstrap_intent` and cluster operations to submit
   one explicit initial Group-0 manifest: cluster identity, selected initial
   replica members, reachable endpoints and confirmed node/rack draft. Peer
   monitors persist compatible preparation before replica startup. Concurrent
   incompatible requests must fail safely without constructing overlapping
   conflicting groups. Partial preparation and initiator crashes must have an
   explicit resume/abort procedure; an abort cannot erase a cluster that has
   committed state. The coordination protocol is a human decision below, not an
   assumption that multicast or smallest-node-ID election provides agreement.

5. Wait for a usable Group-0 leader/quorum, then publish the initial topology
   idempotently. Gate general deployment on both consensus readiness and
   topology publication. Persist cluster binding and Group-0 contact information
   at each accepted node. Other UIs attach to that cluster and read Group 0;
   incompatible cluster bindings are shown separately and never auto-merged.
   Newly discovered nodes remain candidates until a committed admission operation.
   Later Group-0 voting membership changes use the existing reconfiguration path.

6. Route rack changes and deployment mutations from every UI through shared
   console operations and Group 0. Use consistent reads where confirmation needs
   current authority, with watch/refresh for presentation. Record operation
   identity and execution state so UI retries or UI-node loss cannot duplicate a
   deployment. Monitor execution validates the current authorization and rejects
   stale commands. Group-0 unavailability leaves discovery/diagnostics available
   but disables new cluster management mutations.

7. Add a containerd-backed execution path to `crowdb-monitor` alongside existing
   managed-process support. Production uses containerd APIs; nerdctl is an operator
   tool, not a required per-command subprocess. Define image digest, CPU/memory
   boundaries, host networking, persistent config/state/log mounts and permitted
   disk/device inputs. No implicit privileged container or unrestricted host
   device access is required. Preserve process-level liveness and explicit
   start/stop/upgrade operations; do not add a general scheduler or automatic
   global reconciliation loop.

8. Extend `container/single-node-container` packaging/test infrastructure for
   multiple node containers from the same OCI image. Each gets an independent
   persistent root and simulated disk, joins one dedicated user-defined Docker
   bridge, and advertises its internally reachable monitor endpoint. Publish
   distinct host UI ports for browser access. Separate test networks bound the
   discovery scope. Production uses host networking and real disks on supported
   Linux hosts; discovery and cluster-management semantics stay identical.
   OCI compatibility does not claim complete Kubernetes lifecycle integration
   or unverified CPU/OS support.

## Dependencies

- Existing Group-0 KV operations, consistent reads, service registry and
  membership reconfiguration are reused; node discovery does not replace them.
- Existing `crowdb-monitor`, `crowdb-console-shared`, `crowdb-web`,
  `crowdb-protocol` and single-node image infrastructure are the implementation
  base. Existing working-tree changes must be preserved.
- R139 concerns distributed service configuration. This requirement must define
  the minimal deployment-intent/authorization records it needs without silently
  assuming R139 is implemented or replacing all local process configuration.
- R193 concerns failure-budget placement; physical failure-domain records must
  remain distinguishable from editable virtual racks for that integration.
- The backlog index currently references absent R210/R211/R212 detail files.
  Reconcile those workstreams before implementation to avoid duplicating node
  lifecycle, health and cluster-scope contracts; do not assume they are available.
- `/nv/cpp/adcp/topic/deploy.md` is discussion context for light-container
  deployment, not a portable repository dependency. The applicable resource and
  lifecycle choices are captured above.
- Implementation must resolve the bootstrap and authorization decisions below.
  Permanent console, Group-0, configuration and container documentation must be
  updated to the shipped behavior during implementation cleanup.

## Acceptance

1. Setup: a persisted node root. Action: recreate the container, change its IP
   and rack. Assertion: node identity remains unchanged; a second live endpoint
   using the same identity is reported as a conflict (I1). Integration test.
2. Setup: three monitors on a multicast-capable test network. Action: start,
   update and stop nodes. Assertion: every backend discovers reachable peer
   identities/endpoints, removes expired candidate records, and does not modify
   confirmed membership on expiry (I2). Integration test.
3. Setup: multicast disabled and explicit reachable seeds supplied. Action:
   discover peers. Assertion: the same compatibility/binding checks apply;
   discovery does not itself authorize admission (I2). Integration test.
4. Setup: multiple network interfaces and an incompatible peer. Action: enable
   discovery on the management interface only. Assertion: advertisements remain
   scoped there, incompatible nodes are reported and cannot bootstrap (I2).
   Integration test.
5. Setup: uninitialized nodes with rack hints. Action: open any node UI, create
   virtual racks and move nodes. Assertion: hints can be overridden, drafts are
   visibly uncommitted and non-Group-0 deployment is blocked (I3, I6). E2E test.
6. Setup: overlapping candidate nodes and two incompatible bootstrap manifests.
   Action: submit concurrently and restart a prepared monitor. Assertion: durable
   conflict rejection prevents conflicting replica startup; the same operation
   can resume without duplicating a group (I4). Integration test.
7. Setup: bootstrap interrupted before and after Group-0 commit. Action: follow
   the specified recovery flow. Assertion: partial setup is resumable, committed
   cluster identity is preserved and no timeout creates a replacement (I4).
   Integration test.
8. Setup: three selected Group-0 replicas. Action: bootstrap and delay initial
   topology publication. Assertion: general deployment remains gated until both
   quorum readiness and publication; all UIs then read the same confirmed
   node/rack state (I3). E2E test.
9. Setup: an active cluster and a new candidate. Action: discover then admit it
   through another UI. Assertion: discovery alone changes no membership; admission
   persists in Group 0 and any voting change uses reconfiguration (I2, I3).
   Integration test.
10. Setup: two independent initialized clusters on one LAN. Action: discover
    each other. Assertion: their identities remain separate and no automatic
    merge or rebinding occurs (I4). Integration test.
11. Setup: concurrent UI mutations and retried deployment requests. Action:
    update racks/deploy and lose the initiating UI. Assertion: committed outcomes
    are shared and each operation executes at most once or resumes through its
    recorded idempotent stages (I3, I5). Integration test.
12. Setup: a three-replica Group 0 partitioned two-to-one. Action: issue management
    commands on both sides and deliver a previously authorized stale command.
    Assertion: only quorum-authorized mutations execute, stale execution is
    rejected, and minority monitors retain diagnostics without replacement
    bootstrap (I4, I5). Integration test.
13. Setup: several simulated nodes on one physical host in different virtual
    racks. Action: inspect placement inputs. Assertion: the shared physical
    failure domain remains visible and rack changes do not rewrite it (I6).
    Integration test.
14. Setup: one supported-architecture OCI image digest. Action: launch multiple
    Docker bridge nodes and Linux containerd host-network nodes with appropriate
    disks/mounts. Assertion: both discover/bootstrap/manage successfully, state
    survives replacement, internal advertised addresses are reachable and the
    simulated-disk run makes no production-performance claim (I1, I7).
    Integration test.
15. Setup: containerd deployment with explicit devices/resources. Action: start,
    stop, restart and upgrade through monitor. Assertion: requested resource
    limits and mounts are applied, process health is observable and restart
    preserves persistent state (I7). Integration test.

Implementation verification commands (these are required future gates, not
claims that the new acceptance coverage already exists):

```bash
pixi run cargo test -p crowdb-monitor
pixi run cargo test -p crowdb-console-shared
pixi run cargo test -p crowdb-web
pixi run cargo test -p crowdb-protocol
pixi run test-console
pixi run test-console-ui
pixi run rs-fmt-check
pixi run rs-lint
```

## Open Questions

- Bootstrap coordination: use one explicitly selected initiating node with
  durable reservations and resumable manifest delivery, or a quorum/prepare
  protocol over a fixed selected membership. Choose recovery and conflict rules
  before implementation. Neither option guarantees one cluster across arbitrary
  disjoint selections without an explicit shared identity/admission boundary.
- Authorization during Group-0 loss: new management writes stop in all cases.
  Decide whether existing data services must also stop after bounded authority
  expiry, or may continue under their own Paxos/data-plane authority. The former
  matches a strict "only Group-0-authorized nodes are active" policy but couples
  whole-cluster availability to Group 0. Specify lease timing, renewal, fencing
  enforcement points and recovery before claiming that stricter policy.
- Discovery admission trust: explicitly approve discovered candidates in UI,
  or provision deployment credentials for automatic admission. Discovery packets
  and rack hints alone are not authenticated authorization.
- Production monitor location: host system service controlling light containers,
  or a dedicated management container with narrowly scoped runtime access.
  Choose privilege and recovery boundaries; do not expose the containerd socket
  to every application/UI container by default.
