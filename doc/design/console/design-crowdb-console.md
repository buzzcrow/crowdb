<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Console (Overview)

The `crowdb-console` component provides a Web UI and a CLI that share one
Rust core for managing `crowdb-kv-server` clusters. This overview covers
the backend, data model, CLI, and hosting concerns; the frontend SPA
design is detailed in the sub-design `design-crowdb-console-ui.md`.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [2. High-Level Architecture](#2-high-level-architecture)
  - [2.1 Call Path](#21-call-path)
  - [2.2 Reuse Boundary](#22-reuse-boundary)
- [3. Data Model](#3-data-model)
  - [3.1 Physical (deployment) view](#31-physical-deployment-view)
  - [3.2 Logical (usage) view](#32-logical-usage-view)
  - [3.3 Source of truth and freshness](#33-source-of-truth-and-freshness)
  - [3.4 Design decisions](#34-design-decisions)
- [4. Console Configuration and Authority](#4-console-configuration-and-authority)
  - [4.1 Separated local configuration](#41-separated-local-configuration)
  - [4.2 Runtime observation](#42-runtime-observation)
  - [4.3 Group 0 authority](#43-group-0-authority)
  - [4.4 Local runtime namespace](#44-local-runtime-namespace)
- [5. Node Access Model](#5-node-access-model)
  - [5.1 Two transports per node](#51-two-transports-per-node)
  - [5.2 SSH defaults (russh)](#52-ssh-defaults-russh)
  - [5.3 Process lifecycle (deploy / start / stop)](#53-process-lifecycle-deploy--start--stop)
- [6. Web UI Backend (Axum)](#6-web-ui-backend-axum)
  - [6.1 Design Rules](#61-design-rules)
  - [6.2 In-process test API](#62-in-process-test-api)
  - [6.3 Orchestration semantics](#63-orchestration-semantics)
  - [6.4 Resolution rules](#64-resolution-rules)
  - [6.5 Frontend contract](#65-frontend-contract)
- [7. CLI Design](#7-cli-design)
- [8. Error Model and Operation Logging](#8-error-model-and-operation-logging)
- [9. Observability](#9-observability)
- [10. Hardware mutations](#10-hardware-mutations)
- [11. Cluster teardown and verification](#11-cluster-teardown-and-verification)

## 1. Goals and Non-Goals

### Goals
- Single workspace project `crowdb-console` delivering a Web UI and a CLI that share one Rust core.
- Operate against any number of `crowdb-kv-server` instances via their public surfaces (HTTP management API + crowdb-rpc KV / health).
- Model a **Rack → Node → Server Instance → Store → Group → Replica** hierarchy, including a "simulated hardware" mode that runs entirely on `127.0.0.1`.

### Non-Goals
- Bypassing `crowdb-kv-server` to talk to Paxos / WAL / storage internals.
- Multi-tenancy and a general audit-log service.
- Making a console-local file authoritative for cluster topology.

Local deployments use one stable directory per logical server below their
runtime namespace. Each server owns its `data/`, `config/`, `log/`, and
`artifacts/` directories. Process IDs are recorded as live-process ownership,
not used as logical directory identities, so restart continues to use the same
server directory and port assignments.

The default simulated topology is one rack containing all requested nodes.
Benchmarks that need distinct failure domains create additional racks
explicitly; full-stack deployment does not silently move nodes between racks.

## 2. High-Level Architecture

`crowdb-console` is **one project** split across the `lib/` and `app/`
workspace roots: a shared core lib plus two binaries. The console is a
general cluster-management surface (not limited to CROWDB), so crate
names use the `crowdb-*` prefix without `kv`.

```
lib/crowdb-console-shared/   (lib)   data models, HTTP+crowdb-rpc clients, registry, aggregator, error model, SSH session pool, workload generator
app/crowdb-web/              (bin)   Axum backend, static asset server, proxy routes
  src/                             Rust source
  ui/                              React + Vite frontend source (TS, shadcn/ui, React Flow)
  tests/                           integration tests
app/crowdb-cli/              (bin)   clap-based CLI; depends on shared
```

Targets:
- `crowdb-console-shared` → reusable lib for both frontends.
- `crowdb-web` → bin, serves UI + API on `:9920`.
- `crowdb-cli` → bin, the user-facing CLI.

### 2.1 Call Path

The web frontend (SPA backed by Axum) and the CLI follow the same
path through the shared `ops` module:

- **Web**: `user → crowdb-web (Axum) → shared (ops module) → group-0 sysdata + crowdb-kv-server`
- **CLI**: `user → crowdb-cli → shared (ops module) → group-0 sysdata + crowdb-kv-server mgmt`

Both frontends build an `OpContext` with Group 0 discovery seeds. Attached CLI
commands select `--cluster` or `--management-endpoint`, verify the confirmed
cluster UUID through the management origin, then verify the same UUID through
native Group 0 before hardware/KV operations and native workloads. Production
Web uses a versioned process configuration. Both read confirmed hardware and logical records through Group 0
and resolve node management endpoints from live registration. The process
launch registry is local to each bare-metal console. Docker process state is
owned by `crowdb-monitor`.

The CLI talks directly to group-0 system metadata via
`CrowdbSysmdClient` and to individual `crowdb-kv-server` management
APIs via `ServerClient` — no `crowdb-web` intermediary. The `ops`
module in `shared` holds the operation logic that both frontends
call.

```
                ┌──────────────┐        ┌──────────────┐
   user ───►    │  crowdb-web  │   or   │  crowdb-cli  │     (frontend)
                └──────┬───────┘        └──────┬───────┘
                       │   parse input,        │
                       │   render output       │
                       └──────────┬────────────┘
                                  ▼
                          ┌───────────────┐
                          │    shared     │              (business logic:
                          │  (lib crate)  │               ops module,
                          └──────┬────────┘               leader discovery,
                                 │                        registry controls,
                  ┌──────────────┼──────────────┐         SSH session pool)
                  ▼              ▼              ▼
               HTTP            crowdb-rpc            SSH
                  │              │              │
                  ▼              ▼              ▼
              ┌────────────────────────────────────┐
              │           crowdb-kv-server            │     (one per node)
              └────────────────────────────────────┘
```

### 2.2 Reuse Boundary

- All "what to do" lives in `shared` (e.g. `ops::kv_logical::add_group`,
  `ops::kv_server::deploy`, `ops::kv_data::put`, `ops::hardware::add_rack`).
- `web` (Axum) and `cli` (clap) only parse input and render output.
- The web SPA **does not** reimplement business logic; it calls
  `shared` via the Axum backend, never `crowdb-kv-server` directly.
- Both frontends build an `OpContext` and call `shared`'s `ops`
  module directly — the CLI from its verified application target,
  the web backend via `AppState::op_context()` (sharing the
  cached `CrowdbKvClient` and Group 0 management seeds).
- Shared hardware and logical operations provide the same authority and
  conditional publication rules to both frontends. Deployment-mode policy
  controls which process and hardware mutations Web exposes.

## 3. Data Model

The console exposes **two hierarchy views** of the same cluster. Both
views describe the same underlying entities; they differ only in the
direction from which the cluster is observed.

### 3.1 Physical (deployment) view

> "What hardware exists, and what is running on each piece of it."

Rooted at **Rack → Node → Server → PxStore → PxGroup → {LocalReplica,
RemoteReplica…}**. Every entity below `Node` is described from that
node's vantage point. A `PxGroup` has exactly one local replica plus
N−1 remote-replica proxies. This mirrors the `crowdb-kv-server` internal
data structure, which is why this view is also the "debugging view":
the API surfaces the remote-list explicitly so an operator can spot
bugs where a node failed to register all of its peers.

Identity is the parent chain
`(rack_id, node_id, store_id, group_id, replica_id)`.

### 3.2 Logical (usage) view

> "What stores and groups exist in the cluster, regardless of where
> they live."

Rooted at **Cluster → Store → Group → Replica…** with a unified replica
list (no local/remote split; each replica carries a `node_id`). This is
the view that KV traffic, leader resolution, and routine cluster
operations use. Shared operations translate logical IDs into confirmed
membership and live endpoints for both frontends.

Identity is `(store_id[, group_id[, replica_id]])`.

### 3.3 Source of truth and freshness

- **Group 0:** rack and node identity, nonsecret SSH connection settings and
  credential references, disk hierarchy, bindings, KV stores, groups,
  replicas, and service registration.
- **Local process inputs:** a versioned Web process configuration, bare-metal
  launch registry, and per-console secret store. Neither topology nor inline
  SSH secrets are accepted in the launch registry.
- **Live state:** management health, process identity, and current endpoints.
  Docker reads monitor-owned process state; bare-metal process identity is
  checked by `LaunchRuntime`. A stopped process is not a live registration.
- **Unavailable authority:** missing or ambiguous registration and Group 0
  outages are reported as unavailable. The monitor and launch registry do not
  supply fallback topology.

### 3.4 Design decisions

- **No `server_id` namespace.** The server's mgmt/crowdb-rpc URLs live inside
  `Node.server` and are never exposed in console-facing JSON URLs. Since
  the console enforces one server per node, node identity *is* server
  identity.
- **Local/remote split is visible only in the physical view.** The
  logical view collapses replicas into a unified list so cluster-level
  operations can ignore placement. The physical view keeps the split
  for debugging missing peer registrations.
- `StoreView` / `GroupView` / `ReplicaView` reuse `crowdb_kv::cluster::info`
  where possible; the console-side wrapper adds the `node_id`
  projection that the per-server protocol does not encode.

## 4. Console Configuration and Authority

### 4.1 Separated local configuration

`WebProcessConfig` contains the listener, Group 0 management seeds, UI and log
paths, deployment mode, and (for Docker) the monitor status path. Configured
managed Web requires this versioned input. Docker rejects a launch registry.

Standalone Web starts without `--config` in the persistent console `default`
namespace. Its private, atomically replaced `config.json` retains pre-bootstrap
hardware, process launch inputs, and a post-bootstrap topology cache. Web restart
recovers KV processes first, discovers Group 0, reloads its confirmed records,
and then recovers auxiliary services from their recorded launch inputs. The
cache never overrides initialized Group 0 topology. A malformed file or failed
initialized authority recovery reports an error without resetting operator data.

`LaunchRegistry` contains bare-metal process policy: service, node, host,
binary, service config, workspace and auto-start setting. Runtime PID and
start-time identity are retained separately by `LaunchRuntime`. SSH credential
reference IDs are read from Group 0 and resolved against each console's local
secret store. Group 0 never contains private keys, passwords, PIDs, images or
container IDs.

The `ConsoleConfig` struct is an operation input for bootstrap and local
development; standalone Web persists it locally until bootstrap and retains
launch inputs for restart. A sealed `BootstrapIntent`
retains pre-Group-0 identity across interruption and is deleted only after all
committed records are verified.

### 4.2 Runtime observation

The production `/api/preview` snapshot reads Group 0 hardware and logical
records and validates live service registrations. Docker overlays monitor
process status, while bare-metal Web uses its local launch runtime. Missing
monitor status makes Docker runtime observation unavailable. The in-process
Web test router keeps a monitor cache for fixture orchestration; that cache is
not production topology authority.

Manual node Web refreshes per-node KV topology through the admitted management
endpoints before physical reads. Remote process health comes from this live
observation; remote PIDs never enter Web's local process table. The service
list separately queries admitted monitors over their authenticated control
channels for actual live children, with `pid_node_id` identifying the process
namespace. This observation verifies local cluster/node binding and remains
available during Group-0 failure. The Web host neither probes these PIDs via
its own `/proc` nor stores or signals them as local processes. A failed probe
marks the node down, while local launch observations still require a tracked PID.

### 4.3 Group 0 authority

System group (store 0, group 0) replicates hardware and KV-cluster metadata.
Bootstrap initializes selected KV members, wires peers and conditionally
publishes the rack, node, store, group and replica records. A retry compares
sealed identity and already committed content, writes only missing records,
and rejects conflicting content. Nonmember KV processes receive Group 0 seeds
and must register exactly one live identity before logical operations use them.

The metadata namespaces are `/hw/rack`, `/hw/node`, `/hw/dg`, `/hw/disk`,
`/hw/owner`, `/hw/bind`, `/kv/store`, `/kv/group`, `/kv/replica`, and `/srv`.
Logical mutations confirm all node-side steps before publishing membership;
conditional writes and confirmed reads reconcile a lost response. A local
launch or monitor record never substitutes for a missing Group 0 result.

### 4.4 Local runtime namespace

The default local root is `.crowdb-runtime/`, divided into `ephemeral/`,
`persistent/`, `artifacts/`, and `ports/`. A mini-cluster or retained console
deployment is one persistent namespace. Its manifest stores the namespace
identity and the mapping from each `(service kind, logical instance)` to a
port. Service paths live below
`services/<service>/<logical-instance>/{data,config,log,artifacts}`.

`start_new` creates an identity and its assignments. `restart` reuses the
same paths and assignments and never asks for replacement ports. `stop`
terminates recorded processes while preserving the manifest and data.
Deletion is the only operation that releases persistent claims and removes
the namespace. If a recorded port is held by another owner, restart reports
the conflict; it does not silently select a different endpoint.

Disposable deployment and E2E fixtures use ephemeral namespaces with the
same layout. A saved CLI cluster inside an ephemeral fixture inherits that
fixture's process and port ownership; its launcher may exit without releasing
ports still used by the fixture. Process records remain outside disposable
data so cleanup can terminate services even after data removal. Cleanup releases
these nested claims along with the fixture. All local CROWDB data, generated
configuration, logs, benchmark output, and coordination state stay below this
root; no default path uses the system temporary directory. Ordinary clean
operations preserve persistent
namespaces.

## 5. Node Access Model

### 5.1 Two transports per node
| Purpose | Transport |
| --- | --- |
| Deploy / start / stop `crowdb-kv-server` process; copy binary | SSH |
| Runtime mgmt API (add store/group, list, health) | HTTP |
| Runtime KV ops, paxos health | crowdb-rpc |

### 5.2 SSH defaults (russh)
- Crate: **`russh`** (pure Rust, async). No shell-out fallback.
- Default auth: `~/.ssh/*` keys (agent + standard key paths).
- Alternative auth: explicit key path; explicit `user/password`.
- Default host: `127.0.0.1` with the current OS user.
- Pre-flight: every operation calls `ssh::probe(node)` which performs a real handshake before any side-effecting work. Failure surfaces as `NodeUnreachable { node_id, reason }`.

**SSH credential boundary:** Group 0 stores only the SSH user, port and
credential reference associated with a node. Each bare-metal console resolves
the reference in its own local secret store. Bootstrap intent rejects inline
private keys and passwords; the launch registry accepts references only.

Node admission installs each node's public Ed25519 key and proves access in
both directions with strict host-key checking. Preparation reuses one
key-authenticated candidate session and one session per existing peer; the
two directional proofs run concurrently. Ordered preparation commands reuse
their authenticated connection. A failed stage preserves durable progress for
retry, while initialization passwords remain only in the submitted request.

### 5.3 Process lifecycle (deploy / start / stop)

DiskDB deployment accepts an optional positive `keepalive_interval_secs` for
its normal heartbeat policy. Omitting it retains the server default; the
committed remote service configuration preserves it across restart. This
setting does not change ownership or deployment protection rules.

Chunk-KV deployment optionally accepts `stream_chunk_capacity_bytes` (1–256 MiB)
and positive `stream_extent_page_entries`. Omitted values retain server defaults.
These storage parameters are restricted to Chunk-KV and become part of its
committed service configuration, including subsequent container-owned restarts.

`LaunchRuntime` uses the validated launch registry for local or SSH process
start, restart, stop and readiness checks. It records PID plus process start
time as local runtime identity and refuses to signal an unrelated process.
Auto-start policy is reconciled on Web startup and reload. A successful process
launch is not a substitute for a Group 0 service registration. Docker delegates
child recovery and status to `crowdb-monitor`.

## 6. Web UI Backend (Axum)

### 6.1 Design Rules

Configured managed Web uses the managed router and a versioned process configuration.
`/api/preview` combines confirmed Group 0 records, live registration, and the
mode-specific process view. `/api/stores/...` provides authenticated logical
mutations through shared operations in both modes. Bare-metal Web additionally
exposes rack, node, disk-group and disk reads and authenticated mutations, plus
registry-backed launch controls and bootstrap. Automatic single-node Docker Web does not expose
hardware or process mutation routes. Manual node Web exposes candidate/rack
preparation before bootstrap and routes subsequent shared management through
Group 0 and authenticated monitor execution. Unknown managed API routes report
unavailable rather than entering an in-memory topology path.

A mutation is accepted only after the required node-side steps and Group 0
publication are confirmed. The Console assumes a root operator until UI login is introduced. Automatic single-node container
mode rejects topology, deployment, and disk-management writes at the
backend; logical and Access operations use server-held protocol credentials.
Docker Web is writable by default. The process configuration explicitly sets
`readonly = true` to reject every non-read request with 403; `/api/mode` exposes
this flag and the SPA disables write controls accordingly. The public
single-container profile enables readonly, while internal writable profiles
select the same release image with readonly disabled. Docker physical-management
restrictions apply independently of readonly.
The SPA calls the Axum backend; it does not talk directly to KV management
endpoints.

### 6.2 In-process test API

The in-process Web router and `--test-mode` retain fixture orchestration for
browser and integration tests. Their recursive physical views and monitor cache
help exercise the UI. Standalone Web uses these resource APIs with persistent
bootstrap/recovery inputs; `--test-mode` uses isolated ephemeral state. Neither
provides fallback topology for configured managed requests.

### 6.3 Orchestration semantics

For each multi-node operation in the logical tree, the backend obeys
these rules:

- **Plan first, act second.** Resolve every required node + replica id
  from Group 0 membership and live service registrations before issuing
  mutation RPCs. Missing or ambiguous registrations fail the operation.
- **Built on physical primitives.** The orchestrator only calls the
  per-node physical mutators; it never invents a side channel.
- **All-or-nothing where feasible.** On partial failure, attempt to
  undo successful sub-steps and surface the resulting state in the
  error body.
- **Confirmed membership publication.** Complete peer wiring before publishing
  group or replica membership. A new group and its initial replica records
  commit in one conditional batch. Create records conditionally; a concurrent
  conflicting record is preserved. A lost write response is resolved only by
  a linearizable read that confirms the intended record.
- **Deletion preserves authority on node failure.** Confirm deletion on every
  hosting node before removing membership. Store hosts include nodes from
  replica records as well as the store record. Remove descendants before
  parents; an already absent node-side object permits retry.
- **Idempotent retries.** A repeat of the same logical request must
  converge to the same state.
- **Read after write.** A mutation returns only after the required node-side
  and Group 0 confirmation steps complete.

### 6.4 Resolution rules

- Unknown ids → `404`.
- Unreachable node for a physical-tree call → `502 Bad Gateway`.
- Partial logical-tree failure → roll back and report `409 Conflict`
  with structured per-node outcomes (not `207 Multi-Status`).
- All handlers are thin wrappers around `shared` entry points; the
  CLI calls the same entry points.

### 6.5 Frontend contract

The frontend SPA design lives in `design-crowdb-console-ui.md`. The
backend-facing contract here:

- Bundle output is `app/crowdb-web/ui/dist/`; `crowdb-web` serves
  it via SPA fallback.
- The SPA polls the management API on a short interval. An unavailable
  authority clears stale logical rows and is shown explicitly.
- No `/api/cluster/snapshot` aggregate endpoint.

## 7. CLI Design

- `cluster start/stop` own OCI runtime lifecycle through a named local ownership
  receipt. `start` accepts one or three nodes and delegates preparation to the
  normal node monitor. Runtime inspection, logs and image operations use the
  runtime's tools; cleanup belongs to the independent host/Pixi helpers.
- `cluster status/check` observe confirmed application readiness over management
  endpoints, without a runtime socket. A finite check returns nonzero when any
  required dependency is unmet. Runtime facts unavailable remotely remain unknown.
- Hardware, logical KV, S3 and benchmark commands require `--cluster` or
  `--management-endpoint`. Every selected origin must report the same confirmed
  UUID; saved bindings pin that UUID. Native consumers additionally verify
  transport reachability and the committed Group-0 UUID before data access.
  There is no system-address, localhost or launch-registry fallback.
- Hardware and logical KV operations call Group 0 and node management APIs
  directly after target verification. Signed S3 clients obtain access credentials
  and the advertised public URI from the selected management origin.
- Endpoint-only attachment grants no runtime ownership. It cannot start or stop
  containers, create a runtime receipt, change the image or prepare infrastructure.
- The retained workload families are KV, DiskDB, ChunkDB, ChunkIO and S3.
  `bench kv prepare` writes its declared dataset into an existing group.
  Workloads never initialize groups, deploy services, reset cluster topology or
  stop a cluster. Workload-owned data cleanup and ordinary client transport
  workers remain allowed. Standalone RPC tests own their server/client lifetime.
- `kv store/group/replica/data` retain normal topology and data operations.
  Process deployment and reset commands are absent from the command tree.
- Output identifies the copyable command and final result. HTTP exchanges show
  response metadata and format structured bodies; binary bodies are identified
  by type and byte count. Status/check support structured JSON output.
- Ordinary commands log only to the console. A benchmark writes the latest run
  under the runtime root's `artifacts/cli/bench-<family>` directory; the next run
  replaces only that family's previous benchmark log. KV workloads also retain
  a structured `report.json` with operation counts, errors and measured latency
  alongside the human-readable console report. Measurement tools preserve each
  completed invocation before another run replaces the latest directory.

## 8. Error Model and Operation Logging

- `shared::Error` enum covers `NodeUnreachable`, `UpstreamRpc`,
  `Validation`, `NotFound`, `Conflict`. HTTP maps to 4xx/5xx; CLI maps
  to exit codes (0 ok, 1 user error, 2 cluster/network error).
- **Console transcript** — every invocation prints the copyable command and
  result. Direct S3 requests additionally show request/response headers and a
  body marker. Text is visible, XML/JSON is formatted, and binary content is
  summarized by byte count without duplicating it in the trace.
- **Retained diagnostics** — only benchmark commands write files. The latest
  run for each benchmark family lives beneath the runtime root's
  `artifacts/cli/bench-<family>/`
  and replaces the previous run. Benchmark tracing is also mirrored to the
  console. Routine outbound HTTP calls are debug-level so default output keeps
  lifecycle events, warnings, and errors without per-request noise.

## 9. Observability

- `tracing` everywhere; `--vv` switches CLI to debug.
- Web backend exposes `/healthz`. **`/metrics` is deferred**. The Rust
  Prometheus story has multiple competing crates; we will pick one when
  broader observability work for `crowdb-kv-server` begins.
- All console-issued operations attach a correlation id propagated as
  `x-crowdb-kv-corr-id` to `crowdb-kv-server` request headers.
- Regression service metrics have a content contract: KV emits `rust`,
  `cpp-rpc`, `cpp-tree`, and `misc`; DiskDB and ChunkDB emit `rust`,
  `cpp-rpc`, and `misc`; DiskIO emits `cpp-rpc` and `misc`. A tracing or RPC
  log may legitimately remain empty when its configured level observed no
  events; metric validation therefore checks metric sections and counters
  independently of auxiliary log size.

## 10. Hardware mutations

CLI and bare-metal Web use the shared Group 0 hardware operations. Rack,
node, disk-group and disk changes update parent and child records in one
conditional batch where membership changes. Matching retries are confirmed;
conflicting concurrent writes preserve the existing record. Automatic single-node Docker Web does
not expose hardware or process mutations. Manual Docker nodes use the same
shared hardware operations after confirmed Group 0 publication.

The physical Cluster workspace shares one tree and selection across Topology and
Capacity. Cluster/Rack node cards and capacity tables reuse bounded observations;
Node storage owns discovery, accepted file/supplied operations and retained
candidates. Group 0 defines effective disk membership. Candidate Join validates
reviewed media and retains one immutable operation per disk; accepted work runs on
the owning monitor after the browser closes. Offline/Remove/Rejoin retains bytes
and allocation identity, with exclusion and validation gates before participation.
Physical discovery is never permission to reinitialize or replace an existing disk.

Concurrent Access deployment reserves one cluster-scoped Console catalog
Initialize/Activate operation pair in Group 0. Nodes replay that pair; conflicting
local journals remain visible and are not rewritten. Access credential bootstrap
also reserves one encrypted user candidate before conditionally publishing its
access-key record. Response loss/concurrent nodes cannot replace an existing
credential; duplicate or disabled users require explicit review. Enumeration is
bounded and rejects truncated/timed-out results instead of authorizing issuance
from incomplete state. Secrets remain in the established private credential path.

## 11. Cluster teardown and verification

`cluster destroy` requires the local launch registry. It reads confirmed Group
0 membership, removes user stores and groups through shared logical operations,
then removes the system group last. Only after metadata teardown succeeds does
it stop processes named by that console's launch registry. A failed or
unconfirmed step returns an error instead of deleting presumed local topology.

There is no orphan-guessing reset command. A stopped or unreachable node does
not imply its membership should be deleted. `cluster clean` derives its
replica targets from Group 0, wipes each live target, and waits for a new
leader while preserving topology.

Manual node discovery and bootstrap follow the
[deployment architecture](../deploy/design-crowdb-deploy.md). The Cluster sidebar
retains a separate candidate list and explicit draft, fixed bootstrap, publication,
active, stale authority and cleanup states. Each UI reads its local monitor's
observations and the shared Group 0 registry. Discovery filtering selects another
cluster for inspection without merging its membership. Authenticated node updates
retain UUID/numeric allocations and physical failure domains. Loss of Group 0
blocks management; existing data groups use their own Paxos authority and confirmed
endpoint hints for data operations.
