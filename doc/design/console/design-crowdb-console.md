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
  - [7.1 Four-Domain Hierarchy](#71-four-domain-hierarchy)
  - [7.2 Command Hierarchy](#72-command-hierarchy)
  - [7.3 `cluster init` — bootstrap special case](#73-cluster-init--bootstrap-special-case)
  - [7.4 `cluster clean` — data wipe boundary](#74-cluster-clean--data-wipe-boundary)
  - [7.5 `kv server delete` — graceful + require-empty](#75-kv-server-delete--graceful--require-empty)
  - [7.6 Bench subcommand](#76-bench-subcommand)
  - [7.7 Bench lifecycle verbs (deploy / prepare / run / teardown)](#77-bench-lifecycle-verbs-deploy--prepare--run--teardown)
  - [7.8 S3 mini-clusters and benchmarks](#78-s3-mini-clusters-and-benchmarks)
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

Both frontends build an `OpContext` with Group 0 discovery seeds. The CLI
uses `--system-ip` and `--system-port`; production Web uses a versioned process
configuration. Both read confirmed hardware and logical records through Group 0
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
  module directly — the CLI from `--system-ip` / `--system-port` global
  flags, the web backend via `AppState::op_context()` (sharing the
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
same layout. All local CROWDB data, generated configuration, logs, benchmark
output, and coordination state stay below this root; no default path uses the
system temporary directory. Ordinary clean operations preserve persistent
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

### 5.3 Process lifecycle (deploy / start / stop)

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
registry-backed launch controls and bootstrap. Docker Web does not expose
hardware or process mutation routes. Unknown managed API routes report
unavailable rather than entering an in-memory topology path.

A mutation is accepted only after the required node-side steps and Group 0
publication are confirmed. The Console assumes a root operator until UI login is introduced. Container
mode still rejects topology, deployment, and disk-management writes at the
backend; logical and Access operations use server-held protocol credentials.
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

- Binary: `crowdb-cli` (four-domain structure: `crowdb-cli <domain> <verb>`).
- Parser: `clap` derive; one module per domain under `commands/`.
- **Direct-to-system-group call path.** Every verb builds an `OpContext`
  seeded with `--system-ip` / `--system-port` (any system-group management
  endpoint) and talks directly to system metadata via `CrowdbSysmdClient` and to
  individual `crowdb-kv-server` mgmt APIs via `ServerClient`. There is
  no `crowdb-web` intermediary. Leader discovery is automatic. The global
  connection flags are `--system-ip` (default `127.0.0.1`, env
  `CROWDB_SYSTEM_IP`) and `--system-port` (default system-group management
  port, env `CROWDB_SYSTEM_PORT`).
- Output is console-first and human-readable. Every invocation identifies the
  copyable command and final result. HTTP operations show request/response
  headers and body disposition; JSON and XML response bodies are pretty
  printed, while binary bodies are represented by type and byte count. ANSI
  colors distinguish commands, HTTP sections, body metadata, warnings, errors,
  and final success/failure.
- Every non-benchmark command logs only to the console. Each benchmark writes
  the same tracing events to the console and to
  `<cwd>/cli-log/bench-<family>/`; a new run replaces that family's previous
  directory so benchmark logs do not accumulate.

The full command hierarchy is defined in the `clap` derive structs;
this section covers design rules only.

### 7.1 Four-Domain Hierarchy

`cluster` owns hardware metadata and bootstrap, clean, destroy and status.
`kv` owns KV server launch controls, logical store/group/replica operations
and KV data commands. `chunk` owns storage-service launch controls and
maintenance. `bench` owns workload runners. The CLI connects to Group 0
directly and shares the authority operations with production Web.

### 7.2 Command Hierarchy

The `clap` command enums define the exact verbs and flags. Hardware and
logical commands use Group 0 for identity and membership. Process controls
require `--registry`; `cluster init` additionally requires sealed bootstrap
input for first creation. Development `local-deploy` runs a one-shot loopback
cluster and prints the Group 0 management seed for later CLI invocations.

### 7.3 `cluster init` — bootstrap special case

`cluster init` requires `--registry` and a versioned `--bootstrap-file`
for first creation, or a sealed retry intent beside the registry. It takes
`--nodes <n1,n2,n3>` and bootstraps group-0/store-0 on those nodes via
direct node REST calls (the `POST /system/init` mechanism, §4.3),
wires remotes, and writes the hardware + KV-cluster topology into
group-0 sysdata. After `cluster init` completes, subsequent commands
use `--system-ip` / `--system-port` to connect to any node in the newly
created system group.

Initialization also sends Group 0 management seeds to deployed KV processes
outside the selected member set and waits for exactly one live registration
per node. Those processes retain connection hints locally across restart;
they do not become Group 0 members. Logical operations use the confirmed live
registrations rather than treating launch configuration as a live endpoint.

### 7.4 `cluster clean` — data wipe boundary

`cluster clean --store <id> --group <id>` derives the target replica nodes
from confirmed Group 0 membership and resolves every live KV management
registration. It asks each target to wipe user data, then waits for a new
leader. Group 0 hardware, logical records, and process launch policy remain
intact. A missing group, registration, or acknowledgement fails the operation;
a local launch record cannot justify a wipe.

`--restart-services` additionally restarts locally configured DiskIO, DiskDB
and ChunkDB processes in dependency order through `LaunchRuntime`. It requires
a validated launch registry before the wipe begins. KV processes stay running
so Group 0 remains available.

### 7.5 `kv server delete` — graceful + require-empty

All operations use graceful Paxos reconfiguration — no force-kill
path. `delete` requires the server to be **empty**: no replicas, no
groups, no stores hosted. The operator must delete in bottom-up order
— replicas → groups → stores → server → node — before the server or
node can be removed. The CLI refuses `kv server delete` if the server
still hosts replicas, with an error listing the replicas/groups/stores
that must be removed first. Same policy applies to `cluster node
remove` — the node must have no running servers/services before
removal.

Verb distinction:
- `kv server stop` — graceful process stop (keeps server entry, can
  restart later). No reconfiguration; replicas remain registered on
  peers.
- `kv server delete` — graceful removal (requires empty server).
  Removes the server entry after confirming emptiness. No cascading
  delete — the operator does the cascade manually in bottom-up order.

### 7.6 Bench subcommand

- `bench kv <read|write|scan|mix>` runs KV workloads against a target
  store/group. `bench rpc` measures raw RPC transport throughput.
- Bench discovery starts from the explicit Group 0 management seed and
  resolves metrics hosts from confirmed replica membership and live
  registrations. It does not load a console topology file.

### 7.7 Bench lifecycle verbs (deploy / prepare / run / teardown)

The all-in-one `bench kv` verb deploys a 3-node cluster, pre-populates
keys, runs the workload, and tears down — all in one process. For
regression suites that run many sub-tests against the same cluster
configuration, this pays deploy + pre-pop overhead per sub-test. The
lifecycle verbs split this monolith into discrete steps with persistent
deploy metadata:

- **`bench deploy --name <n> --kind kv --mode mem`** — provisions a
  3-node cluster via `BenchFixture` (embedded console-web), then
  detaches the fixture so the `crowdb-kv-server` processes survive CLI
  exit. The deploy metadata (node pids, endpoints, tunables) is
  serialized below `.crowdb-runtime/persistent/bench/<name>/handle.json`
  (`ClusterHandle`). The
  `--kind` flag dispatches to kv (default), rpc (spawns
  `crowdb-rpc-fb-server`), or chunk/storage (not yet implemented).
- **`bench prepare --target <n> --keys N`** — loads handle, builds a
  `CrowdbClient` from the recorded leader endpoint, and writes N keys
  via sequential `put`. Reuses the same `format_key` / `value_for`
  logic as `bench kv`'s pre-populate path.
- **`bench run --target <n> --workload read ...`** — loads handle,
  builds an `AttachedKvTarget` (implements `BenchTarget` with no-op
  provision/cleanup), and calls the shared `run_bench` runner. Reports
  go to that namespace's `artifacts/runs/<timestamp>/`. The cluster stays running
  after the run — multiple `bench run` invocations can attach to the
  same deploy.
- **`bench teardown --target <n>`** — loads handle, SIGTERMs the node
  pids via `stop_pid_with_timeout`, removes `handle.json`. Idempotent:
  a second teardown on the same name exits 0 with "already torn down".

The all-in-one `bench kv` verb is preserved as the quick one-shot path.
Regression scripts use the lifecycle flow: deploy once, prepare when needed,
run compatible sub-tests with reset boundaries, then teardown once. Each
sentinel accepts environment overrides for case selection and duration; KV
read, write, and scan also accept a reduced keyspace. These controls provide a
short structural smoke without changing the default regression matrix.

`ClusterHandle` is persistent namespace metadata, not a config extension. The
single `.crowdb-runtime/` root is gitignored. The
`crowdb-kv-server` child processes survive CLI exit because
`lifecycle::deploy_local` spawns them with `kill_on_drop(false)`.

### 7.8 S3 mini-clusters and benchmarks

`crowdb-cli s3 cluster start --root <path>` is the simple local operator
path. The location, rather than the caller's global console configuration, is
the cluster identity and recovery boundary:

- a missing or empty location is initialized as a three-node cluster;
- a location containing `s3-mini-cluster.json` and versioned local launch state is restarted
  with the same service identities, endpoints, launch commands, KV/WAL/tree
  directories, and DiskIO files;
- a non-empty location without the marker is rejected without modification.

The initial dependency order is KV, DiskDB, DiskIO, ChunkDB, chunk-KV, then
access-server. Every service must become ready before its dependent starts.
Each node owns one sparse, file-backed DiskIO file below the cluster root;
normal S3 bucket metadata, object metadata, streams, and object bytes therefore
survive a complete stop and restart. `stop` terminates recorded processes but
does not remove configuration or storage. `delete` stops the cluster, releases
its persistent port claims, and removes the named location. `status` is
read-only.

First start seals bootstrap intent before publishing Group 0 and publishes the
complete marker only after the access endpoint is ready. Interrupted launch
steps replay from retained process and seed inputs; committed Group 0 content
is verified before a missing step is retried. A non-empty foreign directory is
rejected. Local state cannot reconstruct topology during a Group 0 outage.

The mini topology is intentionally loopback and places its simulated nodes in
one rack, so ChunkDB explicitly permits colocated fragments. This is not the
production capacity contract: a production planner must reject a requested
copy or EC scheme unless distinct healthy failure domains satisfy it.

The local access-server binds only to `127.0.0.1` and explicitly enables its
trusted-network authentication mode. This keeps the local CRUD path small: the
shared `ops::s3` client sends ordinary S3 HTTP operations directly and returns
structured exchange metadata; the CLI renders the console transcript and
streams the result. It is not a production authentication mode and must never
be used for a non-loopback listener. Bucket and object
commands take the same `--root`, discover the persisted endpoint, preserve
S3 errors, and do not fall back to another mutation.

The durable local record holds only versioned launch inputs, process
identities, bootstrap seeds, the storage profile, loopback endpoint and
nonsecret tenant name. It does not contain rack, node or logical topology. The
access master key is injected into a child only while it starts.

`crowdb-cli bench s3` owns a separate, invocation-scoped memory profile. KV and
WAL blocks use memory backing, DiskIO uses memory disks, and chunk-KV keeps its
readable tree and value path in memory. The memory disks retain a large logical
address space while allocating resident pages on demand; logical capacity is
therefore independent of the benchmark's resident-memory budget. The benchmark
always stops its cluster, while leaving the stopped work directory and logs as
diagnostic artifacts.

The benchmark has write, read, range-read, list, and deterministic weighted
mix workloads. It prepares a fixed dataset, runs warm-up outside measurement,
then admits requests until either the operation limit or duration limit is
reached. Workers keep local latency and failure accumulators and use only an
atomic admission counter on the request path. Cleanup deletes only prepared or
actually written benchmark objects, with bounded asynchronous concurrency.

Results report backing choices, measured duration, attempts, successes,
failures, throughput, average latency, p50, p99, prepared bytes, and process
peak resident bytes. Read validates the full response length, range-read
validates the selected interval length, and list validates ordering and
continuation progress. Failures are separated into metadata, protocol,
transport, and resource categories. RSS is sampled asynchronously; exceeding
the configured budget makes the run fail instead of publishing valid
throughput.

The following invariants apply:

- **S3-C1 — Recovery boundary:** only a complete marker identifies a
  restartable file-backed cluster, and stop never removes its stored data.
- **S3-C2 — Real request path:** every benchmark operation traverses the normal
  S3 endpoint and validates the response property relevant to its workload.
- **S3-C3 — Bounded execution:** duration, operation count, concurrency, and
  resident memory are explicit limits; exhausting memory is an error.
- **S3-C4 — Simple ownership:** persistent operator data and ephemeral
  benchmark data use separate storage profiles and lifecycle ownership.

## 8. Error Model and Operation Logging

- `shared::Error` enum covers `NodeUnreachable`, `UpstreamRpc`,
  `Validation`, `NotFound`, `Conflict`. HTTP maps to 4xx/5xx; CLI maps
  to exit codes (0 ok, 1 user error, 2 cluster/network error).
- **Console transcript** — every invocation prints the copyable command and
  result. Direct S3 requests additionally show request/response headers and a
  body marker. Text is visible, XML/JSON is formatted, and binary content is
  summarized by byte count without duplicating it in the trace.
- **Retained diagnostics** — only benchmark commands write files. The latest
  run for each benchmark family lives beneath `<cwd>/cli-log/bench-<family>/`
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
conflicting concurrent writes preserve the existing record. Docker Web does
not expose hardware or process mutations.

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
