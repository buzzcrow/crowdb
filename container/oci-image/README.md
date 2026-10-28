<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Shared OCI image

Build one Linux amd64 image for Docker and containerd. Ubuntu 24.04 is pinned
by digest; the common recipe packages the existing release binaries, UI and SSH.
The build requires Pixi and Linux kernel privileges, with no Docker installation,
socket or containerd service. Running the image is a separate operation.

## Build and import

```bash
pixi run oci-build --output target/crowdb.oci.tar
pixi run --manifest-path container/oci-image/pixi.toml inspect target/crowdb.oci.tar --receipt
# Optional import into the system Docker image store:
pixi run --manifest-path container/oci-image/pixi.toml import target/crowdb.oci.tar --image crowdb-node:dev
```

`oci-build` compiles/stages with the repository environment, then selects the
locked standalone builder environment. `--staged` reuses an existing
`target/container-runtime`. Unsupported platforms fail before compilation.
Only Linux amd64 is enabled; macOS will compile inside a Linux VM backend.

Rootless builds require host-installed setuid `newuidmap`/`newgidmap`, subordinate
UID/GID ranges for the invoking user, and permitted unprivileged user namespaces.
Pixi supplies RootlessKit, BuildKit and runc; it cannot grant kernel privileges.
The builder uses the native snapshotter and host network, so no FUSE or CNI is
required. Startup errors include complete daemon diagnostics. Rootless preflight
validates these requirements before starting BuildKit.
Both rootless build/import and the explicit rootful recipe are supported.

If rootless prerequisites are unavailable, an administrator can explicitly run:

```bash
pixi run bash container/oci-image/stage.sh
pixi install --locked --manifest-path container/oci-image/pixi.toml
pixi run sudo "$(command -v pixi)" run --as-is \
  --manifest-path container/oci-image/pixi.toml build --privileged \
  --context "$PWD/target/container-runtime" --output "$PWD/target/crowdb.oci.tar"
```

There is no automatic privilege fallback. `--registry-mirror host/path` may be
repeated for Docker Hub mirrors; credential-bearing mirror/proxy settings are
rejected. Builds use a private socket and persistent cache, stop their own
process group on exit/interruption, and never stop a system daemon. Concurrent
builds need separate `--cache` directories. The output archive and JSON receipt
are finalized after descriptor/blob/config validation. A mismatched pair is
rejected by import/publication. The receipt records revision, version, base,
tool versions, staged-content hash, image manifest/config digests and archive checksum.
Local uncommitted builds carry HEAD's revision; the content hash identifies the
actual staging. Release CI checks out the exact source revision.

Docker 28.3.3's classic store was tested using explicit Skopeo conversion into a
local Docker archive. This changes only the import representation. The published
OCI graph is unchanged; Docker's local config ID differs from its OCI manifest
digest. Never substitute a local config ID for a registry digest.

The image runs Tini as PID 1 in both runtimes. It reaps orphaned SSH session
processes and forwards stop signals to the monitor; the monitor retains service
shutdown ordering and child exit ownership. No runtime-specific `--init` flag is
needed. Do not replace the image entrypoint with the monitor for normal nodes.

## Local cluster shortcuts

Build and load once, then run a named cluster:

```bash
pixi run oci-build --load
pixi run oci-run --cluster demo
# Explicit equivalent of oci-run:
pixi run oci-run-1c --cluster demo
# Use a different name and port range for a three-container cluster:
pixi run oci-run-3c --cluster demo-three --port-base 9190
pixi run oci-stop --cluster demo-three
pixi run oci-run-3c --cluster demo-three
pixi run oci-clean --cluster demo-three
# Recreate containers and bridge with the original volumes and accepted policy:
pixi run oci-run-3c --cluster demo-three
# Explicitly delete the owned data too:
pixi run oci-clean --cluster demo-three --delete-data
```

Run never compiles, downloads or pulls an image. It accepts `--image`, `--cpus`,
`--memory-mib` and `--port-base` on first creation, and retains those inputs.
A different node count or conflicting input requires another cluster name.
Ports for each node are UI/S3/Iceberg/Dataset at base/base+1/base+2/base+3;
subsequent members add 10 and 20. All published ports bind to `127.0.0.1`.
Names default to `local`; global stop/clean requires explicit `--all`.

Host receipts live in `.crowdb-runtime/persistent/oci-clusters/`. The shortcuts
verify the Docker daemon/context and exact ownership labels before effects.
Clean stops and removes owned containers and unused owned bridges. It keeps
volumes and restart inputs unless `--delete-data` is explicit. Image cleanup
only considers dangling CROWDB images, preserving retained cluster references
and images used by any running or stopped container. Attached or relabelled
networks and volumes referenced elsewhere are rejected. No global prune runs.
These host scripts invoke Docker directly and require no CLI binary or application
target binding. Explicit data deletion also removes a matching optional CLI
target binding; a binding with different ownership is rejected before deletion.

One node and three nodes use the same editable monitor lifecycle. Local mode
prepares test storage and all access services. An incomplete three-node start
waits for peers; it never falls back to singleton bootstrap. Each node retains
its identity, SSH keys, cluster binding and accepted setup in its own volume.
Discovery uses mDNS/DNS-SD; no DNS server installation or peer list is required.
Advertisements supply hints; pinned SSH identity verifies new member addresses.

## Common CLI lifecycle and application targets

The CLI can create the same local Docker clusters and wait for complete
application readiness. It shares host ownership receipts with the shortcuts;
neither entry point implements a separate bootstrap or storage deployment.

```bash
pixi run crowdb-cli --cluster demo cluster start
pixi run crowdb-cli --cluster demo-three cluster start --nodes 3 --port-base 9190
pixi run crowdb-cli --cluster demo-three cluster check --wait 180 --json
pixi run crowdb-cli --cluster demo-three cluster stop
pixi run crowdb-cli --cluster demo-three cluster start
pixi run crowdb-cli --cluster demo-three s3 bucket put example
pixi run crowdb-cli --cluster demo-three s3 object put example hello.txt --text hello
pixi run crowdb-cli --management-endpoint http://127.0.0.1:9190 s3 object get example hello.txt
pixi run oci-clean --cluster demo-three --delete-data
```

Use the built `crowdb-cli` executable on PATH, or substitute its absolute path
after `pixi run`. Start defaults to cluster `local`, one node, 1 CPU, 1536 MiB,
and console port 6135. It accepts `--image`, `--cpus`, `--memory-mib` and
`--port-base` before sealing the first launch inputs. Subsequent starts verify
those inputs and resume existing state. `--wait` bounds readiness observation;
an unmet check returns 1 and leaves accepted work resumable.

For production, `cluster start --network host` runs one node on this physical
host. Select `--runtime docker` or `--runtime containerd`; containerd also accepts
`--runtime-endpoint` and `--snapshotter native|overlayfs`. Python 3.11 or newer
and the selected runtime client must be installed on the host. Load the immutable
node image first; this command never builds or pulls it.

```bash
pixi run crowdb-cli --cluster production cluster start --runtime containerd \
  --network host --snapshotter native --image sha256:<verified-manifest> \
  --nodes 3 --cpus 2 --memory-mib 2048 --data-root /srv/crowdb/data \
  --physical-host-id <machine-id> --interface eth0 --management-address 192.0.2.10 \
  --node-policy-file /srv/crowdb/node-policy.toml \
  --password-file /srv/crowdb/private-password --device /dev/nvme1n1
pixi run crowdb-cli --cluster production cluster stop
pixi run crowdb-cli --cluster production cluster start
pixi run oci-host-clean --cluster production
```

The existing data directory must be writable by UID 10001. The node policy
must select production mode, the same cluster and initial voter count, supplied
media and reachable public protocol URIs. `--nodes 3` selects the three-voter
formation policy; run one node on each host. Host networking uses port 6135 and
rejects `--port-base`. The management address must belong to the selected local
interface. Disk files use `--disk-file`; files and devices are explicit existing
inputs and are never created by the CLI.

The CLI seals a private host receipt before creating a container. Start/resume
and stop verify the recorded daemon, Unix endpoint, context/namespace, owner,
image, resource limits, mounts and startup inputs. Resume preserves the accepted
policy and media identities; foreign same-name containers are rejected.
`oci-host-clean` works without the CLI binary and removes only that verified
container. It preserves data, raw devices, files, credentials and both launch
and application receipts, so the next start can recreate the container.

Status/check and application commands need only management HTTP access. A named
CLI target pins the observed cluster UUID; explicit management endpoints must
agree with that UUID and with one another. An unavailable selected endpoint
does not fall back to localhost. S3 commands retrieve the selected cluster's
published S3 endpoint and credentials, then issue signed native S3 requests.
They require no Docker socket or local deployment directory. HTTP diagnostics
redact authorization and session tokens. The CLI has no S3-specific cluster
lifecycle or container cleanup command.

Workloads attach to the same selected cluster. They never start, stop, reset or
provision it. Native KV/storage clients additionally need network reachability
to the advertised RPC addresses; HTTP-only port forwarding is insufficient.

```bash
pixi run crowdb-cli --cluster demo-three bench kv prepare --store 0 --group 1 --keys 100
pixi run crowdb-cli --cluster demo-three bench s3 mix --operations 100 --dataset-objects 8
pixi run crowdb-cli --cluster demo-three bench diskdb mix --duration-secs 2 --concurrency 1
pixi run crowdb-cli --cluster demo-three bench chunkdb allocate --duration-secs 2 --copy-count 1
pixi run crowdb-cli --cluster demo-three bench chunkio write --objects 1 --duration-secs 2 \
  --mirror-copies 1 --block-size 1048576 --chunk-size 16777216 --prefetch-chunks 1
```

The local storage policy accepts 1 MiB mirror strips with one or two copies.
Use the deployment's supported layout explicitly; a benchmark never changes
that policy. DiskDB checks current disk-group geometry and accounting units
instead of requiring a fixed group/disk count. `bench kv prepare` writes only
its declared keys. S3 workloads use a fresh bucket per invocation and delete
only that workload's objects/bucket. `--max-dataset-bytes` bounds the prepared
dataset; benchmark output does not claim remote memory usage or backend type.

## Direct Docker and Compose

A direct single-container cluster needs a volume and four published ports:

```bash
pixi run docker run -d --name crowdb-one --cpus 1 --memory 1536m \
  -p 127.0.0.1:6135:6135 -p 127.0.0.1:7135:7135 \
  -p 127.0.0.1:8135:8135 -p 127.0.0.1:9135:9135 \
  --mount type=volume,source=crowdb-one-data,target=/opt/crowdb/data \
  crowdb-node:dev
```

For three nodes, create a dedicated bridge and execute these commands separately
in any order. No node addresses or membership list are supplied:

```bash
pixi run docker network create crowdb-demo
pixi run docker run -d --name crowdb-demo-1 --network crowdb-demo --cpus 1 --memory 1536m \
  -e CROWDB_CLUSTER=demo -e CROWDB_INITIAL_VOTERS=3 \
  -e CROWDB_S3_PUBLIC_URI=http://127.0.0.1:7135 \
  -e CROWDB_ICEBERG_PUBLIC_URI=http://127.0.0.1:8135 \
  -e CROWDB_DATASET_PUBLIC_URI=http://127.0.0.1:9135 \
  -p 127.0.0.1:6135:6135 -p 127.0.0.1:7135:7135 \
  -p 127.0.0.1:8135:8135 -p 127.0.0.1:9135:9135 \
  --mount type=volume,source=crowdb-demo-1-data,target=/opt/crowdb/data crowdb-node:dev
pixi run docker run -d --name crowdb-demo-2 --network crowdb-demo --cpus 1 --memory 1536m \
  -e CROWDB_CLUSTER=demo -e CROWDB_INITIAL_VOTERS=3 \
  -e CROWDB_S3_PUBLIC_URI=http://127.0.0.1:7136 \
  -e CROWDB_ICEBERG_PUBLIC_URI=http://127.0.0.1:8136 \
  -e CROWDB_DATASET_PUBLIC_URI=http://127.0.0.1:9136 \
  -p 127.0.0.1:6136:6135 -p 127.0.0.1:7136:7135 \
  -p 127.0.0.1:8136:8135 -p 127.0.0.1:9136:9135 \
  --mount type=volume,source=crowdb-demo-2-data,target=/opt/crowdb/data crowdb-node:dev
pixi run docker run -d --name crowdb-demo-3 --network crowdb-demo --cpus 1 --memory 1536m \
  -e CROWDB_CLUSTER=demo -e CROWDB_INITIAL_VOTERS=3 \
  -e CROWDB_S3_PUBLIC_URI=http://127.0.0.1:7137 \
  -e CROWDB_ICEBERG_PUBLIC_URI=http://127.0.0.1:8137 \
  -e CROWDB_DATASET_PUBLIC_URI=http://127.0.0.1:9137 \
  -p 127.0.0.1:6137:6135 -p 127.0.0.1:7137:7135 \
  -p 127.0.0.1:8137:8135 -p 127.0.0.1:9137:9135 \
  --mount type=volume,source=crowdb-demo-3-data,target=/opt/crowdb/data crowdb-node:dev
```

Equivalent Compose examples are [compose.yaml](compose.yaml) and
[compose-three.yaml](compose-three.yaml):

```bash
pixi run docker compose -f container/oci-image/compose.yaml up -d
pixi run docker compose -f container/oci-image/compose.yaml down
pixi run docker compose -f container/oci-image/compose-three.yaml up -d
pixi run docker compose -f container/oci-image/compose-three.yaml down
```

`down` removes the Compose containers and network, preserving named volumes.
Set `CROWDB_IMAGE` to select the loaded image and `CROWDB_CLUSTER` to isolate
another project. Change the port variables shown in each file before starting
another project concurrently. Keep accepted public origins unchanged on reuse.
Plain Docker and Compose resources have their own ownership; Pixi clean does
not adopt them. Use Docker/Compose stop/remove/down for those resources and
remove each dedicated bridge after its containers are removed.

Read progress and credentials explicitly with standard Docker tools:

```bash
pixi run docker logs crowdb-one
pixi run docker exec crowdb-one crowdb-monitor credentials show --format env
```

The credential command prints S3, Iceberg and Dataset client values and their
public origins. Keep that output private. Setup completion is historical;
protocol availability requires current services and storage to be ready.
See [local storage and client details](../single-node-container/README.md).

## Linux host deployment

Prepare a persistent directory owned by UID/GID 10001 and a private password file
(mode 0600), or preinstall `ssh/authorized_keys` in that directory. Passwords are
only used for admission; each node persists its own Ed25519 keys. Production
requires an explicit stable physical host ID, management interface and resources.
CPU policy is explicit: `--cpus` sets a hard ceiling; `--cpu-shares 1024`
sets one CPU of relative weight and allows use of idle host CPU. These options
are mutually exclusive. Memory remains a verified hard limit in both modes.
Container E2E and CLI clusters default to the shares policy and one Tokio
worker per service. The host launcher accepts `--worker-threads 1` for this
explicit configuration, inherited by services as `TOKIO_WORKER_THREADS`.


```bash
pixi run --manifest-path container/oci-image/pixi.toml -e runtime run --runtime docker --cluster production --initial-voters 3 \
  --image docker.io/crowdb/crowdb-node@sha256:<verified-manifest-digest> \
  --name crowdb-node --network host --cpus 2 --memory-mib 1024 \
  --data-root /var/lib/crowdb --physical-host-id host-one --interface eno1 \
  --password-file /private/crowdb-password
```

Host networking uses port 6135 directly; no `-p` mapping is needed. Fixed listener
ports mean one node per host. Use `--seed http://host:26135` when multicast is unavailable.
Explicit `--device /dev/...` permits individual block devices; the host must grant
UID/GID 10001 access. The launcher checks device type and effective read/write
permissions, actual cgroup v2 CPU/memory limits and the host network namespace.
A failed check retains the owned container and its data for diagnosis and explicit
cleanup. A startup error does not authorize removing runtime or storage state.
Physical hardware I/O must also be validated for the selected deployment.

Use `--node-policy-file /absolute/path/node.toml` to pass the normal node's TOML
startup inputs, including explicit supplied-media setup and public protocol URIs.
The launcher rejects symlinked, oversized, unreadable or malformed files before
runtime creation. It mounts the file read-only; the monitor validates its complete
semantics and seals accepted policy before identity, admission or storage setup.
The file must be readable by container UID/GID 10001 and remain available for
recreation with the same accepted inputs.

For containerd, install the separate locked runtime environment:

```bash
pixi install --locked --manifest-path container/oci-image/pixi.toml -e runtime
pixi run --manifest-path container/oci-image/pixi.toml -e runtime run --runtime containerd --cluster production --initial-voters 3 \
  --address /run/crowdb-containerd/containerd.sock \
  --image docker.io/crowdb/crowdb-node@sha256:<verified-manifest-digest> \
  --name crowdb-node --network host --cpus 2 --memory-mib 1024 \
  --data-root /var/lib/crowdb --physical-host-id host-one --interface eno1 \
  --password-file /private/crowdb-password
```

The host administrator starts containerd separately. Initial support is rootful
Linux, cgroup v2 and host networking. The cluster name maps to the containerd
namespace `crowdb-<cluster>`; runtime namespace is not a separate cluster selector.
The default snapshotter is overlayfs; use `--snapshotter native` where overlayfs
is unavailable. No Docker daemon, runtime socket mount or CNI plugin is needed.
The tested tool versions are containerd 2.4.1, nerdctl 2.4.1 and runc 1.4.3.

## Persistent storage and candidate admission

Keep `/opt/crowdb/data` on a persistent host directory or named runtime volume.
The OCI image contains the programs; the container writable layer is not the
node's durable storage. Node identity, bootstrap receipts and storage journals
must survive container replacement along with the original media.

Use repeated `--disk-file /host/path/disk.img` for existing files and `--device
/dev/...` for raw devices. Both map into `/opt/crowdb/data/disks/<basename>` in
Docker and containerd. Basenames and canonical underlying resources must be
unique. A file and its loop alias cannot be supplied as independent disks.
Retain all recorded source mappings on replacement; changed inode/device,
missing source or insufficient UID/GID 10001 access fails without substitution.
Never mount a Docker or containerd socket into the node to manage its storage.
Standard startup prepares the common disk directory for UID/GID 10001. It does
not recursively change the ownership or mode of supplied files or devices.

- With no supplied inputs, Local node setup defaults to one 16-GiB sparse
  managed file and a 1-GiB host reserve. Production automatic storage uses
  preallocation and rejects sparse allocation. It is created in the retained
  data directory. Additional Create File Disk operations use generated paths in
  that same directory. Sparse allocation is an explicit advanced choice;
  failed preallocation remains a failure.
- Supplied inputs appear in discovery. Review them during node setup or use
  Node → Storage → Join Candidates after Group 0 is ready. Select an existing
  bound DiskGroup and media class; a raw device needs its own acknowledgement.
  Group 0 owns effective membership. Blank verification reads every usable byte
  and never wipes data; nonblank media fail. Supported raw lineage is a whole
  device or a whole-file loop mapping. Partitions, partitioned whole devices,
  stacked devices, ambiguous virtual lineage and loop slices are rejected.
- Physical size can exceed usable whole-zone geometry. The unused tail remains
  untouched. Backing properties distinguish host/resource identity, physical
  bytes, usable bytes and tail bytes. File allocated bytes and shared filesystem
  free space are separate observations, not a sum of virtual file capacities.
- Close/reopen does not cancel accepted per-disk work. Failed rows keep their
  original operation for explicit retry. Offline, then Remove, excludes the disk
  while retaining its bytes, ID, geometry and allocation metadata. Rejoin uses
  that retained identity Offline; explicit validated Online restores participation.
  Removal does not evacuate data or establish recovery/redundancy.
- The accepted deployment policy explicitly selects Local or Production and one
  or three initial voters. Local permits simulated placement with zero physical
  host failure tolerance. Production requires configured credentials and real
  placement; a single production voter cannot satisfy protected ChunkDB
  placement. Supplied or diskless storage is an explicit policy choice. Retained
  policy, identity and bootstrap inputs cannot be replaced by image defaults.

## Daemon and node lifecycle

Keep the runtime Pixi workspace/environment in an administrator-owned, versioned
location. Install its locked runtime environment there. Do not clean or update
an active daemon's environment. [service/containerd.service.in](service/containerd.service.in)
uses a concrete `@RUNTIME_PREFIX@` (the runtime environment directory); render it
into `/etc/systemd/system/crowdb-containerd.service`. Install
[service/containerd.toml](service/containerd.toml) as `/etc/crowdb-containerd.toml`.
The service has dedicated socket/content/state paths and disables CRI. Never
replace a Kubernetes-owned containerd configuration with this profile.

Install/start the unit explicitly with administrator permissions through Pixi.
For upgrades, install a new versioned environment, stop/repoint/restart the unit,
and verify probes and running node identities. The systemd service restarts the
daemon; containerd's restart plugin and `unless-stopped` policy recover exited
nodes. Monitor supervises the child services. Readiness failure alone never
restarts or reinitializes the whole node.

Use standard runtime tools to inspect, read logs, execute probes, stop and remove
nodes. For containerd select its address and namespace on `nerdctl`; for Docker
use its selected context. Removing a stopped container does not delete its
external data. Host networking uses one node per host with the fixed listener
ports; RDMA uses this host-network path.

## CI and acceptance

The manual release workflow constructs one OCI artifact without Docker installed,
then downloads it into independent Docker and Docker-free containerd jobs. Only
matching successful image digests allow publication. The protected publish job
copies the graph with digest preservation, verifies the remote digest, signs it,
and attaches SPDX SBOM and SLSA provenance before updating `latest`. The canonical repository is `crowdb/crowdb-node`; the
`crowdb/crowdb-iceberg`, `crowdb/crowdb-s3` and `crowdb/crowdb-dataset` repositories
are single-node usage aliases of that image. Image names do not select a different lifecycle or UI permission policy.
The Dataset alias uses the same image. Dataset access runs through the
`server access dataset serve` entry point with the versioned REST API and
Dataset-specific chunk storage. No second Docker image is published.
Preview has the same construction/runtime gates without publication authority.
Workflow execution and live registry signing require a release dispatch.

```bash
pixi run test-oci-image
pixi run --skip-deps test-single-node-container
pixi run test-container-e2e --profile bridge-admission --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 6144
pixi run sudo "$(command -v pixi)" run --as-is test-container-e2e --profile host --concurrency 1 --cpus 4 --memory-mib 4096
pixi run test-container-e2e --profile containerd-cluster --concurrency 1 --node-cpus 1 --cpus 4 --memory-mib 8192
# One disposable privileged Linux host, with native containerd and no Docker socket inside:
pixi run sudo "$(command -v pixi)" run --as-is test-container-e2e --profile containerd-host --concurrency 1 --cpus 3 --memory-mib 3072 \
  --archive "$PWD/target/crowdb.oci.tar"
```

Developer-only `tests/build-sandbox.py` and `tests/runtime-sandbox.py` use system
Docker to provide disposable privileged Linux hosts. Inside those hosts the
builder/runtime tests have no Docker CLI/socket. They verify the isolated process
contract, not a bare-metal installation. The native host gate exercises production metadata lifecycle separately from
Local full-protocol readiness and retained recreation. Production metadata alone
is not whole-cluster Ready. Enforced resources, host networking and retained
identity/key checks apply to both policies. Three isolated Linux namespace hosts additionally cover
discovery, SSH admission, Group-0 quorum and member recovery. This simulation is
not a physical multi-host network qualification. Existing Docker tests cover
application writes and crash recovery. Local registry transfer retained the exact
OCI digest; Syft generated an SPDX 2.3 SBOM from the tested archive. macOS VM execution, ARM64, Kubernetes orchestration and broader layer
E2E migration remain separate follow-up work.

Build tools: BuildKit 0.34.0, RootlessKit 3.2.0, runc 1.4.3, Skopeo 1.24.1.
Publishing tools: Syft 1.54.1 and Cosign 3.1.3. Conda-forge supplies runc/Skopeo;
checked-in Pixi source recipes supply missing tools from SHA256-pinned upstream
releases. All six upstream projects use Apache-2.0 licensing; recipes retain
upstream sources/checksums and license metadata. The dedicated Linux workspace
leaves the repository's macOS dependency solve unchanged.

Default listener ports are defined in [protocol ports.rs](../../lib/crowdb-protocol/src/port/ports.rs).
Generated packaging defaults are maintained with [tools/ports](../../tools/ports/README.md).
