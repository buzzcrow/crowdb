<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Shared OCI image

Build one Linux amd64 image for Docker and containerd. Ubuntu 24.04 is pinned
by digest; the common recipe packages the existing release binaries, UI and SSH.
The build requires Pixi and Linux kernel privileges, with no Docker installation,
socket or containerd service. Running the image is a separate operation.

## Build and import

```bash
pixi run image-build --output target/crowdb.oci.tar
pixi run image-inspect target/crowdb.oci.tar --receipt
# Optional import into the system Docker image store:
pixi run image-import target/crowdb.oci.tar --image crowdb-node:dev
```

`image-build` compiles/stages with the repository environment, then selects the
locked standalone builder environment. `--staged` reuses an existing
`target/container-runtime`. Unsupported platforms fail before compilation.
Only Linux amd64 is enabled; macOS will compile inside a Linux VM backend.

Rootless builds require host-installed setuid `newuidmap`/`newgidmap`, subordinate
UID/GID ranges for the invoking user, and permitted unprivileged user namespaces.
Pixi supplies RootlessKit, BuildKit and runc; it cannot grant kernel privileges.
The builder uses the native snapshotter and host network, so no FUSE or CNI is
required. Startup errors include complete daemon diagnostics. Rootless preflight
is implemented; current local acceptance exercised the explicit rootful mode.

If rootless prerequisites are unavailable, an administrator can explicitly run:

```bash
pixi run stage-single-node-container
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

## Startup policies

The image defaults to `CROWDB_STARTUP_MODE=manual`: discovery, SSH and UI start;
operators admit candidates, configure storage and initialize Group 0. A single
local candidate can form an editable cluster and later admit more nodes.

Explicit `CROWDB_STARTUP_MODE=single` creates virtual disks and initializes the
existing development profile automatically. UI editing and backend mutations
are disabled. Both policies use the same image. Existing single-node development
helpers select `single` explicitly. Persisted bootstrap/admission records reject
incompatible policy changes; do not change modes on a populated data directory.

For a local automatic Docker container:

```bash
pixi run docker run -d --name crowdb-single --cpus 2 --memory 1g \
  -e CROWDB_STARTUP_MODE=single -p 9090:9090 -p 9091:9091 -p 9092:9092 \
  --mount type=volume,source=crowdb-data,target=/opt/crowdb/data \
  crowdb-node:dev
pixi run docker exec crowdb-single crowdb-monitor readiness
```

## Linux host deployment

Prepare a persistent directory owned by UID/GID 10001 and a private password file
(mode 0600), or preinstall `ssh/authorized_keys` in that directory. Passwords are
only used for admission; each node persists its own Ed25519 keys. Production
requires an explicit stable physical host ID, management interface and resources.

```bash
pixi run image-run --runtime docker \
  --image docker.io/crowdb/crowdb-node@sha256:<verified-manifest-digest> \
  --name crowdb-node --network host --cpus 2 --memory-mib 1024 \
  --data-root /var/lib/crowdb --physical-host-id host-one --interface eno1 \
  --password-file /private/crowdb-password
```

Host networking uses port 9090 directly; no `-p` mapping is needed. Fixed listener
ports mean one node per host. Use `--seed host:9095` when multicast is unavailable.
Explicit `--device /dev/...` permits individual block devices; the host must grant
UID/GID 10001 access. The launcher checks device type and effective read/write
permissions, actual cgroup v2 CPU/memory limits and the host network namespace.
A failed check removes only the container created by that launch. Data remains.
Physical hardware I/O must also be validated for the selected deployment.

For containerd, install the separate locked runtime environment:

```bash
pixi install --locked --manifest-path container/oci-image/pixi.toml -e runtime
pixi run image-run --runtime containerd \
  --address /run/crowdb-containerd/containerd.sock \
  --image docker.io/crowdb/crowdb-node@sha256:<verified-manifest-digest> \
  --name crowdb-node --network host --cpus 2 --memory-mib 1024 \
  --data-root /var/lib/crowdb --physical-host-id host-one --interface eno1 \
  --password-file /private/crowdb-password
```

The host administrator starts containerd separately. Initial support is rootful
Linux, cgroup v2, namespace `crowdb`, and host networking. `k8s.io` is rejected.
The default snapshotter is overlayfs; use `--snapshotter native` where overlayfs
is unavailable. No Docker daemon, runtime socket mount or CNI plugin is needed.
The tested tool versions are containerd 2.4.1, nerdctl 2.4.1 and runc 1.4.3.

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

```bash
pixi run image-ctl --runtime containerd --address /run/crowdb-containerd/containerd.sock logs crowdb-node
pixi run image-ctl --runtime containerd --address /run/crowdb-containerd/containerd.sock readiness crowdb-node
pixi run image-ctl --runtime containerd --address /run/crowdb-containerd/containerd.sock stop crowdb-node
pixi run image-ctl --runtime containerd --address /run/crowdb-containerd/containerd.sock remove crowdb-node
```

The lifecycle helper accepts launcher-owned containers only; `remove` requires a
stopped node and does not remove data or images. Recreate with `image-run` and the
same root/host identity to retain UUID, SSH keys and cluster binding. `restart`
and `liveness` are also available. Use `--runtime docker` for the same operations.

## CI and acceptance

The manual release workflow constructs one OCI artifact without Docker installed,
then downloads it into independent Docker and Docker-free containerd jobs. Only
matching successful image digests allow publication. The protected publish job
copies the graph with digest preservation, verifies the remote digest, signs it,
and attaches SPDX SBOM and SLSA provenance before updating `latest`. The canonical repository is `crowdb/crowdb-node`; the
`crowdb/crowdb-iceberg`, `crowdb/crowdb-s3` and `crowdb/crowdb-dataset` repositories
are single-node usage aliases of that image. Their examples explicitly set
`CROWDB_STARTUP_MODE=single`; image names alone do not select startup mode.
The Dataset alias reserves its usage name; Dataset access is not yet available. No second Docker image is published.
Preview has the same construction/runtime gates without publication authority.
Workflow execution and live registry signing require a release dispatch.

```bash
pixi run test-oci-image
pixi run --skip-deps test-single-node-container
pixi run python container/single-node-container/tests/node-containers.py
pixi run python container/single-node-container/tests/host-node.py
pixi run python container/oci-image/tests/containerd-cluster.py
# Administrator-started isolated Linux host, Docker absent:
pixi run --manifest-path container/oci-image/pixi.toml -e runtime \
  python container/oci-image/tests/containerd-host.py "$PWD/target/crowdb.oci.tar"
```

Developer-only `tests/build-sandbox.py` and `tests/runtime-sandbox.py` use system
Docker to provide disposable privileged Linux hosts. Inside those hosts the
builder/runtime tests have no Docker CLI/socket. They verify the isolated process
contract, not a bare-metal installation. The runtime gate exercises both modes,
Group-0 admission/bootstrap, enforced resources, probes, SIGTERM and persisted
identity/key recovery. Three isolated Linux namespace hosts additionally cover
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
