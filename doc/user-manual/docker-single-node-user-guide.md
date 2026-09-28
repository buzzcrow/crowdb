<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Single-Node Container Guide

Run an Iceberg REST catalog and its storage on one Linux amd64 host.
For evaluation only; no production or upgrade guarantee.

## Quick start

```sh
docker run -d --name crowdb-iceberg \
  -p 127.0.0.1:80:80 \
  crowdb-iceberg-single-node:v0.1.0-dev
```

This example uses the local `v0.1.0-dev` image. If you received an image archive,
load it first with `docker load -i IMAGE.tar`. Docker Hub publication has not yet been verified; the manual release job is
retained for later validation. Docker creates an anonymous volume for the data.

Check startup, then retrieve your client credentials:

```sh
docker inspect --format '{{.State.Health.Status}}' crowdb-iceberg
docker exec crowdb-iceberg crowdb-monitor credentials show --format env
```

Wait for `healthy`. Connect your Iceberg client to `http://localhost` using
`ICEBERG_TOKEN` from the credential output. Port 80 serves both the REST catalog
and Iceberg FileIO; no separate S3 port is needed for Iceberg.

Stop and start the same container without losing its data:

```sh
docker stop --time 120 crowdb-iceberg
docker start crowdb-iceberg
```

The quick start exposes only Iceberg to the host. The GUI is not ready for use
and is not published. For data you want to reuse after deleting and recreating
the container, use a named volume as shown below.

## Common options

- `-d`: run in the background.
- `--name crowdb-iceberg`: give the container a convenient name for later commands.
- `-p 127.0.0.1:80:80`: publish Iceberg on host loopback. Fixed host ports require
  explicit mapping; the image cannot publish them automatically.
- `-v crowdb-data:/opt/crowdb/data`: keep data in a named volume. Docker creates
  it if needed; reuse it with the same image when recreating the container.
- `--restart unless-stopped`: automatically restart after failure or Docker
  daemon restart, unless you explicitly stopped the container.
- `--stop-timeout 120`: allow 120 seconds for shutdown before Docker sends SIGKILL.
- `--log-driver json-file --log-opt max-size=30m --log-opt max-file=5`:
  rotate Docker's captured logs separately from CROWDB's internal logs.

### Example: persistent data and automatic restart

Use this instead of the quick-start command:

```sh
docker run -d --name crowdb-iceberg \
  -p 127.0.0.1:80:80 \
  -v crowdb-data:/opt/crowdb/data \
  --restart unless-stopped --stop-timeout 120 \
  --log-driver json-file --log-opt max-size=30m --log-opt max-file=5 \
  crowdb-iceberg-single-node:v0.1.0-dev
```

Do not attach the same data volume to two running containers. A bind mount can
replace the named volume, but its directory must be writable by UID/GID 10001.

## Connect clients

The credential command prints `ICEBERG_URI` and `ICEBERG_TOKEN`, plus credentials
for the optional independent S3 service. Keep this output private. Credentials
remain the same when the data volume is reused.

### Iceberg with PyIceberg

Set `ICEBERG_URI` and `ICEBERG_TOKEN` in your client environment using the values
from the credential command. With PyIceberg installed:

```python
import os
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "crowdb",
    type="rest",
    uri=os.environ["ICEBERG_URI"],
    token=os.environ["ICEBERG_TOKEN"],
)
catalog.create_namespace_if_not_exists("demo")
print(catalog.list_namespaces())
```

### Optional independent S3 access

Add `-p 127.0.0.1:81:81` when creating the container if you also need the general
S3 API. Connect to `http://localhost:81` with path-style addressing, region
`us-east-1`, and the printed `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
Uploading an S3 object does not register an Iceberg table.

The examples provide local HTTP access. Changing host ports or using a remote
client also requires reachable Iceberg FileIO URLs; changing only the client's
catalog URI is insufficient. The container's ports are independent of bare-metal
service defaults.

## Troubleshooting

```sh
docker logs --tail 100 crowdb-iceberg
docker exec crowdb-iceberg crowdb-monitor liveness
docker exec crowdb-iceberg crowdb-monitor readiness
```

- `starting`: initialization or recovery is still running; inspect the logs.
- `unhealthy`: a required service is unavailable. Liveness checks the monitor;
  readiness checks the complete deployment. Successful probes exit zero.
- `exited`: inspect logs before restarting. Repeated child failures exhaust the
  restart budget. Docker's restart policy handles container exits; an unhealthy
  health check alone does not restart the container.
- Port already allocated: another process uses host port 80. Free the port or
  configure a different host mapping and reachable client/FileIO endpoints.
- Existing volume rejected: use the original image and credentials. Do not edit
  bootstrap manifests or secrets to bypass compatibility checks.

CROWDB logs are under `/opt/crowdb/data/log`. Monitor events are in
`monitor/monitor.log`; child logs have service-specific directories. Log rotation
uses a 30 MiB target and five retained files per channel across child restarts.
Docker's own log retention is configured separately, as in the extended example.

## Data, backup and limitations

- The data volume contains storage, metadata, credentials and logs. Anonymous
  volumes are not automatically reused by a newly created container; record the
  volume identity before deleting the original container.
- Stop the container before copying the entire volume. Preserve ownership,
  private permissions and sparse files. Restore with the exact image version;
  copying only disk images is insufficient.
- All services and four sparse 16 GiB disk images share one host. This provides
  no host fault tolerance. Monitor actual filesystem space; configured disk
  capacity is not a usable-capacity guarantee.
- Deleted Iceberg content can continue occupying space. Physical reclamation
  is disabled in this profile. Cross-version volume migration is not promised.
- PyIceberg namespace/table operations and boto3 object operations have container
  acceptance coverage. Spark, Flink, Trino, dataframe workflows and selected ORC
  data are not certified by those checks.

### Advanced crash diagnostics

Core dumps can contain credentials and user data. Linux host `kernel.core_pattern`
controls their destination; the image does not change it. A Docker core ulimit
alone does not guarantee a dump.

- File patterns use the process's filesystem namespace; relative paths use its
  working directory. The destination must be writable.
- Ubuntu Apport can reject container crashes when its container forwarding
  support is absent. This image does not include an Apport agent; do not assume
  a report will appear under the host's `/var/crash`.
- systemd-coredump uses the host journal and usually `/var/lib/systemd/coredump`;
  inspect with `coredumpctl` on the host.
- Docker Desktop uses its Linux VM's collector policy.

The image retains function symbols but currently has no DWARF source-line
information. Do not assume that the data volume contains usable core dumps.
See the [Linux core manual](https://www.man7.org/linux/man-pages/man5/core.5.html)
for host collection rules.
