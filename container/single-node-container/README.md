<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Single-node container development

This page describes building and running CROWDB from source on a Linux amd64
development or CI host.

This profile explicitly selects `test_single_node`. KV groups have one voter;
chunk writes use one 1 MiB mirror copy with no EC or conversion. The profile
provides no data protection, and a failed copy returns an I/O error. Production
requires at least three voting nodes and protected placement.

The profile keeps chunk capacity separate from strip size. Tree chunks use
`storage.tree_chunk_capacity_bytes` in `chunk-kv.toml`; a 16 MiB tree chunk
contains multiple 1 MiB mirror strips. Stream chunks use
`storage.stream_chunk_capacity_bytes`. S3 and Iceberg each accept their own
`s3.small_write.chunk_capacity_bytes` or
`iceberg.small_write.chunk_capacity_bytes` and large `max_chunk_size` values
in `access.toml`. The shared `small_write` section remains a fallback. The
profile also sets RPC worker and client connection counts explicitly so local
resource use can be tuned without changing production defaults.

```sh
pixi run build-single-node-container
pixi run test-single-node-container
```

- Compilation runs on the host with the locked repository dependencies. Cargo
  and CMake reuse existing build outputs; npm uses its local download cache.
- The build stages release programs, their required shared libraries, UI and
  deployment files under `target/container-runtime`. Existing runtime data and
  credentials are never part of the Docker context.
- Docker only packages these files into the pinned Ubuntu runtime image. It
  does not install Pixi or compilers, compile source, or use a custom base image.
- Packaging checks dynamic linkage inside Ubuntu and verifies the staged
  revision/version against image metadata. A different host ABI must pass these
  checks and the container tests before its artifacts can be used.
- The default local image is `crowdb-iceberg-single-node:dev`. Set
  `CROWDB_CONTAINER_IMAGE` to build and test a separate candidate tag.

`pixi run stage-single-node-container` produces the runtime directory without
building a Docker image. Work on a `release/<version>` branch whose `VERSION`
matches the branch name, such as `release/0.2.1`. After pushing each candidate
commit, select that branch in the GitHub Actions manual run form, or dispatch
it from a clean checkout that matches the remote branch:

```sh
pixi run -- python tools/release.py --dry-run
pixi run -- python tools/release.py --execute
```

After the image passes container verification, publication updates four tags
to the same image digest:

- `crowdb/crowdb-iceberg:<version>` and `crowdb/crowdb-iceberg:latest`.
- `crowdb/crowdb-s3:<version>` and `crowdb/crowdb-s3:latest`.

The repositories provide separate entry points for Iceberg and S3 users. Both
contain the same runtime, including one Access Server listening on Iceberg
port 9092 and S3 port 9091. Publish the ports needed by the client; using both
interfaces requires only one container. The container verification runs boto3,
AWS CLI and PyIceberg clients against that runtime, including reads after recovery
and persisted-volume restart.

Rerunning publication from an older release branch also updates both `latest`
tags, so use the newest release branch for moving tags.

The script only dispatches the workflow; it does not change files or push.
The dry run does not contact GitHub.

The workflow builds and tests the container, then waits for DockerHub environment approval.
Before publishing, it checks that the remote branch still points to the same
commit. A newer branch commit requires a new run. Each successful run replaces
both repositories' version and `latest` tags and signs the digest in each
repository. It does not
create a Git tag or GitHub Release. Fix a failed candidate on the release
branch and run the workflow again.

The workflow archives the verified runtime, then packages those same files in
its publish job without recompiling them.

## Public ports

The default host mappings match the container listener ports:

- Console: `9090:9090`, optional.
- S3: `9091:9091`.
- Iceberg catalog and native FileIO: `9092:9092`.

Port `9093` is reserved for future Dataset access; the current image does not
listen on it.

Publish only the interfaces needed by the client. The generated default client
addresses use `localhost` with these ports. An Iceberg deployment with a different
host name or mapped port must also configure its advertised public URI; changing
the catalog connection URI alone does not change delegated FileIO endpoints.
The Access Server runs as the existing unprivileged user without a low-port
binding capability.

## S3 container usage

Start the published S3 image with a named data volume and map host port 9091
to the container's S3 port 9091:

```sh
docker run -d --name crowdb-s3 \
  --mount type=volume,source=crowdb-s3-data,target=/opt/crowdb/data \
  -p 127.0.0.1:9091:9091 \
  crowdb/crowdb-s3:latest
docker inspect --format '{{.State.Health.Status}}' crowdb-s3
```

Wait for `healthy`, then obtain the generated client credentials:

```sh
docker exec crowdb-s3 crowdb-monitor credentials show --format env
```

Configure boto3 with endpoint `http://127.0.0.1:9091`, the displayed AWS
credentials and region, and path-style addressing:

```python
import boto3
from botocore.config import Config

client = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9091",
    config=Config(s3={"addressing_style": "path"}),
)
client.create_bucket(Bucket="example")
client.put_object(Bucket="example", Key="hello.txt", Body=b"hello")
print(client.get_object(Bucket="example", Key="hello.txt")["Body"].read())
```

Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION`
in the client's environment using the displayed values before running this
example. To use Iceberg from the same container, also map container port 9092.
These published images use the single-node development profile described above.

## Tested S3 client recipes

- The locked `s3-e2e` environment pins boto3/botocore 1.43.92, AWS CLI 2.36.47,
  and rclone 1.75.1 (the packaged binary identifies its build as `1.75.1-DEV`). Run repository acceptance commands through Pixi.
- boto3 uses its default checksum calculation and validation settings. Only
  path-style addressing, SigV4 and the endpoint are configured. Ordinary and multipart
  uploads verify request CRC32 checksums and exact downloaded bytes. Presigned
  URLs and AWS chunked trailers have separate integrity tests. Supported request
  checksums do not imply arbitrary response checksum negotiation.
- AWS CLI uses the configured recipe below for discovery, ordinary and multipart
  transfers, prefix listing, server copy, sync with deletion, and recursive
  cleanup. One concurrent request matches the bounded development fixture.
  The separate `pixi run -e s3-e2e test-aws-cli-concurrent` gate verifies ten
  concurrent requests with the same classic transfer and path-style setup on
  the real-storage fixture, including exact bytes and cleanup. The container
  recipe retains one worker. Copy explicitly requests COPY metadata; the CLI's broader
  property/annotation discovery is unsupported.

```ini
[default]
region = us-east-1
s3 =
    addressing_style = path
    preferred_transfer_client = classic
    max_concurrent_requests = 1
    multipart_threshold = 8MB
    multipart_chunksize = 5MB
```

Use that profile in `AWS_CONFIG_FILE`, export the generated credentials, then
run `aws --endpoint-url http://127.0.0.1:9091 ...`. For server copy, supply
`--metadata-directive COPY` to `aws s3 cp`. The repeatable repository gate is
`pixi run -e s3-e2e test-aws-cli-e2e`; container acceptance runs the same recipe
and checks persisted bytes after recovery before publication credentials become
available.

- The configured rclone recipe covers discovery, ordinary/multipart transfer,
  prefix listing, server copy, sync/delete and exact downloads. It uses the
  Other provider, path-style addressing, ListObjectsV2, no system metadata,
  one transfer/checker/upload worker, an 8 MiB multipart cutoff and 5 MiB parts.
  User metadata preserves file modification times; checksum metadata remains
  enabled. Run `pixi run -e s3-e2e test-rclone-e2e`. Container acceptance runs
  this recipe alongside AWS CLI and verifies both after recovery/restart.
- Mounting the endpoint as a filesystem with s3fs-fuse is unsupported.
- Optional [Java, JavaScript and Go SDK recipes](../../app/crowdb-access-server/tests/common/s3_sdks/README.md)
  run independently through manual Pixi tasks and workflow dispatch. Their
  isolated language toolchains and checks are outside container/release gates.
- User metadata supports lowercase keys and printable ASCII values, with a
  combined key/value size up to 2 KiB. PUT, multipart initiation and COPY/REPLACE
  persist the attributes; HEAD and GET return them. Duplicate names and invalid
  values are rejected. Older multipart session schemas are unsupported;
  restart recovery applies to the current format, with no migration decoder.
- The endpoint has one configured namespace shared by accepted credentials;
  per-user ACLs, versioning and annotations are unsupported.
  ListBuckets CreationDate is a stable Unix-epoch placeholder. ListObjectsV2
  supports `encoding-type=url` for keys requiring XML-safe encoding.

## Crash collection boundary

For host configuration, restoring its collector, and GDB commands for both
container and bare-metal cores, see the
[crash debugging guide](../../doc/dev/crash_debugging.md).

The image does not configure the host's Linux core collector. Inspect
`/proc/sys/kernel/core_pattern` on the Docker host before expecting a dump in
the mounted data volume. A leading `|` sends a crash to a host-side collector;
relative `core` or `core.*` patterns write in the crashing process's working
directory. The container runs the monitor and managed children from the private
`/opt/crowdb/data/crash` directory. On monitor startup and after a child is
reaped, it removes older regular `core` files and keeps the newest one.
The directory has mode `0700`; symlinks named `core.*` are not followed or
deleted. This is one-core retention, not a promise that the host creates a
volume file. Other relative filename patterns are outside this retention rule.

For a host with a relative `core` pattern, add a size bound to `docker run`:

```sh
--ulimit core=1073741824:1073741824
```

The example bounds each core to 1 GiB. A small bound may truncate a dump and
make some stack frames unavailable. Core collection can also be suppressed by
the host's dumpability policy, including for executables with file capabilities.
The container never changes `core_pattern` or the host's dumpability policy.

- On a systemd-coredump host, use `coredumpctl list crowdb-kv-server` to find
  the host report, then `coredumpctl --output=/private/core dump
  crowdb-kv-server` as an authorized host user to export it. Check that the
  result is readable and nonempty before symbolization.
- On an Ubuntu Apport host, find the matching report in `/var/crash` on the
  Docker host. Create a private directory, then run `sudo apport-unpack
  /var/crash/REPORT.crash /private/crowdb-core/unpacked`. The extracted
  `CoreDump` is the file to pass to the symbolizer. Apport reports may be
  readable only by the host administrator; preserve the private permissions
  when granting the debugging user access. A pipe pattern does not create a
  volume file. Apport may fail to resolve a CROWDB executable because its
  `/opt/crowdb/bin` path exists only inside the container; it may also ignore
  executables outside host distribution packages. Check the host's Apport log
  when no report appears.
  If the report or `CoreDump` is absent, collection is unavailable for that
  crash; do not substitute a log or an unrelated dump.
- On Docker Desktop, inspect the Linux VM's collector. The desktop host's
  native crash directory is not the container's core directory.

Core dumps can contain credentials and user data. Keep exports in a private
directory and do not attach them to ordinary logs or issues. The release image
contains stripped binaries, so a core may give only a limited stack trace.
No host collector change is required by the image build.

Collector behavior follows the [Linux core pattern documentation](https://docs.kernel.org/admin-guide/sysctl/kernel.html),
[systemd-coredump manual](https://www.freedesktop.org/software/systemd/man/250/systemd-coredump.socket.html),
and [Ubuntu Apport documentation](https://ubuntu.com/project/docs/contributors/debugging/apport/).
