<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Local node storage and client usage

One node is a topology case of the normal editable cluster lifecycle. For image
builds, named Docker shortcuts, independent Docker commands and Compose, use the
[shared OCI guide](../oci-image/README.md). The old manual/single startup mode
selector is removed; use the accepted deployment mode and initial voter policy.

## Node-first local storage policy

The node-first local policy accepts one or three initial voters. Each member
prepares one sparse 16-GiB file with a 16-GiB zone, 1-MiB allocation units and
a 1-GiB free-space reserve. These are test media; sparse capacity does not
reserve physical host space. Production does not fall back to this policy.

- One-node writes use one mirror copy; three-node writes use two copies.
- Tree, stream and Access payload allocation uses 1-MiB mirror strips. Tree
  chunk capacity remains independent of strip capacity.
- Mirror-to-EC conversion is disabled, including Dataset payload writes.
- Local placement declares zero physical-node failure tolerance. Colocated
  containers retain the same physical-host identity. The explicit local
  placement allowance can put two copies on separate extents of the same
  disk; copy count does not promise disk, node or rack protection. Inspect
  each strip's placement assessment for its actual protection.
- Accepted media and operation identities survive restart. Failed preparation
  is retained for diagnosis; restarting is not permission to replace its data.

The node-first local client origins default to `http://127.0.0.1` on ports
7135 (S3), 8135 (Iceberg) and 9135 (Dataset). When publishing different host
ports or addresses, supply `CROWDB_S3_PUBLIC_URI`, `CROWDB_ICEBERG_PUBLIC_URI`
and `CROWDB_DATASET_PUBLIC_URI` to each node. These HTTP origins are sealed in
the node policy before startup; omitted restart values retain the accepted
origins. Internal discovery, service listeners and Console requests continue
to use node addresses. The node does not inspect the Docker socket for NAT.

Read client credentials explicitly with:

```sh
pixi run docker exec NODE crowdb-monitor credentials show --format env
```

This exports S3 credentials, the Iceberg writer token and the Dataset bearer
with their public origins. The private client file is
`/opt/crowdb/data/console/secrets/client.env`; it has mode `0600`. Server master
keys stay in the protected server files. Do not put client credential output
into logs or cluster status.

## Client listeners and readiness

The same Access service exposes S3 at 7135, Iceberg at 8135 and Dataset at 9135.
The Web UI listens at 6135. Publish these ports when using a bridge. A listener
or an old completed setup record is not proof of current storage availability.

Use the client origins returned by the credential command. Iceberg metadata and
Parquet URLs and Dataset payload references must be reachable from the client;
changing Docker port mappings without changing the accepted public origins
before first startup produces unreachable references. Internal service addresses
remain independent of public origins.

All members of one local cluster share the accepted client credential set.
Retain each member's own data volume across stop, clean and image replacement.
Default `oci-clean` removes the containers and their unused owned bridge;
`--delete-data` additionally deletes the selected helper's owned volumes.
Ordinary `docker compose down` also preserves its named volumes.

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
  The canonical `pixi run test-e2e-s3 --case s3.protocol::test_aws_cli_workflow` case verifies ten
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
run `aws --endpoint-url http://127.0.0.1:7135 ...`. For server copy, supply
`--metadata-directive COPY` to `aws s3 cp`. The repeatable repository gate is
`pixi run test-e2e-s3 --case s3.protocol::test_aws_cli_workflow`; container acceptance runs the same recipe
and checks persisted bytes after recovery before publication credentials become
available.

- The configured rclone recipe covers discovery, ordinary/multipart transfer,
  prefix listing, server copy, sync/delete and exact downloads. It uses the
  Other provider, path-style addressing, ListObjectsV2, no system metadata,
  one transfer/checker/upload worker, an 8 MiB multipart cutoff and 5 MiB parts.
  User metadata preserves file modification times; checksum metadata remains
  enabled. Run `pixi run test-e2e-s3 --case s3.protocol::test_rclone_workflow`. Container acceptance runs
  this recipe alongside AWS CLI and verifies both after recovery/restart.
- Mounting the endpoint as a filesystem with s3fs-fuse is unsupported.
- Optional [Java, JavaScript and Go SDK recipes](../../e2e-test/clients/S3-SDK.md)
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

## Iceberg and Dataset clients

The [Iceberg ecosystem guide](../../e2e-test/clients/ecosystem/README.md) describes
locked PyIceberg and external ecosystem clients. PyIceberg tests write real
metadata/Parquet objects and read them through each node's configured public
origin. Dataset has its own bearer token and REST namespace; use its returned
payload references rather than constructing storage URLs.

The OCI image carries the same server binaries for all protocols. It neither
mounts the Docker socket nor delegates bootstrap or storage preparation to the
host CLI. Monitor-owned accepted operations continue after the launching shell
or browser exits. Changing an image does not reset node identity or storage.

Default listener ports are defined in [protocol ports.rs](../../lib/crowdb-protocol/src/port/ports.rs).
Generated packaging defaults are maintained with [tools/ports](../../tools/ports/README.md).
