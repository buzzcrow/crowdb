<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Docker Hub repository drafts

Editing notes (do not paste this section into Docker Hub):

- Copy each description into **Description**, select the suggested categories,
  and copy the content under **Repository overview** into the overview editor.
- These are English drafts for two different user audiences.
- The S3 image publication is being added to the release workflow. Publish its
  tags before posting the S3 quick start as available to users.
- Examples follow the current container configuration and retained client tests.
  They have not been rerun against the public registry images for this draft.
- Category names follow [Docker Hub repository information](https://docs.docker.com/docker-hub/repos/manage/information/).

## crowdb/crowdb-s3

### Description

S3-compatible object storage with native CROWDB storage, in one container for development and testing.

### Category

- Databases & storage
- Developer tools

### Repository overview

# CROWDB S3

Run S3-compatible object storage locally with one Docker command. Use boto3 to
create buckets, upload objects, and read them back without setting up a
separate storage cluster.

CROWDB is an open-source distributed storage platform. This image packages its
storage services and S3 endpoint into a single-node development environment.

## Supported operations

- Bucket creation, listing, existence checks, and deletion.
- Object upload, overwrite, download, metadata retrieval, and deletion.
- Prefix and delimiter listing with ListObjectsV2 and continuation tokens.
- Range and conditional reads.
- Multipart upload, part replacement, part listing, completion, and abort.
- AWS Signature Version 4 authentication and presigned request verification.
- Content-MD5 validation and S3-style ETags.

The repository includes boto3 end-to-end tests for core operations, multipart
uploads, interrupted requests, and recovery after service restart.

## Quick start

Requires Docker on Linux amd64, or a Docker environment capable of running
Linux amd64 containers.

```sh
docker run -d --name crowdb-s3 \
  --mount type=volume,source=crowdb-s3-data,target=/opt/crowdb/data \
  -p 127.0.0.1:9091:9091 \
  crowdb/crowdb-s3:latest
```

Check startup and wait until the health status is `healthy`:

```sh
docker inspect --format '{{.State.Health.Status}}' crowdb-s3
docker logs crowdb-s3
```

Display the generated client credentials:

```sh
docker exec crowdb-s3 crowdb-monitor credentials show --format env
```

Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` in
your client environment using the displayed values. Keep credentials private.
The S3 endpoint is `http://127.0.0.1:9091` and uses path-style addressing.

## Upload and download with boto3

Install the client in your Python environment:

```sh
python -m pip install boto3
```

Run after setting the AWS environment variables above:

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9091",
    config=Config(
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    ),
)

s3.create_bucket(Bucket="example")
s3.put_object(Bucket="example", Key="hello.txt", Body=b"Hello from CROWDB!")
print(s3.get_object(Bucket="example", Key="hello.txt")["Body"].read())
print(s3.list_objects_v2(Bucket="example")["Contents"])
```

The checksum configuration matches the retained container boto3 example.
Compatibility with every SDK default or S3 tool is not implied.

## Data and tags

- Mount `/opt/crowdb/data` to retain data and generated credentials across
  container recreation. Reuse the same volume when recreating the container.
- `latest` is the moving release tag; select a version tag for an explicit
  release. Version tags can be replaced when that release is republished;
  pin an image digest for exact image identity.
- An optional console is available by also mapping container port `9090`.

## Development preview scope

This image runs a single-node profile with one voting replica and one copy of
stored data. It provides no redundancy. Use disposable data for development,
testing, and public evaluation. Production use and on-disk upgrade compatibility
are not supported.

The supported S3 API is a subset of Amazon S3. Server-side copy, batch deletion,
versioning, lifecycle policies, bucket policies/ACLs, server-side encryption,
object tags, replication, and event notifications are outside the current
advertised surface. See the project documentation for updates.

## Related image

[crowdb/crowdb-iceberg](https://hub.docker.com/r/crowdb/crowdb-iceberg) provides
an Iceberg-focused quick start. Both repositories contain the same runtime:
one Access Server listens on S3 port `9091` and Iceberg port `9092`. Map both ports
if you need both interfaces in one container. Their namespaces and authorization
remain separate; general S3 buckets do not expose Iceberg table files.

## Links and license

- [Project website](https://crowdb.dev/)
- [Source and issues](https://github.com/buzzcrow/crowdb)
- [Container documentation](https://github.com/buzzcrow/crowdb/blob/main/container/single-node-container/README.md)
- [License: Apache-2.0](https://github.com/buzzcrow/crowdb/blob/main/LICENSE)

## crowdb/crowdb-iceberg

### Description

Apache Iceberg REST catalog with native CROWDB storage, in one container for development and testing.

### Category

- Databases & storage
- Data science
- Developer tools

### Repository overview

# CROWDB Iceberg

Create Apache Iceberg tables, append data, and read it back with PyIceberg—all
from a single container providing a REST catalog and native table-file storage.
No separate object store is needed for this local workflow.

CROWDB is an open-source distributed storage platform. Iceberg catalog, table,
snapshot, and immutable file semantics are implemented directly over CROWDB's
storage core.

## What is included

- An Apache Iceberg REST catalog endpoint.
- Namespace and table operations, snapshot metadata, and atomic table commits.
- Native immutable table-file storage and delegated FileIO credentials.
- A single-node storage environment with automatic initialization and generated
  client credentials.
- A web console, available on container port `9090`.

The container acceptance suite exercises PyIceberg table creation, append,
Arrow/pandas reads, and retrieval after service recovery and container restart.

## Quick start

Requires Docker on Linux amd64, or a Docker environment capable of running
Linux amd64 containers. The example uses host port `9092`, matching the tested
Iceberg catalog and file endpoint setup.

```sh
docker run -d --name crowdb-iceberg \
  --mount type=volume,source=crowdb-iceberg-data,target=/opt/crowdb/data \
  -p 127.0.0.1:9092:9092 \
  crowdb/crowdb-iceberg:latest
```

Check startup and wait until the health status is `healthy`:

```sh
docker inspect --format '{{.State.Health.Status}}' crowdb-iceberg
docker logs crowdb-iceberg
```

Display the generated client credentials:

```sh
docker exec crowdb-iceberg crowdb-monitor credentials show --format env
```

Set `ICEBERG_TOKEN` in your client environment using the displayed value. Keep
the token private. The REST catalog URI is `http://127.0.0.1:9092`.

## Create and read a table with PyIceberg

Install the clients in your Python environment:

```sh
python -m pip install 'pyiceberg[pyarrow]' pandas
```

Run after setting `ICEBERG_TOKEN`:

```python
import os

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "crowdb",
    type="rest",
    uri="http://127.0.0.1:9092",
    token=os.environ["ICEBERG_TOKEN"],
)

catalog.create_namespace("demo")
rows = pa.Table.from_pandas(
    pd.DataFrame({"order_id": [1, 2], "amount": [120, 80]}),
    preserve_index=False,
)
table = catalog.create_table("demo.orders", schema=rows.schema)
table.append(rows)
print(catalog.load_table("demo.orders").scan().to_pandas())
```

Use a fresh namespace/table for subsequent runs, or load the existing table
instead of creating it again. The catalog provides the table's FileIO
configuration; this example needs no separately configured S3 service.

## Data and tags

- Mount `/opt/crowdb/data` to retain tables and generated credentials across
  container recreation. Reuse the same volume when recreating the container.
- `latest` is the moving release tag; select a version tag for an explicit
  release. Version tags can be replaced when that release is republished;
  pin an image digest for exact image identity.
- To open the console, also publish `-p 127.0.0.1:9090:9090` and visit
  `http://127.0.0.1:9090`.

## Development preview scope

This image runs a single-node profile with one voting replica and one copy of
stored data. It provides no redundancy. Use disposable data for development,
testing, and public evaluation. Production use and on-disk upgrade compatibility
are not supported. Dataset access and direct GPU delivery are not available in
this image.

The example covers a local PyIceberg workflow. Compatibility with every Iceberg
engine, file format, or deployment configuration is not implied. Start with the
published quick start and tested client recipes.

## Related image

[crowdb/crowdb-s3](https://hub.docker.com/r/crowdb/crowdb-s3) provides an
object-storage-focused quick start. Both repositories contain the same runtime:
one Access Server listens on Iceberg port `9092` and general S3 port `9091`.
Map both ports if you need both interfaces in one container. Iceberg table
storage and general S3 buckets have separate namespaces and authorization.

## Links and license

- [Project website](https://crowdb.dev/)
- [Iceberg quick start](https://crowdb.dev/docs/quickstart/)
- [Source and issues](https://github.com/buzzcrow/crowdb)
- [Container documentation](https://github.com/buzzcrow/crowdb/blob/main/container/single-node-container/README.md)
- [License: Apache-2.0](https://github.com/buzzcrow/crowdb/blob/main/LICENSE)
