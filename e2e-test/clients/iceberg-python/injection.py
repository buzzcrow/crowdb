# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Native injection followed by unmodified PyIceberg and Arrow reads."""

import hashlib
import os
from pathlib import Path
import secrets
import tempfile
import time
import uuid

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField
import requests


def upload(table, data):
    if os.environ.get("CROWDB_E2E_NATIVE_LOADER") == "1":
        from crowdb_tpc_loader.native_upload import new_upload_id, upload as native_upload

        uri = table.location() + f"/data/selected-{new_upload_id()}.parquet"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "selected.parquet"
            path.write_bytes(data)
            first = native_upload(table.io.properties, uri, path, len(data), 4096, lambda _: None)
            second = native_upload(table.io.properties, uri, path, len(data), 4096, lambda _: None)
        assert first[0] == second[0]
        return first[0]
    properties = table.io.properties
    random = secrets.randbits(74)
    identity = uuid.UUID(int=(int(time.time() * 1000) << 80) | (7 << 76)
                         | ((random >> 62) << 64) | (2 << 62) | (random & ((1 << 62) - 1)))
    uri = table.location() + f"/data/selected-{identity}.parquet"
    url = properties["s3.endpoint"].rstrip("/") + "/" + uri.removeprefix("s3://")
    url += f"?crowdb-native-upload={identity}"
    credentials = Credentials(properties["s3.access-key-id"], properties["s3.secret-access-key"],
                              properties["s3.session-token"])
    selected = None
    for _ in range(2):
        request = AWSRequest(method="PUT", url=url, data=data,
                             headers={"x-amz-content-sha256": hashlib.sha256(data).hexdigest()})
        S3SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
        response = requests.put(url, data=data, headers=dict(request.headers), timeout=60)
        response.raise_for_status()
        location = response.json()["location"]
        assert location.startswith(table.location() + "/f/v1/"), location
        assert selected is None or selected == location
        selected = location
    return selected


def verify(config):
    catalog = load_catalog("native", type="rest", uri=config["endpoint"], token="w" * 32)
    identifier = ("direct_moved", "selected")
    if config.get("verify_restart"):
        table = catalog.load_table(identifier)
    else:
        table = catalog.create_table(("default", "selected"), Schema(NestedField(1, "id", LongType())))
        sink = pa.BufferOutputStream()
        expected = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
        pq.write_table(expected, sink)
        location = upload(table, sink.getvalue().to_pybytes())
        table.add_files([location])
        original = table.scan().plan_files()[0].file.file_path
        assert original == location
        catalog.create_namespace("direct_moved")
        catalog.rename_table(("default", "selected"), identifier)
        catalog.drop_namespace("default")
        catalog.create_namespace("default")
        table = catalog.load_table(identifier)
        assert table.scan().plan_files()[0].file.file_path == original
    selected = table.scan().to_arrow()
    assert selected.column("id").to_pylist() == [1, 2, 3]
    if os.environ.get("CROWDB_E2E_NATIVE_LOADER") == "1":
        import duckdb

        with duckdb.connect() as connection:
            connection.register("selected", selected)
            assert connection.execute("SELECT count(*), sum(id) FROM selected").fetchone() == (3, 6)
    assert "/f/v1/" in table.scan().plan_files()[0].file.file_path
    # PyIceberg's Arrow FileIO opens the returned URL and performs Parquet footer
    # range reads. No additional location field is supplied to either SDK.
    print("Native injection: retry, manifest registration, Arrow query, move and retained URL passed")
