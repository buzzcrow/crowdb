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


def file_request(properties, method, location, data=b"", query=""):
    url = properties["s3.endpoint"].rstrip("/") + "/" + location.removeprefix("s3://")
    credentials = Credentials(properties["s3.access-key-id"], properties["s3.secret-access-key"],
                              properties["s3.session-token"])
    request = AWSRequest(method=method, url=url + query, data=data,
                         headers={"x-amz-content-sha256": hashlib.sha256(data).hexdigest()})
    S3SigV4Auth(credentials, "s3", "us-east-1").add_auth(request)
    response = requests.request(method, url + query, data=data, headers=dict(request.headers), timeout=60)
    response.raise_for_status()
    return response


def verify_file(table, location, stem, data):
    prefix = table.location() + f"/f/v1/{stem}."
    assert location.startswith(prefix) and location.endswith(".parquet"), location
    locator = location[len(prefix):-len(".parquet")]
    assert locator and locator.isascii(), location
    assert all(character.isalnum() or character in "_-" for character in locator), location
    digest = '"' + hashlib.sha256(data).hexdigest() + '"'
    head = file_request(table.io.properties, "HEAD", location)
    assert head.status_code == 200, head.status_code
    assert int(head.headers["Content-Length"]) == len(data), head.headers
    assert head.headers["ETag"] == digest, head.headers
    response = file_request(table.io.properties, "GET", location)
    assert response.status_code == 200, response.status_code
    assert response.headers["ETag"] == digest, response.headers
    assert int(response.headers["Content-Length"]) == len(data), response.headers
    assert response.content == data, location
    actual = pq.read_table(pa.BufferReader(response.content))
    expected = pq.read_table(pa.BufferReader(data))
    assert actual.equals(expected), (actual, expected)


def upload(table, data):
    locations = []
    if os.environ.get("CROWDB_E2E_NATIVE_LOADER") == "1":
        from crowdb_tpc_loader.native_upload import new_upload_id, upload as native_upload

        stem = f"selected-{new_upload_id()}"
        uri = table.location() + f"/data/{stem}.parquet"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "selected.parquet"
            path.write_bytes(data)
            for _ in range(2):
                result = native_upload(table.io.properties, uri, path, len(data), 4096, lambda _: None)
                verify_file(table, result[0], stem, data)
                locations.append(result[0])
    else:
        random = secrets.randbits(74)
        identity = uuid.UUID(int=(int(time.time() * 1000) << 80) | (7 << 76)
                             | ((random >> 62) << 64) | (2 << 62) | (random & ((1 << 62) - 1)))
        stem = f"selected-{identity}"
        uri = table.location() + f"/data/{stem}.parquet"
        for _ in range(2):
            response = file_request(table.io.properties, "PUT", uri, data,
                                    f"?crowdb-native-upload={identity}")
            location = response.json()["location"]
            assert response.headers["ETag"] == '"' + hashlib.sha256(data).hexdigest() + '"', response.headers
            verify_file(table, location, stem, data)
            locations.append(location)
    assert locations[0] != locations[1], locations
    verify_file(table, locations[0], stem, data)
    return locations[1]


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
