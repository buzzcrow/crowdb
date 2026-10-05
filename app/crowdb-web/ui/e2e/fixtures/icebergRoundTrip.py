# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Append and read real rows from a table created through the console UI."""
import json
import sys
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.io import load_file_io

catalog = load_catalog("ui-round-trip", type="rest", uri=sys.argv[1])
table = catalog.load_table((sys.argv[2], "events"))
# The UI create response vends a writer grant; Console GETs vend read grants.
table.io = load_file_io({**table.io.properties, **json.load(sys.stdin)}, location=table.location())
expected = [{"id": 1, "message": "three nodes"}, {"id": 2, "message": "雪"}, {"id": 3, "message": "S3 + Iceberg"}]
table.append(pa.Table.from_pylist(expected, schema=pa.schema([("id", pa.int64()), ("message", pa.string())])))
actual = catalog.load_table((sys.argv[2], "events")).scan().to_arrow().to_pylist()
assert sorted(actual, key=lambda row: row["id"]) == expected, actual
print("Read back all 3 Iceberg rows with exact values")
