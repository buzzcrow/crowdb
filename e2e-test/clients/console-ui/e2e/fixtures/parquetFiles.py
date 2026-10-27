# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Generate local upload inputs; all Catalog mutations remain in the browser."""
import base64
import json
import pyarrow as pa
import pyarrow.parquet as pq


def encode(table):
    output = pa.BufferOutputStream()
    pq.write_table(table, output, compression="NONE")
    return base64.b64encode(output.getvalue().to_pybytes()).decode("ascii")


print(json.dumps({
    "a": encode(pa.table({"id": [1, 2], "message": ["alpha", "雪"]})),
    "b": encode(pa.table({"id": [3, 4], "message": ["beta", "gamma"]})),
    "bad": encode(pa.table({"id": ["wrong type"], "message": ["reject"]})),
}))
