# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Catalog-selected dataframe and bounded batch acceptance, never static-file scans."""

import importlib.metadata
import json
import os
from pathlib import Path
import sys

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import LessThan

NAMESPACE = ("ecosystem",)
TABLE = NAMESPACE + ("batches",)
HANDOFF = NAMESPACE + ("handoff",)
BATCH_ROWS = 8192
PINS = {"pyiceberg": "0.11.1", "pyarrow": "25.0.1", "pandas": "3.0.6",
        "polars": "1.35.2", "duckdb": "1.4.3"}
SCHEMA = pa.schema([pa.field("id", pa.int64()), pa.field("amount", pa.int64())])


def catalog():
    return load_catalog("ecosystem", type="rest", uri=os.environ["CROWDB_PREVIEW_ICEBERG_URI"],
                        token=os.environ["ICEBERG_TOKEN"])


def state_path():
    return Path(os.environ["CROWDB_ECOSYSTEM_STATE"])


def rows(start, end):
    return pa.table({"id": range(start, end), "amount": range(start, end)}, schema=SCHEMA)


def write():
    cat = catalog()
    cat.create_namespace(NAMESPACE)
    table = cat.create_table(TABLE, schema=SCHEMA, properties={"format-version": "2"})
    table.append(rows(0, BATCH_ROWS))
    first = table.current_snapshot().snapshot_id
    table.append(rows(BATCH_ROWS, 2 * BATCH_ROWS))
    second = table.current_snapshot().snapshot_id
    assert first != second
    handoff = cat.create_table(HANDOFF, schema=SCHEMA, properties={"format-version": "2"})
    handoff.append(pa.table({"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}, schema=SCHEMA))
    state_path().write_text(json.dumps({"first": first, "second": second,
                                      "handoff": handoff.current_snapshot().snapshot_id}))


def consume(table, snapshot, count):
    # Retain only one batch and scalar accumulators. Never call read_all/to_arrow
    # or concatenate the batch stream into an eagerly materialized dataframe.
    seen = total = batches = maximum = 0
    with table.scan(snapshot_id=snapshot, selected_fields=("id", "amount")).to_arrow_batch_reader() as reader:
        assert reader.schema == SCHEMA
        for batch in reader:
            assert 0 < batch.num_rows <= BATCH_ROWS
            seen += batch.num_rows
            batches += 1
            maximum = max(maximum, batch.num_rows)
            total += pc.sum(pc.multiply(batch.column("amount"), 2)).as_py()
    assert seen == count
    assert total == count * (count - 1)
    assert batches >= count // BATCH_ROWS
    return {"rows": seen, "batches": batches, "maximum_batch_rows": maximum}


def verify():
    state = json.loads(state_path().read_text())
    table = catalog().load_table(TABLE)
    assert table.current_snapshot().snapshot_id == state["second"]
    evidence = []
    for snapshot, count in [(state["first"], BATCH_ROWS), (state["second"], 2 * BATCH_ROWS)]:
        scan = table.scan(snapshot_id=snapshot, row_filter=LessThan("id", 16), selected_fields=("id", "amount"))
        expected = pd.DataFrame({"id": range(16), "amount": range(16)}, dtype="int64")
        pd.testing.assert_frame_equal(scan.to_pandas().sort_values("id").reset_index(drop=True), expected)
        frame = scan.to_polars().sort("id")
        assert frame.schema == {"id": pl.Int64, "amount": pl.Int64}
        assert frame.to_dict(as_series=False) == {"id": list(range(16)), "amount": list(range(16))}
        with scan.to_arrow_batch_reader() as reader:
            actual = sorted((row for batch in reader for row in batch.to_pylist()), key=lambda row: row["id"])
        assert actual == [{"id": i, "amount": i} for i in range(16)]
        evidence.append({"snapshot": snapshot, **consume(table, snapshot, count)})
    print(json.dumps({"client": "python", "versions": PINS, "snapshots": evidence}))


def handoff():
    table = catalog().load_table(HANDOFF)
    actual = sorted((r["id"], r["amount"]) for r in table.scan(selected_fields=("id", "amount")).to_arrow().to_pylist())
    assert actual == [(1, 10), (3, 30), (4, 40), (5, 50), (6, 60)], actual
    assert table.current_snapshot().snapshot_id == json.loads(state_path().read_text())["engine_snapshot"]
    initial = json.loads(state_path().read_text())["handoff"]
    old = table.scan(snapshot_id=initial, selected_fields=("id", "amount")).to_arrow().to_pylist()
    assert sorted((r["id"], r["amount"]) for r in old) == [(1, 10), (2, 20), (3, 30), (4, 40)]
    print(json.dumps({"client": "python-handoff", "snapshot": table.current_snapshot().snapshot_id, "rows": actual}))


def capture():
    state = json.loads(state_path().read_text())
    state["engine_snapshot"] = catalog().load_table(HANDOFF).current_snapshot().snapshot_id
    state_path().write_text(json.dumps(state))


if __name__ == "__main__":
    for package, version in PINS.items():
        assert importlib.metadata.version(package) == version, package
    {"write": write, "verify": verify, "handoff": handoff, "capture": capture}[sys.argv[1]]()
