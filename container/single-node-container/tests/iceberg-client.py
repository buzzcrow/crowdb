import hashlib
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


NAMESPACE = ("crowdb-preview-e2e",)
TABLE = NAMESPACE + ("events",)
ORDERS = NAMESPACE + ("orders",)
LARGE = NAMESPACE + ("large",)
LARGE_PAYLOAD = hashlib.shake_256(b"crowdb-single-node-large-file").digest(9 * 1024 * 1024)


def main():
    catalog = load_catalog(
        "crowdb-preview",
        type="rest",
        uri=os.environ["CROWDB_PREVIEW_ICEBERG_URI"],
        token=os.environ["ICEBERG_TOKEN"],
    )
    if sys.argv[1] == "write":
        catalog.create_namespace(NAMESPACE, {"preview": "persisted"})
        table = catalog.create_table(
            TABLE,
            Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True)),
        )
        table.transaction().set_properties({"preview": "persisted"}).commit_transaction()
        orders = pd.DataFrame(
            [
                (1, "Beijing", "paid", 120),
                (2, "Shanghai", "paid", 80),
                (3, "Beijing", "cancelled", 200),
                (4, "Shanghai", "paid", 60),
                (5, "Shenzhen", "paid", 50),
                (6, "Beijing", "paid", 30),
            ],
            columns=["order_id", "city", "status", "amount_usd"],
        )
        arrow_orders = pa.Table.from_pandas(orders, preserve_index=False)
        table = catalog.create_table(ORDERS, schema=arrow_orders.schema)
        table.append(arrow_orders)
        large_schema = pa.schema([pa.field("payload", pa.binary())])
        large = catalog.create_table(LARGE, schema=large_schema)
        large.append(pa.Table.from_pylist([{"payload": LARGE_PAYLOAD}], schema=large_schema))
    assert catalog.namespace_exists(NAMESPACE)
    assert NAMESPACE in catalog.list_namespaces()
    assert catalog.load_namespace_properties(NAMESPACE) == {"preview": "persisted"}
    assert TABLE in catalog.list_tables(NAMESPACE)
    assert LARGE in catalog.list_tables(NAMESPACE)
    assert catalog.load_table(TABLE).properties["preview"] == "persisted"
    assert catalog.load_table(LARGE).scan().to_arrow().column("payload")[0].as_py() == LARGE_PAYLOAD
    verify_native_listing(catalog.load_table(ORDERS))
    saved = catalog.load_table(ORDERS).scan().to_pandas()
    revenue = (
        saved[saved["status"] == "paid"]
        .groupby("city", as_index=False)
        .agg(orders=("order_id", "count"), revenue_usd=("amount_usd", "sum"))
        .sort_values("revenue_usd", ascending=False)
        .reset_index(drop=True)
    )
    expected = pd.DataFrame(
        [("Beijing", 2, 150), ("Shanghai", 2, 140), ("Shenzhen", 1, 50)],
        columns=["city", "orders", "revenue_usd"],
    )
    pd.testing.assert_frame_equal(revenue, expected, check_dtype=False)


def verify_native_listing(table):
    # Exact metadata access and directory discovery use the same delegated FileIO.
    file = table.io.new_input(table.metadata_location)
    assert file.exists()
    with file.open() as stream:
        assert stream.read(1) == b"{"
    prefix = file._path.rsplit("/", 1)[0]
    files = file._filesystem.get_file_info(fs.FileSelector(prefix, recursive=True))
    assert file._path in [entry.path for entry in files if entry.type == fs.FileType.File]
    missing = table.io.new_input(table.location().rstrip("/") + "/data/listing-probe-missing.parquet")
    assert not missing.exists()


if __name__ == "__main__":
    main()
