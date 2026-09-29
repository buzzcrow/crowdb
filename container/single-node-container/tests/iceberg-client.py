import os
import sys

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


NAMESPACE = ("crowdb-preview-e2e",)
TABLE = NAMESPACE + ("events",)
ORDERS = NAMESPACE + ("orders",)


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
    assert catalog.namespace_exists(NAMESPACE)
    assert NAMESPACE in catalog.list_namespaces()
    assert catalog.load_namespace_properties(NAMESPACE) == {"preview": "persisted"}
    assert TABLE in catalog.list_tables(NAMESPACE)
    assert catalog.load_table(TABLE).properties["preview"] == "persisted"
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


if __name__ == "__main__":
    main()
